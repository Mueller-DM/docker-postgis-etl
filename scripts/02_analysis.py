"""
Phase 2: Enterprise Spatial Analysis.
Executes advanced PostGIS topology logic to flatten overlapping land agencies, 
private parcels, and agricultural geometry into a single Master Scout Fabric.
"""
import os
import sys
import time
import logging
import warnings
import pandas as pd
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")

# --- LOGGING SETUP ---
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger()
logger.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

if logger.hasHandlers():
    logger.handlers.clear()

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

qa_log_file = os.path.join(LOG_DIR, "pipeline_qa.log")
file_handler = logging.FileHandler(qa_log_file)
file_handler.setLevel(logging.WARNING) 
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# --- ENVIRONMENT & GLOBALS ---
DB_URL = os.getenv("DB_URL", "postgresql://postgres:postgrespassword@postgis_db:5432/onx_hunting")
TARGET_GMU = os.getenv("TARGET_GMU", "102")

def run_analysis():
    logging.info(f"Starting Governed Enterprise Pipeline for Unit {TARGET_GMU}...")
    engine = create_engine(DB_URL)

    try:
        # 1. Dynamic Column & Gap Detection
        try:
            df = pd.read_sql(f"SELECT * FROM co_parcels_{TARGET_GMU} LIMIT 1", engine)
            found_col = next((c for c in ['owner_name', 'own_name', 'owner', 'name', 'parcel_owner'] if c in df.columns), None)
            owner_select = f"p.{found_col}" if found_col else "NULL"
            
            addr_col = next((c for c in ['situsadd', 'situs_address', 'address', 'sitadd'] if c in df.columns), None)
            addr_select = f"p.{addr_col}" if addr_col else "NULL"
            
            owner_city_col = next((c for c in ['ownaddcty', 'owner_city', 'city'] if c in df.columns), None)
            owner_state_col = next((c for c in ['ownaddstt', 'owner_state', 'state'] if c in df.columns), None)
            city_sql = f"p.{owner_city_col}" if owner_city_col else "NULL"
            state_sql = f"p.{owner_state_col}" if owner_state_col else "NULL"
            has_parcels = True
        except Exception as e:
            logging.warning(f"Authoritative Parcel Gap Detected in base layer: {e}")
            has_parcels = False
            owner_select, addr_select, city_sql, state_sql = "NULL", "NULL", "NULL", "NULL"

        # 2. Pheasant Layers
        try:
            with engine.connect() as conn:
                has_phc = conn.execute(text(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'pheasant_conc_{TARGET_GMU}');")).scalar()
                has_pho = conn.execute(text(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'pheasant_overall_{TARGET_GMU}');")).scalar()
        except Exception: 
            has_phc, has_pho = False, False

        # TOPOLOGY FIX: Using vp.core_geom to catch edges and odd shapes
        ph_lat = ""
        ph_sel = "NULL as pheasant_status,"
        if has_phc and has_pho:
            ph_lat = f"""
            LEFT JOIN LATERAL (SELECT 'High Density Concentration' as status FROM pheasant_conc_{TARGET_GMU} WHERE vp.core_geom && geometry AND ST_Intersects(vp.core_geom, geometry) LIMIT 1) phc ON true
            LEFT JOIN LATERAL (SELECT 'Overall Range' as status FROM pheasant_overall_{TARGET_GMU} WHERE vp.core_geom && geometry AND ST_Intersects(vp.core_geom, geometry) LIMIT 1) pho ON true
            """
            ph_sel = "COALESCE(phc.status, pho.status) as pheasant_status,"
        elif has_phc:
            ph_lat = f"LEFT JOIN LATERAL (SELECT 'High Density Concentration' as status FROM pheasant_conc_{TARGET_GMU} WHERE vp.core_geom && geometry AND ST_Intersects(vp.core_geom, geometry) LIMIT 1) phc ON true"
            ph_sel = "phc.status as pheasant_status,"

        gz_lat = ""
        gz_sel = "NULL as is_gold_zone,"
        if has_phc or has_pho:
            gz_lat = f"LEFT JOIN LATERAL (SELECT 'Yes' as is_gold FROM prime_gold_zones_{TARGET_GMU} gz WHERE vp.core_geom && gz.geometry AND ST_Intersects(vp.core_geom, gz.geometry) LIMIT 1) gz ON true"
            gz_sel = "gz.is_gold as is_gold_zone,"

        # engine.begin() automatically commits transactions upon successful completion
        with engine.begin() as conn:
            # ==========================================
            # STEP 1: CONTINUOUS BASE GEOMETRY (GMU Wall-to-Wall)
            # ==========================================
            logging.info("[Step 1/5] Building Continuous Base Geometry...")
            conn.execute(text(f"DROP TABLE IF EXISTS tmp_vp_{TARGET_GMU};"))
            
            if has_parcels:
                conn.execute(text(f"""
                CREATE TABLE tmp_vp_{TARGET_GMU} AS
                SELECT 
                    ROW_NUMBER() OVER () as pid,
                    ST_MakeValid(p.geometry) as valid_geom,
                    CASE WHEN ST_IsEmpty(ST_Buffer(ST_MakeValid(p.geometry), -0.00015)) THEN ST_PointOnSurface(ST_MakeValid(p.geometry)) ELSE ST_Buffer(ST_MakeValid(p.geometry), -0.00015) END as core_geom,
                    ST_PointOnSurface(ST_MakeValid(p.geometry)) as center_pt,
                    {owner_select} as private_owner, {addr_select} as physical_address, {city_sql} as owner_city, {state_sql} as owner_state,
                    p.landacres, p.landusedsc, '{TARGET_GMU}' as gmu_id
                FROM co_parcels_{TARGET_GMU} p
                JOIN gmu_boundary_{TARGET_GMU} g ON ST_Intersects(ST_PointOnSurface(ST_MakeValid(p.geometry)), g.geometry);
                """))
                
                conn.execute(text(f"""
                INSERT INTO tmp_vp_{TARGET_GMU} (pid, valid_geom, core_geom, center_pt, private_owner, physical_address, owner_city, owner_state, landacres, landusedsc, gmu_id)
                WITH unioned_parcels AS (
                    SELECT ST_Union(ST_MakeValid(geometry)) as geom FROM co_parcels_{TARGET_GMU}
                ),
                diff AS (
                    SELECT ST_Difference(g.geometry, COALESCE(u.geom, 'GEOMETRYCOLLECTION EMPTY'::geometry)) as geom 
                    FROM gmu_boundary_{TARGET_GMU} g
                    LEFT JOIN unioned_parcels u ON true
                ),
                dumped AS (
                    SELECT (ST_Dump(geom)).geom as diff_geom FROM diff
                ),
                grid AS (
                    SELECT (ST_Dump(ST_Intersection(d.diff_geom, grid_cell.geom))).geom as grid_geom
                    FROM dumped d
                    CROSS JOIN LATERAL ST_SquareGrid(0.003, d.diff_geom) AS grid_cell
                )
                SELECT 
                    (SELECT COALESCE(MAX(pid), 0) FROM tmp_vp_{TARGET_GMU}) + ROW_NUMBER() OVER (),
                    ST_MakeValid(grid_geom),
                    ST_MakeValid(grid_geom),
                    ST_PointOnSurface(ST_MakeValid(grid_geom)),
                    NULL, NULL, NULL, NULL,
                    ROUND((ST_Area(grid_geom::geography) * 0.000247105)::numeric, 1),
                    'Unmapped Area',
                    '{TARGET_GMU}'
                FROM grid
                WHERE ST_GeometryType(grid_geom) IN ('ST_Polygon', 'ST_MultiPolygon') AND NOT ST_IsEmpty(grid_geom);
                """))
            else:
                conn.execute(text(f"""
                CREATE TABLE tmp_vp_{TARGET_GMU} AS
                WITH grid AS (
                    SELECT (ST_Dump(ST_Intersection(g.geometry, grid_cell.geom))).geom as geom
                    FROM gmu_boundary_{TARGET_GMU} g
                    CROSS JOIN LATERAL ST_SquareGrid(0.003, g.geometry) AS grid_cell
                )
                SELECT 
                    ROW_NUMBER() OVER () as pid, ST_MakeValid(geom) as valid_geom, ST_MakeValid(geom) as core_geom, ST_PointOnSurface(ST_MakeValid(geom)) as center_pt,
                    NULL::text as private_owner, NULL::text as physical_address, NULL::text as owner_city, NULL::text as owner_state, 
                    ROUND((ST_Area(geom::geography) * 0.000247105)::numeric, 1) as landacres, NULL::text as landusedsc, '{TARGET_GMU}' as gmu_id
                FROM grid
                WHERE ST_GeometryType(geom) IN ('ST_Polygon', 'ST_MultiPolygon') AND NOT ST_IsEmpty(geom);
                """))
                
            conn.execute(text(f"CREATE INDEX idx_vp_core_{TARGET_GMU} ON tmp_vp_{TARGET_GMU} USING GIST (core_geom);"))
            conn.execute(text(f"CREATE INDEX idx_vp_pt_{TARGET_GMU} ON tmp_vp_{TARGET_GMU} USING GIST (center_pt);"))
            conn.execute(text(f"ANALYZE tmp_vp_{TARGET_GMU};"))

            # ==========================================
            # STEP 2 & 3: KNN WATER & CROPS
            # ==========================================
            logging.info("[Step 2-3/5] Spatial Aggregations...")
            conn.execute(text(f"DROP TABLE IF EXISTS tmp_w_{TARGET_GMU};"))
            conn.execute(text(f"""
            CREATE TABLE tmp_w_{TARGET_GMU} AS
            SELECT vp.pid, w.name as nearest_water_name, w.feature as nearest_water_type, ROUND((ST_Distance(vp.center_pt::geography, w.geometry::geography) * 0.000621371)::numeric, 2) as distance_miles
            FROM tmp_vp_{TARGET_GMU} vp CROSS JOIN LATERAL (SELECT name, feature, geometry FROM nhd_hr_water_{TARGET_GMU} WHERE geometry && ST_Expand(vp.center_pt, 0.1) ORDER BY vp.center_pt <-> geometry LIMIT 1) w;
            """))
            conn.execute(text(f"DROP TABLE IF EXISTS tmp_c_{TARGET_GMU};"))
            conn.execute(text(f"""
            CREATE TABLE tmp_c_{TARGET_GMU} AS
            SELECT vp.pid, STRING_AGG(DISTINCT c.cover_type, ', ') as crop_type
            FROM tmp_vp_{TARGET_GMU} vp JOIN map_crop_habitat_{TARGET_GMU} c ON vp.core_geom && c.geometry AND ST_Intersects(vp.core_geom, c.geometry) GROUP BY vp.pid;
            """))

            # ==========================================
            # STEP 4: PRIME HABITAT ZONES (Materialized)
            # ==========================================
            logging.info("[Step 4/5] Calculating Geometric Intersections for Prime Zones...")
            conn.execute(text(f"DROP TABLE IF EXISTS prime_gold_zones_{TARGET_GMU};"))
            
            if has_phc or has_pho:
                hab_layer = f"pheasant_conc_{TARGET_GMU}" if has_phc else f"pheasant_overall_{TARGET_GMU}"
                prime_crops = "('Corn', 'Sorghum', 'Winter Wheat', 'Sunflowers', 'Millet', 'Alfalfa', 'Oats')"
                
                conn.execute(text(f"""
                CREATE TEMP TABLE tmp_acc_{TARGET_GMU} AS
                SELECT geometry FROM walk_in_access_{TARGET_GMU}
                UNION ALL SELECT geometry FROM surface_ownership_{TARGET_GMU} WHERE adm_manage != 'PRI';
                """))
                conn.execute(text(f"CREATE INDEX idx_tacc_{TARGET_GMU} ON tmp_acc_{TARGET_GMU} USING GIST (geometry);"))
                conn.execute(text(f"ANALYZE tmp_acc_{TARGET_GMU};"))
                
                conn.execute(text(f"""
                CREATE TEMP TABLE tmp_prm_{TARGET_GMU} AS
                SELECT geometry, cover_type FROM map_crop_habitat_{TARGET_GMU} WHERE cover_type IN {prime_crops};
                """))
                conn.execute(text(f"CREATE INDEX idx_tprm_{TARGET_GMU} ON tmp_prm_{TARGET_GMU} USING GIST (geometry);"))
                conn.execute(text(f"ANALYZE tmp_prm_{TARGET_GMU};"))
                
                conn.execute(text(f"""
                CREATE TEMP TABLE tmp_ac_crp_{TARGET_GMU} AS
                SELECT ST_Multi(ST_CollectionExtract(ST_Intersection(a.geometry, c.geometry), 3)) as geometry, c.cover_type as crop_type
                FROM tmp_acc_{TARGET_GMU} a JOIN tmp_prm_{TARGET_GMU} c ON a.geometry && c.geometry AND ST_Intersects(a.geometry, c.geometry);
                """))
                conn.execute(text(f"CREATE INDEX idx_tac_crp_{TARGET_GMU} ON tmp_ac_crp_{TARGET_GMU} USING GIST (geometry);"))
                conn.execute(text(f"ANALYZE tmp_ac_crp_{TARGET_GMU};"))
                
                conn.execute(text(f"""
                CREATE TABLE prime_gold_zones_{TARGET_GMU} AS
                SELECT ST_Multi(ST_CollectionExtract(ST_Intersection(ac.geometry, ph.geometry), 3)) as geometry, ac.crop_type
                FROM tmp_ac_crp_{TARGET_GMU} ac JOIN {hab_layer} ph ON ac.geometry && ph.geometry AND ST_Intersects(ac.geometry, ph.geometry);
                """))
                
                conn.execute(text(f"DELETE FROM prime_gold_zones_{TARGET_GMU} WHERE ST_IsEmpty(geometry) OR ST_Area(geometry::geography) < 2000;"))
                conn.execute(text(f"CREATE INDEX idx_gold_{TARGET_GMU} ON prime_gold_zones_{TARGET_GMU} USING GIST (geometry);"))
                conn.execute(text(f"DROP TABLE tmp_acc_{TARGET_GMU}; DROP TABLE tmp_prm_{TARGET_GMU}; DROP TABLE tmp_ac_crp_{TARGET_GMU};"))

            # ==========================================
            # STEP 5: FINAL ASSEMBLY
            # ==========================================
            logging.info("[Step 5/5] Building Master Scout Fabric...")
            conn.execute(text(f"DROP TABLE IF EXISTS scout_fabric_{TARGET_GMU};"))
            
            conn.execute(text(f"""
            CREATE TABLE scout_fabric_{TARGET_GMU} AS
            SELECT 
                ST_Multi(ST_CollectionExtract(vp.valid_geom, 3)) as geometry,
                regs.legal_start, regs.legal_end, regs.reg_rule,
                vp.gmu_id, vp.private_owner, vp.physical_address, vp.owner_city, vp.owner_state, vp.landacres, vp.landusedsc,
                wd.nearest_water_name, wd.nearest_water_type, wd.distance_miles as dist_to_water,
                c.crop_type, pub.public_agency, pub.public_office, swa.swa_name, swa.swa_url, wia.wia_rules, wia.wia_habitat, wia.wia_url, wia.wia_close, 
                {ph_sel} {gz_sel}
                CASE 
                    WHEN swa.swa_name IS NOT NULL THEN swa.swa_name 
                    WHEN pub.public_agency IS NOT NULL THEN pub.public_agency || ' (Public Land)' 
                    WHEN vp.private_owner IS NOT NULL THEN vp.private_owner
                    ELSE 'No Parcel Data Available'
                END as hover_label
            FROM tmp_vp_{TARGET_GMU} vp
            LEFT JOIN tmp_w_{TARGET_GMU} wd ON vp.pid = wd.pid
            LEFT JOIN tmp_c_{TARGET_GMU} c ON vp.pid = c.pid
            LEFT JOIN LATERAL (SELECT CASE WHEN adm_manage = 'PRI' THEN NULL ELSE adm_manage END as public_agency, CASE WHEN adm_manage = 'PRI' THEN NULL ELSE adm_name END as public_office FROM surface_ownership_{TARGET_GMU} WHERE vp.core_geom && geometry AND ST_Intersects(vp.core_geom, geometry) LIMIT 1) pub ON true
            LEFT JOIN LATERAL (SELECT propname as swa_name, cpw_url as swa_url FROM campgrounds_{TARGET_GMU} WHERE vp.core_geom && geometry AND ST_Intersects(vp.core_geom, geometry) LIMIT 1) swa ON true
            LEFT JOIN LATERAL (SELECT coverlabel as wia_rules, cover as wia_habitat, url as wia_url, closedate as wia_close FROM walk_in_access_{TARGET_GMU} WHERE vp.core_geom && geometry AND ST_Intersects(vp.core_geom, geometry) LIMIT 1) wia ON true
            LEFT JOIN gmu_regulations_{TARGET_GMU} regs ON vp.gmu_id = regs.gmu_id
            {ph_lat} {gz_lat};
            """))
            conn.execute(text(f"CREATE INDEX idx_fab_geom_{TARGET_GMU} ON scout_fabric_{TARGET_GMU} USING GIST (geometry);"))
            conn.execute(text(f"DROP TABLE IF EXISTS tmp_vp_{TARGET_GMU}; DROP TABLE IF EXISTS tmp_w_{TARGET_GMU}; DROP TABLE IF EXISTS tmp_c_{TARGET_GMU};"))

        logging.info("[SUCCESS] Data Engineering Pipeline Complete.")

    except Exception as e:
        logging.error(f"Pipeline Failed: {e}")

def check_logs_for_issues():
    """Scans the QA log file and prints a high-visibility alert to the console if errors exist."""
    if os.path.exists(qa_log_file):
        with open(qa_log_file, 'r') as f:
            logs = f.read()
            # Check if any ERROR or WARNING tags were written to the log 
            if " - ERROR - " in logs or " - WARNING - " in logs:
                print("\n" + "="*60, flush=True)
                print(" [!] ISSUES DETECTED DURING PIPELINE EXECUTION [!]", flush=True)
                print(" Please check logs/pipeline_qa.log for specific details.", flush=True)
                print("="*60 + "\n", flush=True)

if __name__ == "__main__":
    run_analysis()
    check_logs_for_issues()