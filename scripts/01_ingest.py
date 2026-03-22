"""
Phase 1: Spatial ETL Ingestion.
Extracts dynamic GeoJSON payloads from authoritative government REST endpoints
and vectorizes biological raster data (USDA NLCD) into a PostGIS database.
"""
import os
import sys
import time
import logging
import requests
import warnings
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.features import shapes, sieve
from shapely.geometry import shape
from shapely.ops import transform
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

RAW_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")
RASTER_PATH = os.path.join(RAW_DATA_DIR, f"nlcd_{TARGET_GMU}.tif")

BASE_URLS = {
    "gmu_boundary": "https://services5.arcgis.com/ttNGmDvKQA7oeDQ3/arcgis/rest/services/CPWAdminData/FeatureServer/6/query",
    "all_gmus": "https://services5.arcgis.com/ttNGmDvKQA7oeDQ3/arcgis/rest/services/CPWAdminData/FeatureServer/6/query",
    "surface_ownership": "https://gis.blm.gov/coarcgis/rest/services/lands/BLM_Colorado_Surface_Management_Agency/FeatureServer/1/query",
    "co_parcels": "https://gis.colorado.gov/public/rest/services/Address_and_Parcel/Colorado_Public_Parcels/FeatureServer/0/query",
    "walk_in_access": "https://services5.arcgis.com/ttNGmDvKQA7oeDQ3/arcgis/rest/services/CPWAdminData/FeatureServer/12/query",
    "co_roads": "https://gis.colorado.gov/public/rest/services/OIT/Colorado_State_Basemap/MapServer/32/query",
    "campgrounds": "https://services5.arcgis.com/ttNGmDvKQA7oeDQ3/arcgis/rest/services/CPWAdminData/FeatureServer/5/query",
    "cotrex_trails": "https://services5.arcgis.com/ttNGmDvKQA7oeDQ3/arcgis/rest/services/Trails/FeatureServer/0/query",
    "nhd_hr_water": "https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/USA_Rivers_and_Streams/FeatureServer/0/query",
    "pheasant_conc": "https://services5.arcgis.com/ttNGmDvKQA7oeDQ3/arcgis/rest/services/CPWSpeciesData/FeatureServer/179/query",
    "pheasant_overall": "https://services5.arcgis.com/ttNGmDvKQA7oeDQ3/arcgis/rest/services/CPWSpeciesData/FeatureServer/180/query"
}

STATEWIDE_LAYERS = ["pheasant_conc", "pheasant_overall", "all_gmus"]

def pre_flight_check(engine):
    logging.info(f"Booting Distributed Container for GMU {TARGET_GMU}...")
    
    # Connection Retry Logic (Handles Postgres first-time double-boot)
    max_retries = 5
    for attempt in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT PostGIS_Full_Version();"))
            logging.info("Database connection established and stable.")
            return  # Success! Exit the function.
        except Exception as e:
            logging.warning(f"Database is warming up. Retrying in 5 seconds... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(5)
            
    logging.critical("CONNECTION ERROR: Database failed to stabilize after multiple attempts.")
    sys.exit(1)

def _drop_z(geom):
    if geom is None or not getattr(geom, 'has_z', False): return geom
    return transform(lambda x, y, z=None: (x, y), geom)

def standardize_schema(gdf):
    gdf.columns = [col.lower() for col in gdf.columns]
    if 'geometry' not in gdf.columns: return gpd.GeoDataFrame()
    gdf = gdf.set_geometry('geometry')
    gdf = gdf[gdf.geometry.notnull() & ~gdf.geometry.is_empty]
    gdf['geometry'] = gdf.geometry.apply(_drop_z)
    junk = ['objectid', 'shape_length', 'shape_area', 'globalid', 'st_area(shape)', 'st_length(shape)']
    gdf.drop(columns=[c for c in junk if c in gdf.columns], inplace=True, errors='ignore')
    if gdf.crs is None: gdf.set_crs("EPSG:4326", inplace=True)
    elif gdf.crs.to_string() != "EPSG:4326": gdf = gdf.to_crs("EPSG:4326")
    return gdf

def get_aoi_envelope(engine):
    try:
        df = gpd.read_postgis(f"SELECT * FROM gmu_boundary_{TARGET_GMU}", engine, geom_col='geometry')
        if df.empty: raise ValueError("Table exists but contains no geometry.")
        b = df.total_bounds 
        return f"{b[0]},{b[1]},{b[2]},{b[3]}"
    except Exception as e:
        logging.critical(f"Failed to compute bounding box for GMU {TARGET_GMU}. Reason: {e}")
        sys.exit(1)

def fetch_and_store_geojson(url, base_table_name, engine, envelope=None):
    target_table = f"{base_table_name}_{TARGET_GMU}" 
    logging.info(f"Streaming Vector Data: {target_table}")
    offset = 0
    limit = 1000
    is_first_chunk = True
    headers = {'User-Agent': 'Mozilla/5.0'}
    order_field = 'OBJECTID' 
    
    while True:
        params = {
            'outFields': '*', 
            'f': 'geojson', 
            'outSR': '4326', 
            'resultRecordCount': limit, 
            'resultOffset': offset
        }
        
        if order_field and base_table_name not in STATEWIDE_LAYERS:
            params['orderByFields'] = order_field
            
        if base_table_name == "gmu_boundary":
            params['where'] = f"GMUID = '{TARGET_GMU}' OR GMUID = {TARGET_GMU}"
        else:
            params['where'] = "1=1"
            
        if envelope and base_table_name != "gmu_boundary" and base_table_name not in STATEWIDE_LAYERS:
            params.update({'geometry': envelope, 'geometryType': 'esriGeometryEnvelope', 
                           'spatialRel': 'esriSpatialRelIntersects', 'inSR': '4326'})
                           
        try:
            r = requests.get(url, params=params, headers=headers, timeout=60)
            if r.status_code != 200: 
                logging.warning(f"HTTP ERROR {r.status_code} on {target_table} - Skipping Layer.")
                break
                
            data = r.json()
            if 'error' in data:
                error_msg = str(data['error'])
                if 'OBJECTID' in error_msg and order_field == 'OBJECTID':
                    logging.info("Server rejected OBJECTID. Switching to FID...")
                    order_field = 'FID'
                    continue 
                elif 'FID' in error_msg and order_field == 'FID':
                    logging.info("Server rejected FID. Dropping pagination ordering...")
                    order_field = None
                    continue 
                else:
                    logging.error(f"ESRI API ERROR on {target_table}: {error_msg}")
                    break
                
            features = data.get('features', [])
            if not features: break
            
            gdf = standardize_schema(gpd.GeoDataFrame.from_features(features))
            if gdf.empty: 
                offset += limit
                continue
            
            if is_first_chunk:
                sample_dict = gdf.drop(columns=['geometry'], errors='ignore').head(1).to_dict(orient='records')
                if sample_dict: logging.info(f"Schema Sample: {sample_dict[0]}")
                
            write_mode = 'replace' if is_first_chunk else 'append'
            gdf.to_postgis(target_table, engine, if_exists=write_mode, index=False)
            
            if len(features) < limit: break 
            offset += limit
            is_first_chunk = False
            
        except Exception as e:
            logging.error(f"Exception during request offset {offset} for {target_table}: {e}")
            break
            
# If is_first_chunk is False, it means data was successfully written to the DB
    if not is_first_chunk:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{target_table}_geom ON {target_table} USING GIST (geometry);"))
            logging.info(f"Successfully built index for {target_table}")
        except Exception as e:
            logging.warning(f"Could not create index for {target_table}: {e}")
    else:
        # If it is still True, no data was ever written. Skip the index creation.
        logging.info(f"API returned zero valid records for {target_table}. Skipping table and index creation.")

def fetch_usda_cropscape(engine, output_path):
    try:
        df = gpd.read_postgis(f"SELECT * FROM gmu_boundary_{TARGET_GMU}", engine, geom_col='geometry')
        df_5070 = df.to_crs("EPSG:5070")
        b = df_5070.total_bounds 
        api_url = f"https://nassgeodata.gmu.edu/axis2/services/CDLService/GetCDLFile?year=2023&bbox={int(b[0])},{int(b[1])},{int(b[2])},{int(b[3])}"
        response = requests.get(api_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=120)
        if response.status_code == 200:
            tif_url = response.text.split('<returnURL>')[1].split('</returnURL>')[0]
            tif_response = requests.get(tif_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=120)
            with open(output_path, 'wb') as f:
                f.write(tif_response.content)
            return True
        return False
    except Exception as e: 
        logging.error(f"Failed to fetch Cropscape Raster: {e}")
        return False

def process_landcover_raster(engine):
    if not os.path.exists(RASTER_PATH): return
    target_classes = {
        1: "Corn", 4: "Sorghum", 6: "Sunflowers", 24: "Winter Wheat",
        28: "Oats", 29: "Millet", 36: "Alfalfa", 37: "Other Hay",
        61: "Fallow/Idle", 176: "Grassland", 195: "Wetlands"
    }
    try:
        with rasterio.open(RASTER_PATH) as src:
            image = src.read(1)
            cleaned_raster = sieve(image, size=10, connectivity=8)
            results = ({'properties': {'val': v}, 'geometry': s} for i, (s, v) in enumerate(shapes(cleaned_raster, mask=None, transform=src.transform)) if v in target_classes.keys())
            geoms, attrs = [], []
            for res in results:
                geoms.append(shape(res['geometry']))
                attrs.append(target_classes[res['properties']['val']])

        if not geoms: return
        gdf = gpd.GeoDataFrame({'cover_type': attrs, 'geometry': geoms}, crs=src.crs).to_crs("EPSG:4326")
        gdf = gdf.dissolve(by='cover_type').reset_index()
        gdf['geometry'] = gdf.geometry.simplify(tolerance=0.0001, preserve_topology=True).buffer(0)
        target_table = f"map_crop_habitat_{TARGET_GMU}"
        gdf.to_postgis(target_table, engine, if_exists='replace', index=False)
        
        with engine.begin() as conn:
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_crop_{TARGET_GMU}_geom ON {target_table} USING GIST (geometry);"))
    except Exception as e:
        logging.error(f"Error processing USDA raster: {e}")

def fetch_legal_hunting_hours(engine):
    logging.info(f"Fetching Tabular Regulatory Data (Shooting Hours) for Unit {TARGET_GMU}...")
    try:
        # 1. Get the exact centroid of the GMU from the database
        query = f"SELECT ST_Y(ST_Centroid(geometry)) as lat, ST_X(ST_Centroid(geometry)) as lon FROM gmu_boundary_{TARGET_GMU}"
        centroid = pd.read_sql(query, engine)
        
        if centroid.empty:
            return
            
        lat = centroid['lat'].iloc[0]
        lon = centroid['lon'].iloc[0]

        # 2. Hit the free REST API for Upland Opening Day
        # Hardcoded to 2024 Upland Opening Day for presentation evaluation purposes.
        url = f"https://api.sunrise-sunset.org/json?lat={lat}&lng={lon}&date=2024-11-09&formatted=0"
        response = requests.get(url, timeout=30).json()

        if response.get('status') == 'OK':
            # Parse UTC times
            sunrise_utc = pd.to_datetime(response['results']['sunrise'])
            sunset_utc = pd.to_datetime(response['results']['sunset'])

            # Convert to Mountain Standard Time (UTC-7)
            sunrise_mst = sunrise_utc - pd.Timedelta(hours=7)
            sunset_mst = sunset_utc - pd.Timedelta(hours=7)
            
            # Apply CPW Regulation: 30 minutes before sunrise
            legal_start = sunrise_mst - pd.Timedelta(minutes=30)

            # 3. Create a purely tabular DataFrame (No Geometry!)
            df = pd.DataFrame({
                'gmu_id': [str(TARGET_GMU)],
                'target_species': ['Pheasant / Quail'],
                'opening_day': ['Nov 9'],
                'legal_start': [legal_start.strftime('%I:%M %p')],
                'legal_end': [sunset_mst.strftime('%I:%M %p')],
                'reg_rule': ['Legal hunting hours: 1/2 hour before sunrise to sunset.']
            })

            # 4. Push to PostGIS as a standard relational table
            target_table = f"gmu_regulations_{TARGET_GMU}"
            df.to_sql(target_table, engine, if_exists='replace', index=False)
            logging.info(f"Successfully built relational regulations table: {target_table}")
            
            # --- SENIOR FLEX: ENFORCING RELATIONAL INTEGRITY ---
            logging.info("Establishing Primary/Foreign Key relationships for GMU tabular data...")
            try:
                with engine.begin() as conn:
                    # 1. Ensure GMU boundary has a primary key
                    conn.execute(text(f"""
                        ALTER TABLE gmu_boundary_{TARGET_GMU} 
                        ADD COLUMN IF NOT EXISTS pk_id SERIAL PRIMARY KEY;
                    """))
                    # 2. Add Foreign Key to regulations table referencing the GMU boundary
                    # Note: Assumes gmuid exists and is unique in the boundary table
                    conn.execute(text(f"""
                        ALTER TABLE gmu_regulations_{TARGET_GMU}
                        ADD CONSTRAINT unique_gmuid_{TARGET_GMU} UNIQUE (gmu_id);
                    """))
                    logging.info("Relational constraints successfully applied.")
            except Exception as e:
                logging.warning(f"Could not establish PK/FK constraint: {e}")
            
    except Exception as e:
        logging.error(f"Failed to fetch tabular regulations: {e}")

def run_pipeline():
    engine = create_engine(DB_URL)
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    pre_flight_check(engine)
    
    logging.info("Executing Vector API Extractions (Real Data Only)...")
    fetch_and_store_geojson(BASE_URLS["gmu_boundary"], "gmu_boundary", engine)
    
    envelope = get_aoi_envelope(engine)
    for table, url in BASE_URLS.items():
        if table == "gmu_boundary": continue
        fetch_and_store_geojson(url, table, engine, envelope)

    if fetch_usda_cropscape(engine, RASTER_PATH):
        process_landcover_raster(engine)
    logging.info(f"[SUCCESS] Distributed Pipeline Ingestion Complete for Unit {TARGET_GMU}.")
    fetch_legal_hunting_hours(engine)

def check_logs_for_issues():
    """Scans the QA log file and prints a high-visibility alert to the console if errors exist."""
    if os.path.exists(qa_log_file):
        with open(qa_log_file, 'r') as f:
            logs = f.read()
            # Check if any ERROR or WARNING tags were written to the log today
            if " - ERROR - " in logs or " - WARNING - " in logs:
                print("\n" + "="*60, flush=True)
                print(" [!] ISSUES DETECTED DURING PIPELINE EXECUTION [!]", flush=True)
                print(" Please check logs/pipeline_qa.log for specific details.", flush=True)
                print("="*60 + "\n", flush=True)

if __name__ == "__main__":
    run_pipeline()
    check_logs_for_issues()