"""
Phase 3: Cartographic Presentation Layer.
Compiles the PostGIS Master Scout Fabric into a standalone, interactive 
HTML web map with zoom-dependent UI rendering and custom hunter telemetry.
"""
import os
import sys
import logging
import warnings
import traceback
import geopandas as gpd
import pandas as pd
import folium
from folium.plugins import MeasureControl, Fullscreen, Draw, Search
from sqlalchemy import create_engine

warnings.filterwarnings("ignore")

# --- PRODUCTION LOGGING SETUP ---
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
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "outputs")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, f"UNIT_{TARGET_GMU}_MASTER_SCOUT.html")

# --- CARTOGRAPHY CONFIGURATION ---
AGENCY_TRANSLATOR = {
    'BLM': 'Bureau of Land Management (BLM)',
    'STA': 'State Land Board (STATE)',
    'STATE': 'State Land Board (STATE)',
    'USFS': 'US Forest Service (USFS)',
    'USFS_NG': 'US Forest Service (USFS)',
    'FWS': 'US Fish and Wildlife (FWS)',
    'NPS': 'National Park Service (NPS)'
}

AGENCY_SHORT_CODE = {
    'BLM': 'BLM', 'STA': 'STATE', 'STATE': 'STATE', 
    'USFS': 'USFS', 'USFS_NG': 'USFS', 
    'FWS': 'FWS', 'NPS': 'NPS',
    'LOCAL': 'State & Local Public Land'
}

KNOWN_AGENCIES = {'BLM': '#DAA520', 'STATE': '#1E90FF', 'USFS': '#228B22', 'FWS': '#9370DB', 'NPS': '#8A2BE2'}

KNOWN_CROPS = {
    'corn': '#FFD700', 'wheat': '#F5DEB3', 'sorghum': '#FF8C00',
    'alfalfa': '#32CD32', 'millet': '#DAA520', 'fallow': '#D3D3D3',
    'idle': '#D3D3D3', 'grassland': '#9ACD32', 'wetlands': '#008B8B', 
    'sunflowers': '#FFD700', 'oats': '#F5DEB3', 'hay': '#98FB98'
}
FALLBACK_COLORS = ['#e6194B', '#f58231', '#ffe119', '#bfef45', '#3cb44b', '#42d4f4', '#4363d8', '#911eb4', '#f032e6', '#a9a9a9', '#800000', '#808000']

def style_roads(feature):
    return {'color': '#ffffff', 'weight': 1.5, 'opacity': 0.8, 'className': 'noclick'}

def style_trails(feature):
    return {'color': '#FF1493', 'weight': 2, 'dashArray': '4, 4', 'className': 'noclick'}

def safe_load_layer(table_name, engine):
    logging.info(f"Loading {table_name} into memory...")
    try:
        gdf = gpd.read_postgis(f"SELECT * FROM {table_name}", engine, geom_col='geometry')
        if not gdf.empty and 'geometry' in gdf.columns:
            if gdf.crs is None: 
                gdf.set_crs('EPSG:4326', inplace=True)
            elif gdf.crs.to_string() != 'EPSG:4326': 
                gdf = gdf.to_crs('EPSG:4326')
            
            gdf = gdf[gdf.geometry.notnull() & ~gdf.geometry.is_empty]
            gdf['geometry'] = gdf.geometry.simplify(tolerance=0.00005, preserve_topology=True)
            gdf = gdf[gdf.geometry.notnull() & ~gdf.geometry.is_empty] 
            return gdf
            
    except Exception as e:
        error_msg = str(e).lower()
        # 1. Handle known missing data (Expected Behavior)
        if "does not exist" in error_msg or "undefinedtable" in error_msg:
            logging.info(f"[EMPTY DATA] Table '{table_name}' not found in database. Skipping layer.")
        # 2. Flag actual critical failures for manual QA (Unexpected Behavior)
        else:
            logging.error(f"[MANUAL CHECK REQUIRED] DB failure on {table_name}. Error: {e}")
            
    return gpd.GeoDataFrame()

def generate_tabbed_popup(row, idx, is_base=False):
    owner = str(row.get('private_owner', '')).strip()
    address = str(row.get('physical_address', '')).strip()
    acres = str(row.get('landacres', '')).strip()
    legal_start = str(row.get('legal_start', '')).strip()
    legal_end = str(row.get('legal_end', '')).strip()
    opening_day = str(row.get('opening_day', 'Nov 9')).strip()
    agency_raw = str(row.get('public_agency', '')).strip()
    if 'USFS' in agency_raw.upper(): agency_raw = 'USFS'
    agency = AGENCY_TRANSLATOR.get(agency_raw.upper(), agency_raw) if agency_raw else ''
    
    office = str(row.get('public_office', '')).strip()
    crop = str(row.get('crop_type', '')).strip()
    wia_rules = str(row.get('wia_rules', '')).strip()
    wia_close = str(row.get('wia_close', '')).strip()
    wia_habitat = str(row.get('wia_habitat', '')).strip()
    wia_url = str(row.get('wia_url', '')).strip() 
    
    swa_name = str(row.get('swa_name', '')).strip()
    pheasant = str(row.get('pheasant_status', '')).strip()
    water = str(row.get('nearest_water_name', '')).strip()
    dist = str(row.get('dist_to_water', '')).strip()
    is_gold_zone_db = str(row.get('is_gold_zone', '')).strip()

    def is_valid(val): return val and val.lower() not in ['nan', 'none', 'null', '']

    res_html = f"<p style='margin: 4px 0;'><b>GMU Number:</b> {TARGET_GMU}</p>"
    
    if is_valid(is_gold_zone_db) and is_gold_zone_db.lower() == 'yes':
        res_html += "<div style='background: #FFFACD; border: 2px dashed #DAA520; padding: 6px; text-align: center; margin: 8px 0; border-radius: 4px;'><b style='color: #8B4513; font-size: 13px;'>PRIME HABITAT / ACCESS</b><br><i style='font-size: 10px;'>Intersection of High-Value Crops & Access</i></div>"

    if is_valid(legal_start) and is_valid(legal_end):
        res_html += f"<div style='background: #f0f8ff; border: 1px solid #87cefa; padding: 6px; text-align: center; margin: 8px 0; border-radius: 4px;'><b style='color: #4682b4; font-size: 12px;'>Opening Day Shooting Hours ({opening_day})</b><br><span style='font-size: 11px;'>{legal_start} - {legal_end}</span></div>"
        
    res_html += "<hr style='margin: 6px 0;'>"
    
    if not is_base:
        # LOGIC TRAP: Multi-Tiered Public Entity Detection
        owner_clean = owner.lower() if is_valid(owner) else ""
        fed_kws = ['u s a', 'usa', 'united states', 'blm', 'bureau of land', 'forest service', 'national forest']
        state_kws = ['state of', 'colorado state', 'parks and wildlife', 'board of land', 'dept of', 'department of']
        local_kws = ['town of', 'city of', 'county', 'municipal', 'school district']
        
        is_fed = any(k in owner_clean for k in fed_kws)
        is_state = any(k in owner_clean for k in state_kws)
        is_local = any(k in owner_clean for k in local_kws)
        is_public_parcel = is_fed or is_state or is_local

        if is_valid(swa_name):
            res_html += f"<p style='margin: 4px 0;'><b>Ownership:</b> State Wildlife Area</p><p style='margin: 4px 0;'><b>Property:</b> <span style='color: #228B22; font-weight: bold;'>{swa_name}</span></p>"
        elif is_valid(agency) and (not is_valid(owner) or is_public_parcel):
            res_html += f"<p style='margin: 4px 0;'><b>Ownership:</b> Public Land</p><p style='margin: 4px 0;'><b>Agency:</b> <span style='color: #DAA520; font-weight: bold;'>{agency}</span></p>"
            if is_valid(office): 
                res_html += f"<p style='margin: 4px 0;'><b>Field Office:</b> {office}</p>"
        elif is_valid(owner):
            if is_public_parcel:
                own_type = "Federal" if is_fed else "State" if is_state else "Municipal"
                res_html += f"<p style='margin: 4px 0;'><b>Ownership:</b> {own_type} Public Land (County Record)</p>"
                
                pretty_owner = owner.title().replace('U S A', 'US Government').replace('Usa', 'US Government')
                res_html += f"<p style='margin: 4px 0;'><b>Owner:</b> <span style='color: #DAA520; font-weight: bold;'>{pretty_owner}</span></p>"
                
                if is_valid(agency):
                    res_html += f"<p style='margin: 4px 0; font-size: 10px; color: #888;'><i>Federal Overlay: {agency}</i></p>"
            else:
                res_html += f"<p style='margin: 4px 0;'><b>Ownership:</b> Private Land</p>"
                res_html += f"<p style='margin: 4px 0;'><b>Landowner:</b> <span style='color: #555;'>{owner}</span></p>"
                if is_valid(address): 
                    res_html += f"<p style='margin: 4px 0;'><b>Address:</b> {address}</p>"
                if is_valid(acres) and acres not in ['0', '0.0']:
                    try: res_html += f"<p style='margin: 4px 0;'><b>Parcel Size:</b> {round(float(acres), 1)} Acres</p>"
                    except: pass
                if is_valid(agency):
                    res_html += f"<p style='margin: 4px 0; font-size: 10px; color: #888;'><i>*Note: Federal layer shows {agency} overlap here, likely a mapping shift. Trust county parcel owner.</i></p>"
        else:
            if is_valid(agency):
                res_html += f"<p style='margin: 4px 0;'><b>Ownership:</b> Public Land</p><p style='margin: 4px 0;'><b>Agency:</b> <span style='color: #DAA520; font-weight: bold;'>{agency}</span></p>"
            else:
                res_html += f"<p style='margin: 4px 0;'><b>Ownership:</b> <span style='color:#888;'>No parcel data available</span></p>"

    res_html += f"""
    <div style='background: #f4f6f4; border: 1px solid #ccc; padding: 8px; border-radius: 4px; margin-top: 10px; margin-bottom: 6px;'>
        <h5 style='margin: 0 0 6px 0; color: #2F4F4F; border-bottom: 1px solid #ddd; padding-bottom: 4px;'>Colorado Field Resources</h5>
        <a href='https://cpw.widen.net/s/bklvf2bjmw/colorado-small-game--waterfowl-brochure' target='_blank' style='display:block; margin-top:4px; color: #228B22; text-decoration: none; font-size: 11px;'><b>Official Small Game Regulations</b></a>
        <a href='https://cpw.state.co.us/hunting/small-game/pheasant-and-quail-forecast' target='_blank' style='display:block; margin-top:5px; color: #228B22; text-decoration: none; font-size: 11px;'><b>Pheasant & Quail Forecast</b></a>
        <a href='https://cpw.state.co.us/activities/hunting/where-hunt/walk-access-program' target='_blank' style='display:block; margin-top:5px; color: #D2691E; text-decoration: none; font-size: 11px;'><b>Walk-In Access (WIA) Program</b></a>
        <a href='https://www.cpwshop.com/' target='_blank' style='display:block; margin-top:5px; color: #B22222; text-decoration: none; font-size: 11px;'><b>Purchase License / Habitat Stamp</b></a>
    </div>
    """
    
    hab_html = ""

    if is_valid(pheasant):
        p_color = "#8B0000" if "High" in pheasant else "#D2691E"
        hab_html += f"<p style='margin: 4px 0;'><b>Pheasant Activity:</b> <span style='color: {p_color}; font-weight: bold;'>{pheasant}</span></p><hr style='margin: 6px 0;'>"
    
    if is_valid(crop): 
        hab_html += f"<p style='margin: 4px 0;'><b>USDA Crop:</b> <span style='color:green; font-weight:bold;'>{crop}</span></p>"
    
    has_wia = is_valid(wia_rules) or is_valid(wia_close) or is_valid(wia_habitat) or is_valid(wia_url)
    if has_wia:
        hab_html += f"<hr style='margin: 6px 0;'><p style='margin: 4px 0;'><b>Walk-In Access:</b> <span style='color:#FF4500; font-weight:bold;'>Yes</span></p>"
        if is_valid(wia_rules): 
            hab_html += f"<p style='margin: 4px 0;'><b>Rules:</b> {wia_rules}</p>"
        if is_valid(wia_close): 
            hab_html += f"<p style='margin: 4px 0;'><b>Closes:</b> {wia_close}</p>"
        if is_valid(wia_url):
            hab_html += f"<a href='{wia_url}' target='_blank' style='display:block; margin-top:4px; color: #FF4500; font-weight: bold;'>View WIA Details</a>"
            
    if is_valid(water) and is_valid(dist):
        hab_html += f"<hr style='margin: 6px 0;'><p style='margin: 4px 0; color: #1E90FF;'><b>Nearest Water:</b> {water} ({dist} mi)</p>"

    if not hab_html: 
        hab_html = "<p style='margin: 4px 0; color: #888;'><i>No specific habitat data mapped here.</i></p>"

    html = f"""
    <div style='min-width: 280px; font-family: sans-serif; font-size: 13px;'>
        <div style='background: #228B22; color: white; padding: 6px; text-align: center; border-radius: 4px 4px 0 0;'><h4 style='margin: 0;'>Unit {TARGET_GMU} Intel</h4></div>
        <div style="display: flex; background: #eee; border-bottom: 2px solid #ccc;">
            <button onclick="document.getElementById('res-{idx}').style.display='block'; document.getElementById('hab-{idx}').style.display='none';" style="flex: 1; padding: 8px; cursor: pointer; border: none; background: none; font-weight: bold; font-size: 12px; color: #333;">Field Resources</button>
            <button onclick="document.getElementById('hab-{idx}').style.display='block'; document.getElementById('res-{idx}').style.display='none';" style="flex: 1; padding: 8px; cursor: pointer; border: none; background: none; font-weight: bold; font-size: 12px; color: #333;">Habitat</button>
        </div>
        <div id="res-{idx}" style="display: block; padding: 10px; background: white; border: 1px solid #ccc; border-top: none; height: 210px; overflow-y: auto;">{res_html}</div>
        <div id="hab-{idx}" style="display: none; padding: 10px; background: white; border: 1px solid #ccc; border-top: none; height: 210px; overflow-y: auto;">{hab_html}</div>
    </div>
    """
    return html

def build_master_scout_map():
    engine = create_engine(DB_URL)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logging.info(f"Compiling Enterprise Presentation Map for Unit {TARGET_GMU}...")

    try:
        fabric_gdf = safe_load_layer(f"scout_fabric_{TARGET_GMU}", engine)
        gmu_gdf = safe_load_layer(f"gmu_boundary_{TARGET_GMU}", engine)
        all_gmus_gdf = safe_load_layer(f"all_gmus_{TARGET_GMU}", engine)
        co_roads_gdf = safe_load_layer(f"co_roads_{TARGET_GMU}", engine)
        cotrex_trails_gdf = safe_load_layer(f"cotrex_trails_{TARGET_GMU}", engine)
        hydro_gdf = safe_load_layer(f"nhd_hr_water_{TARGET_GMU}", engine)
        camp_gdf = safe_load_layer(f"campgrounds_{TARGET_GMU}", engine)
        wia_gdf = safe_load_layer(f"walk_in_access_{TARGET_GMU}", engine)
        blm_gdf = safe_load_layer(f"surface_ownership_{TARGET_GMU}", engine)
        crops_gdf = safe_load_layer(f"map_crop_habitat_{TARGET_GMU}", engine)
        parcels_gdf = safe_load_layer(f"co_parcels_{TARGET_GMU}", engine)
        ph_conc_gdf = safe_load_layer(f"pheasant_conc_{TARGET_GMU}", engine)
        ph_over_gdf = safe_load_layer(f"pheasant_overall_{TARGET_GMU}", engine)
        gold_gdf = safe_load_layer(f"prime_gold_zones_{TARGET_GMU}", engine)

        public_only_gdf = gpd.GeoDataFrame()
        mapped_agencies = {}
        if not blm_gdf.empty and 'adm_manage' in blm_gdf.columns:
            public_only_gdf = blm_gdf[blm_gdf['adm_manage'] != 'PRI']
            if not public_only_gdf.empty:
                color_idx = 0
                for agency in public_only_gdf['adm_manage'].dropna().unique():
                    raw_name = str(agency).strip().upper()
                    if raw_name.lower() in ['unknown', 'nan', 'none', 'null', '']: continue
                    if 'USFS' in raw_name: raw_name = 'USFS'
                    
                    short_name = AGENCY_SHORT_CODE.get(raw_name, raw_name)
                    if short_name not in mapped_agencies:
                        if short_name in KNOWN_AGENCIES:
                            mapped_agencies[short_name] = KNOWN_AGENCIES[short_name]
                        else:
                            mapped_agencies[short_name] = FALLBACK_COLORS[color_idx % len(FALLBACK_COLORS)]
                            color_idx += 1

        mapped_crops = {}
        if not crops_gdf.empty and 'cover_type' in crops_gdf.columns:
            color_idx = 0
            for crop in crops_gdf['cover_type'].dropna().unique():
                if str(crop).strip().lower() in ['unknown', 'nan', 'none', 'null', '']: continue
                mapped_crops[str(crop)] = next((color for key, color in KNOWN_CROPS.items() if key in str(crop).lower()), FALLBACK_COLORS[(color_idx + 4) % len(FALLBACK_COLORS)])
                color_idx += 1

        def dynamic_crop_style(feature):
            crop = str(feature.get('properties', {}).get('cover_type', ''))
            return {'color': mapped_crops.get(crop, '#8B4513'), 'weight': 1, 'fillColor': mapped_crops.get(crop, '#8B4513'), 'fillOpacity': 0.5, 'className': 'noclick'}

        def dynamic_water_style(feature):
            if 'intermittent' in str(feature.get('properties', {}).get('feature', '')).lower():
                return {'color': '#87CEFA', 'weight': 1.0, 'opacity': 0.5, 'dashArray': '5, 5', 'className': 'noclick'} 
            return {'color': '#1E90FF', 'weight': 1.5, 'opacity': 0.6, 'className': 'noclick'} 

        if not gmu_gdf.empty and not gmu_gdf.geometry.is_empty.all():
            center_y, center_x = gmu_gdf.geometry.centroid.y.mean(), gmu_gdf.geometry.centroid.x.mean()
        elif not fabric_gdf.empty and not fabric_gdf.geometry.is_empty.all():
            center_y, center_x = fabric_gdf.geometry.centroid.y.mean(), fabric_gdf.geometry.centroid.x.mean()
        else:
            center_y, center_x = 40.0, -103.0 

        m = folium.Map(location=[center_y, center_x], zoom_start=11, control_scale=True, tiles=None, prefer_canvas=False)
        
        svg_patterns = """
        <svg style="height: 0; width: 0; position: absolute;" aria-hidden="true">
            <defs>
        """
        for short_name, hex_color in mapped_agencies.items():
            safe_id = short_name.replace(' ', '_').replace('/', '_')
            svg_patterns += f"""
                <pattern id="pat-{safe_id}" width="220" height="220" patternUnits="userSpaceOnUse" patternTransform="rotate(-45)">
                    <line x1="0" y1="110" x2="220" y2="110" stroke="{hex_color}" stroke-width="2.5" opacity="0.6" />
                    <text x="110" y="105" font-family="sans-serif" font-size="14" font-weight="900" fill="{hex_color}" opacity="0.75" text-anchor="middle" letter-spacing="1">{short_name}</text>
                </pattern>
            """
        svg_patterns += "</defs></svg>"
        m.get_root().html.add_child(folium.Element(svg_patterns))

        custom_css_js = """
        <style>
            path.noclick { pointer-events: none !important; }
            .leaflet-control-layers-list { max-height: 35vh; overflow-y: auto; overflow-x: hidden; padding-right: 10px; }
            .north-arrow { position: fixed; bottom: 70px; left: 15px; width: 35px; height: 35px; z-index:9999; pointer-events:none; }
            .hide-tooltips .leaflet-tooltip-pane { visibility: hidden !important; display: none !important; }
        </style>
        <div class="north-arrow">
            <svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
                <polygon points="50,0 20,100 50,80 80,100" fill="rgba(0,0,0,0.6)" stroke="white" stroke-width="2"/>
                <text x="50" y="70" fill="white" font-size="24" font-family="sans-serif" text-anchor="middle" font-weight="bold">N</text>
            </svg>
        </div>
        <script>
            document.addEventListener("DOMContentLoaded", function() {
                setTimeout(function() {
                    var map_keys = Object.keys(window).filter(k => k.startsWith('map_'));
                    if(map_keys.length > 0) {
                        var myMap = window[map_keys[0]];
                        function toggleTooltips() {
                            var mapContainer = myMap.getContainer();
                            if (myMap.getZoom() < 13) {
                                mapContainer.classList.add('hide-tooltips');
                            } else {
                                mapContainer.classList.remove('hide-tooltips');
                            }
                        }
                        myMap.on('zoomend', toggleTooltips);
                        toggleTooltips();
                    }
                }, 1000);
            });
        </script>
        """
        m.get_root().html.add_child(folium.Element(custom_css_js))
        
        folium.TileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', attr='Esri', name='Esri Satellite').add_to(m)
        folium.TileLayer('https://services.arcgisonline.com/ArcGIS/rest/services/Reference/World_Transportation/MapServer/tile/{z}/{y}/{x}', attr='Esri', name='Esri Roads & Labels', overlay=True, control=True, show=False).add_to(m)
        folium.TileLayer('https://services.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}', attr='Esri', name='Esri City Labels', overlay=True, control=True, show=True).add_to(m)
        folium.TileLayer('OpenStreetMap', name='OpenStreetMap (Topo)').add_to(m)

        if not gmu_gdf.empty: 
            folium.GeoJson(
                gmu_gdf, 
                name="GMU Boundary", 
                show=True,
                style_function=lambda x: {'color': 'black', 'weight': 4, 'fillColor': 'transparent', 'className': 'noclick'}
            ).add_to(m)
            
        if not all_gmus_gdf.empty:
            def statewide_gmu_style(feature):
                gmu_id = str(feature.get('properties', {}).get('gmuid', ''))
                if gmu_id == str(TARGET_GMU): 
                    return {'color': 'transparent', 'weight': 0, 'fillOpacity': 0, 'className': 'noclick'}
                return {'color': '#555555', 'weight': 1.5, 'dashArray': '5, 5', 'fillColor': 'transparent', 'className': 'noclick'}
            
            statewide_group = folium.FeatureGroup(name="Colorado GMU Boundaries", show=True)
            folium.GeoJson(all_gmus_gdf, style_function=statewide_gmu_style).add_to(statewide_group)
            
            for _, row in all_gmus_gdf.iterrows():
                if pd.notnull(row.geometry) and not row.geometry.is_empty:
                    g_id = str(row.get('gmuid', ''))
                    if g_id and g_id != str(TARGET_GMU):
                        pt = row.geometry.representative_point()
                        lbl = f'<div style="font-size:12px; color:#555555; font-weight:bold; text-shadow:1px 1px 1px #fff; transform:translate(-50%,-50%);">GMU {g_id}</div>'
                        folium.Marker(location=[pt.y, pt.x], icon=folium.DivIcon(html=lbl), interactive=False).add_to(statewide_group)
            statewide_group.add_to(m)

        public_labels = folium.FeatureGroup(name="Public Land", show=True)
        if not public_only_gdf.empty: 
            def public_land_pattern(feature):
                agency_raw = str(feature.get('properties', {}).get('adm_manage', '')).strip().upper()
                if 'USFS' in agency_raw: agency_raw = 'USFS'
                short_name = AGENCY_SHORT_CODE.get(agency_raw, agency_raw)
                safe_id = short_name.replace(' ', '_').replace('/', '_')
                color = mapped_agencies.get(short_name, '#DAA520')
                return {'color': color, 'weight': 2.5, 'fillColor': f'url(#pat-{safe_id})', 'fillOpacity': 1.0, 'className': 'noclick'}
                
            folium.GeoJson(
                public_only_gdf, 
                style_function=public_land_pattern
            ).add_to(public_labels)
        public_labels.add_to(m)

        if not parcels_gdf.empty: 
            folium.GeoJson(
                parcels_gdf, 
                name="Private Parcels", 
                show=False, 
                style_function=lambda x: {'color': 'white', 'weight': 0.5, 'fillOpacity': 0, 'className': 'noclick'}
            ).add_to(m)

        if not ph_conc_gdf.empty: 
            folium.GeoJson(
                ph_conc_gdf, 
                name="Pheasant High Concentration", 
                show=True, 
                style_function=lambda x: {'color': '#D2691E', 'weight': 1.5, 'fillColor': '#D2691E', 'fillOpacity': 0.25, 'className': 'noclick'}
            ).add_to(m)

        if not ph_over_gdf.empty: 
            folium.GeoJson(
                ph_over_gdf, 
                name="Pheasant Overall Range", 
                show=False, 
                style_function=lambda x: {'color': '#B8860B', 'weight': 1, 'fillColor': '#DAA520', 'fillOpacity': 0.2, 'className': 'noclick'}
            ).add_to(m)

        if not crops_gdf.empty: 
            folium.GeoJson(
                crops_gdf, 
                name="USDA Crop Habitat", 
                show=False, 
                style_function=dynamic_crop_style
            ).add_to(m)

        if not wia_gdf.empty: 
            folium.GeoJson(
                wia_gdf, 
                name="Walk-In Access", 
                show=True, 
                style_function=lambda x: {'color': '#FFD700', 'weight': 1.5, 'fillColor': '#FFD700', 'fillOpacity': 0.3, 'className': 'noclick'}
            ).add_to(m)

        if not gold_gdf.empty:
            folium.GeoJson(
                gold_gdf, 
                name="Prime Habitat / Access", 
                show=True, 
                style_function=lambda x: {'color': '#FF0000', 'weight': 2, 'fillColor': '#FFD700', 'fillOpacity': 0.6, 'dashArray': '3, 3', 'className': 'noclick'}
            ).add_to(m)

        if not hydro_gdf.empty: 
            folium.GeoJson(
                hydro_gdf, 
                name="Rivers & Streams", 
                show=True, 
                style_function=dynamic_water_style,
                tooltip=folium.GeoJsonTooltip(fields=['name'], aliases=['Water:']) if 'name' in hydro_gdf.columns else None
            ).add_to(m)

        if not co_roads_gdf.empty: 
            folium.GeoJson(
                co_roads_gdf, 
                name="Local Roads", 
                show=False, 
                style_function=style_roads
            ).add_to(m)
            
        if not cotrex_trails_gdf.empty: 
            folium.GeoJson(
                cotrex_trails_gdf, 
                name="Trails (COTREX)", 
                show=False, 
                style_function=style_trails
            ).add_to(m)

        if not camp_gdf.empty and 'geometry' in camp_gdf.columns:
            camp_layer = folium.FeatureGroup(name="State Wildlife Areas", show=True)
            for idx, row in camp_gdf.iterrows():
                if pd.notnull(row.geometry) and not row.geometry.is_empty:
                    point = row.geometry.centroid
                    name = str(row.get('propname', row.get('rec_name', 'Facility')))
                    prop_type = str(row.get('proptype', 'Recreation Area'))
                    acres = round(float(row.get('acres', 0)), 1) if row.get('acres') else "Unknown"
                    
                    is_swa = 'SWA' in name.upper() or 'SWA' in prop_type.upper()
                    icon_color = 'darkgreen' if is_swa else 'green'
                    icon_prefix = 'tree' if is_swa else 'campground'
                    
                    tooltip_html = f"<div style='font-family: sans-serif; font-size: 12px;'><b>{name}</b><hr style='margin: 4px 0;'>Type: {prop_type}<br>Acres: {acres}</div>"
                    folium.Marker(location=[point.y, point.x], icon=folium.Icon(color=icon_color, icon=icon_prefix, prefix='fa'), tooltip=tooltip_html).add_to(camp_layer)
            camp_layer.add_to(m)

        # ---------------------------------------------------------
        # THE MASTER CLICK LAYER 
        # ---------------------------------------------------------

            def create_search_index(r):
                addr = str(r.get('physical_address', '')).strip()
                if addr and addr.lower() not in ['nan', 'none', '']: return addr
                return 'Unknown'
                
            fabric_gdf['search_index'] = fabric_gdf.apply(create_search_index, axis=1)
            
            def create_hover_tooltip(r):
                owner = str(r.get('private_owner', '')).strip()
                addr = str(r.get('physical_address', '')).strip()
                agency_raw = str(r.get('public_agency', '')).strip()
                if 'USFS' in agency_raw.upper(): agency_raw = 'USFS'
                crop = str(r.get('crop_type', '')).strip()
                
                res = f"<div style='font-family: sans-serif; font-size: 12px; min-width: 150px;'>"
                
                # hover label matching
                is_pub_parcel = False
                owner_valid = owner and owner.lower() not in ['nan', 'none', '', 'data gap (requires supplement)']
                
                if owner_valid:
                    owner_clean = owner.lower()
                    fed_kws = ['u s a', 'usa', 'united states', 'blm', 'bureau of land', 'forest service', 'national forest']
                    state_kws = ['state of', 'colorado state', 'parks and wildlife', 'board of land', 'dept of', 'department of']
                    local_kws = ['town of', 'city of', 'county', 'municipal', 'school district']
                    if any(k in owner_clean for k in fed_kws + state_kws + local_kws):
                        is_pub_parcel = True
                
                agency_clean = AGENCY_TRANSLATOR.get(agency_raw.upper(), agency_raw)
                
                # Federal data ALWAYS overrides county "U S A" data if it exists
                if agency_raw and agency_raw.lower() not in ['nan', 'none', ''] and (not owner_valid or is_pub_parcel):
                    res += f"<b>Public Land:</b> {agency_clean}<br>"
                elif owner_valid:
                    if is_pub_parcel:
                        pretty_owner = owner.title().replace('U S A', 'US Govt').replace('Usa', 'US Govt')
                        res += f"<b>Public Land:</b> {pretty_owner}<br>"
                    else:
                        res += f"<b>Landowner:</b> {owner}<br>"
                        if addr and addr.lower() not in ['nan', 'none', '']: res += f"<b>Address:</b> {addr}<br>"
                else:
                    res += "<b>Owner:</b> No Parcel Data Available<br>"
                    
                if crop and crop.lower() not in ['nan', 'none', '']:
                    res += f"<b>Crop:</b> {crop.title()}<br>"
                    
                res += "<i style='color:#888;'>(Click for full intel)</i></div>"
                return res
                
            fabric_gdf['hover_tooltip'] = fabric_gdf.apply(create_hover_tooltip, axis=1)
            fabric_gdf['popup_html'] = [generate_tabbed_popup(row, f"fab_{idx}") for idx, row in fabric_gdf.iterrows()]
            intel_layer = folium.GeoJson(
                fabric_gdf, 
                style_function=lambda x: {'color': 'transparent', 'fillColor': 'white', 'fillOpacity': 0.01, 'weight': 0},
                popup=folium.GeoJsonPopup(fields=['popup_html'], labels=False),
                tooltip=folium.GeoJsonTooltip(fields=['hover_tooltip'], labels=False),
                control=False 
            ).add_to(m)
            
            Search(layer=intel_layer, geom_type='Polygon', placeholder='Search Physical Address...', collapsed=False, search_label='search_index').add_to(m)

        MeasureControl(position='topleft', primary_length_unit='miles').add_to(m)
        Fullscreen().add_to(m)
        Draw(export=True, position='topleft', edit_options={'edit': False, 'remove': False}).add_to(m)
        folium.LayerControl(position='topright', collapsed=False).add_to(m)

        legend_html = f'''
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css"/>
        <div style="position: fixed; bottom: 30px; right: 10px; width: 250px; max-height: 45vh; overflow-y: auto; overflow-x: hidden;
                    z-index:9999; background: white; padding: 10px; border: 2px solid #333; border-radius: 6px; font-family: sans-serif; font-size: 11px; box-shadow: 2px 2px 5px rgba(0,0,0,0.3);">
            <h4 style="margin:0 0 6px 0; color:#2F4F4F; font-size: 13px;"><b>Scout Legend</b></h4>
            <i style="background:#FFFACD;width:10px;height:10px;display:inline-block;border:2px dashed #DAA520; opacity:0.8;"></i> <b>Prime Habitat / Access</b><br>
            <i style="background:#FFD700;width:10px;height:10px;display:inline-block;border:1px solid #000; opacity:0.5;"></i> Walk-In Access<br>
            <i style="background:#ffffff;width:10px;height:10px;display:inline-block;border:1px solid #555555; opacity:0.8;"></i> Private Boundaries<br>
            <hr style="margin:6px 0;"><b>Pheasant Habitat</b><br>
            <i style="background:#D2691E;width:10px;height:10px;display:inline-block;border:1px solid #D2691E; opacity:0.35;"></i> High Concentration<br>
            <i style="background:#DAA520;width:10px;height:10px;display:inline-block;border:1px solid #B8860B; opacity:0.2;"></i> Overall Range<br>
        '''
        if mapped_agencies:
            legend_html += '<hr style="margin:6px 0;"><b>Public Land</b><br>'
            for agency, color in mapped_agencies.items():
                legend_html += f'<i style="background:transparent;width:10px;height:10px;display:inline-block;border:2px solid {color};"></i> {agency}<br>'
        
        legend_html += '<hr style="margin:6px 0;"><b>USDA Crops</b><br>'
        for crop, color in mapped_crops.items():
            legend_html += f'<i style="background:{color};width:10px;height:10px;display:inline-block;border:1px solid #000; opacity:0.6;"></i> {str(crop).title()}<br>'

        legend_html += '''
            <hr style="margin:6px 0;">
            <i style="border-bottom:2px solid #1E90FF;width:15px;height:0px;display:inline-block;margin-bottom:3px; opacity:0.6;"></i> River / Stream<br>
            <i style="border-bottom:2px solid #ffffff;width:15px;height:0px;display:inline-block;margin-bottom:3px;background-color:#ccc;"></i> Maintained Roads<br>
            <i style="border-bottom:2px dashed #FF1493;width:15px;height:0px;display:inline-block;margin-bottom:3px;"></i> Trails<br>
            <hr style="margin:6px 0;">
            <i class="fas fa-tree" style="color: darkgreen; width: 15px; text-align: center;"></i> <b>State Wildlife Area (SWA)</b><br>
            <i class="fas fa-campground" style="color: green; width: 15px; text-align: center;"></i> <b>Campground</b>
        </div>
        '''
        m.get_root().html.add_child(folium.Element(legend_html))
        m.save(OUTPUT_FILE)
        logging.info(f"Spatial rendering complete: {OUTPUT_FILE}")

    except Exception as e:
        logging.error(f"Map Build Failed: {e}")
        traceback.print_exc()

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
    build_master_scout_map()
    check_logs_for_issues()