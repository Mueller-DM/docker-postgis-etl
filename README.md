# onX Technical Challenge: Upland Hunting Intelligence (GMU 102)

## Project Objective
This project delivers an automated, reproducible Spatial ETL pipeline and an interactive web map designed for a first-time Upland Bird (Pheasant) hunter in Colorado's GMU 102. 

While Big Game hunting relies on macro-spatial analysis (e.g., 100,000-acre National Forest blocks), Upland hunting is a game of micro-spatial intersections. A successful pheasant hunt requires the precise overlap of physical habitat—Food (Cultivated Crops) and Cover (Wetlands/Grasslands)—with legal Access (Walk-In properties) and verified biological presence (CPW Pheasant Concentration ranges). 

This pipeline was engineered to demonstrate how to ingest, vectorize, and spatially flatten highly fragmented micro-habitats into a performant UI, proving the ability to handle complex property conflation and data normalization.

---
## Data Sources & Provenance
**A Note on "Included Datasets" for Grading:** To fulfill the requirement to include all datasets used in map creation while maintaining a scalable, production-level architecture, this project does not rely on static, manually downloaded shapefiles. Instead, the `01_ingest.py` script programmatically extracts all raw vector data directly from the authoritative REST endpoints below. The only "static" dataset included in the `data/raw/` directory is the clipped NLCD GeoTIFF, as the WCS endpoint for raster extraction is exceptionally slow. All intermediate and normalized vector datasets are generated dynamically within the PostGIS container during execution.

To ensure data accuracy and pipeline reproducibility, all datasets are pulled dynamically from authoritative government REST and WCS endpoints during the ingestion phase. No static shapefiles are hardcoded.

* **Colorado Parks and Wildlife (CPW) Admin API:** GMU Boundaries, Walk-In Access (WIA) properties, State Wildlife Areas (Campgrounds/Facilities), and COTREX Trails.
* **Colorado Parks and Wildlife (CPW) Species API:** Pheasant High Concentration and Pheasant Overall Range geometries.
* **Bureau of Land Management (BLM) National Data:** Colorado Surface Management Agency (SMA) dataset for federal and state public land boundaries.
* **State of Colorado GIS (OIT):** Colorado Public/Private Tax Parcels and Maintained County Roads.
* **USGS / USFWS:** National Hydrography Dataset (NHD) High Resolution for rivers and intermittent streams.
* **USDA National Agricultural Statistics Service (NASS):** CropScape (NLCD) 2023 30m resolution raster data, used to extract specific agricultural vectors (Corn, Sorghum, Wheat, etc.).

---

## System Architecture & Reproducibility
Relational Database Design: The pipeline is architected as a true RDBMS. While spatial queries leverage aggressive GiST indexing for performance, the tabular data (e.g., dynamic hunting regulations/shooting hours) strictly enforces Primary Key / Foreign Key constraints linked to the authoritative GMU boundaries, ensuring absolute data integrity downstream.

To ensure 100% reproducibility and eliminate local OS dependency conflicts (specifically GDAL/C++ bindings), the entire ETL architecture is containerized. 

Reviewers do not need to manage virtual environments or configure a local database. The pipeline is orchestrated via Docker Compose:

* **Storage Engine:** `postgis/postgis:15-3.3` (Spatial Database)
* **ETL Engine:** `python:3.11-slim` (Debian container pre-loaded with GDAL, GEOS, and PROJ binaries)

---

## Pipeline Execution Flow

### Phase 1: Automated Data Collection (`01_ingest.py`)
* **Vector APIs:** Extracts dynamic JSON payloads directly from authoritative REST endpoints. GeoPandas standardizes all coordinate systems to EPSG:4326.
* **Raster-to-Vector:** Hits the USGS WCS API, dynamically crops the NLCD GeoTIFF to the GMU boundary, and utilizes Rasterio to trace biological pixels (Agriculture, Wetlands) into PostGIS vector polygons.

> **Pipeline Architecture Note:** The ingestion phase utilizes an iterative ETL approach (Python `requests` with pagination), dynamically pulling only the target GMU bounding box. This was engineered specifically to respect the project time limit and avoid hitting CPW API rate limits during grading. A production nationwide rollout would shift to an ELT architecture—bulk-syncing the entire state dataset into a cloud data warehouse (e.g., Snowflake/PostGIS) via tools like Airbyte, executing the spatial conflation entirely within the database.

### Phase 2: Enterprise Spatial Analysis (`02_analysis.py`)
* Uses advanced PostGIS SQL (`ST_Intersection`, `ST_SquareGrid`, `ST_MakeValid`) to flatten overlapping land agencies, private parcels, and agricultural data into a single, continuous "Master Scout Fabric". 
* Executes automatic conflation logic to resolve Federal-to-County boundary shifts (e.g., USFS polygon bleeding over private tax parcels).

### Phase 3: Dynamic Map Generation (`03_generate_map.py`)
* Compiles the PostGIS fabric into an interactive HTML map with zoom-dependent label rendering, dynamic SVG crosshatching for public lands, and a unified hunter dashboard.

---

## Execution Instructions

> **Security & Configuration Note:** For the purpose of this interview, database credentials (`postgrespassword`) are hardcoded in the `docker-compose.yml` and Python scripts. This is an intentional decision to guarantee a "zero-configuration" grading experience for the review team. In a production environment, these hardcoded fallbacks would be removed, and credentials would be injected dynamically via a Secret Manager or `.env` file omitted from version control.

Ensure Docker Desktop is running on your machine. Open your terminal and navigate into the unzipped project directory:

```bash
cd path/to/David_Mueller_onX_Technical
```

### Option A: The One-Click Automated Run (Default GMU 102)
This single command orchestrates the entire environment. It spins up the PostGIS database, builds the Python ETL container with GDAL dependencies, and automatically executes Phase 1, Phase 2, and Phase 3 in sequence for the default unit.

```bash
docker-compose up --build
```

### Option B: Step-by-Step Execution (Testing Dynamic GMUs)
This pipeline was engineered to scale dynamically across the state. To test this modularity, you can execute the pipeline step-by-step and pass a custom target variable (e.g., running GMU 88 or 98 instead of 102).

```bash
# 1. Spin up the PostGIS Database in the background
docker-compose up -d postgis_db

# 2. Run Phase 1: Data Ingestion (Targeting GMU 88)
docker-compose run -e TARGET_GMU=88 --rm etl_pipeline python scripts/01_ingest.py

# 3. Run Phase 2: Spatial Conflation & Analysis
docker-compose run -e TARGET_GMU=88 --rm etl_pipeline python scripts/02_analysis.py

# 4. Run Phase 3: Map Generation
docker-compose run -e TARGET_GMU=88 --rm etl_pipeline python scripts/03_generate_map.py
```

### Viewing the Results
Once the terminal completes the final phase, navigate to the local `outputs/` folder in your file explorer and double-click the generated HTML file to open it in any web browser.

**Cartography Note:** The final presentation layer utilizes Folium to generate a standalone HTML file. While a production environment would utilize a dynamic vector tile server (e.g., Tippecanoe / PMTiles served via FastAPI to a Mapbox GL JS frontend), Folium was chosen strictly for this deliverable to guarantee a zero-dependency, single-file grading experience for the review team.

**QA/QC Logging:**
Check the `logs/pipeline_qa.log` file. The pipeline utilizes a dual-handler logger; while terminal output remains quiet, any API degradation, connection timeouts, or schema gaps encountered during the run are automatically captured in this persistent log file for manual review.