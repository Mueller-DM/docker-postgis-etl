Automated Spatial ETL & Conflation Pipeline

Project Objective This project demonstrates an automated, reproducible Spatial ETL pipeline designed to ingest, vectorize, and spatially flatten highly fragmented micro-habitats and land ownership boundaries.

While the output of this specific pipeline is geared toward environmental modeling and recreational access (specifically upland bird habitats in Colorado), the core engineering focus is on complex property conflation, dynamic data normalization, and containerized spatial architecture. It demonstrates the ability to transform raw, disparate spatial APIs into a performant, unified spatial fabric.

Data Provenance & Dynamic Ingestion To maintain a scalable, production-level architecture, this project avoids relying on static, manually downloaded shapefiles. Instead, the ingestion phase programmatically extracts all raw vector data directly from authoritative REST endpoints.

Vector APIs: Extracts dynamic JSON payloads directly from authoritative government REST endpoints (Colorado Parks and Wildlife, BLM, State of Colorado OIT, USGS).

Raster-to-Vector: Hits the USGS WCS API, dynamically crops the NLCD GeoTIFF to the target bounding box, and utilizes rasterio to trace biological pixels into PostGIS vector polygons.

System Architecture & Reproducibility To ensure 100% reproducibility and eliminate local OS dependency conflicts (specifically GDAL/C++ bindings), the entire ETL architecture is fully containerized.

Docker Compose Orchestration:

Storage Engine: postgis/postgis:15-3.3 (Spatial Database)

ETL Engine: python:3.11-slim (Debian container pre-loaded with GDAL, GEOS, and PROJ binaries)

Relational Database Design: The pipeline is architected as a true RDBMS. Spatial queries leverage aggressive GiST indexing for performance, while the tabular data strictly enforces Primary Key / Foreign Key constraints, ensuring absolute data integrity downstream.

Pipeline Execution Flow Phase 1: Automated Data Collection (01_ingest.py)

Standardizes all coordinate systems to EPSG:4326 using GeoPandas.

Architecture Note: This demonstration utilizes an iterative Python request approach for targeted extraction. A production rollout would shift to an ELT architecture, bulk-syncing state-wide datasets into a cloud data warehouse before executing spatial conflation natively within the database.

Phase 2: Enterprise Spatial Analysis (02_analysis.py)

Leverages advanced PostGIS SQL (ST_Intersection, ST_SquareGrid, ST_MakeValid) to flatten overlapping land agencies, private parcels, and agricultural data into a continuous spatial fabric.

Executes automatic conflation logic to resolve boundary shifts (e.g., resolving federal polygons bleeding over private tax parcels).

Phase 3: Dynamic Map Generation (03_generate_map.py)

Compiles the PostGIS fabric into a standalone interactive HTML map for QA and presentation.

Execution Instructions Security Note: For the purpose of this demonstration, database credentials are included in the docker-compose.yml. In a production environment, these would be managed dynamically via a Secret Manager or environment variables.

Ensure Docker Desktop is running. Open your terminal and navigate into the project directory:

Bash cd path/to/docker-postgis-etl Option A: Automated Run (Default Target Area) This command spins up the PostGIS database, builds the Python ETL container, and executes Phase 1, 2, and 3 in sequence.

Bash docker-compose up --build Option B: Step-by-Step Modular Execution This pipeline is engineered to scale dynamically. You can execute the pipeline step-by-step and pass a custom target variable (e.g., targeting a different management unit like 88).

Bash

1. Spin up the PostGIS Database in the background
docker-compose up -d postgis_db

2. Run Phase 1: Data Ingestion (Targeting GMU 88)
docker-compose run -e TARGET_GMU=88 --rm etl_pipeline python scripts/01_ingest.py

3. Run Phase 2: Spatial Conflation & Analysis
docker-compose run -e TARGET_GMU=88 --rm etl_pipeline python scripts/02_analysis.py

4. Run Phase 3: Map Generation
docker-compose run -e TARGET_GMU=88 --rm etl_pipeline python scripts/03_generate_map.py Viewing the Results & Logging Output: Once the final phase completes, navigate to the local outputs/ folder and open the generated HTML file. (Note: Folium is used here strictly to provide a zero-dependency, standalone demonstration. Production environments would utilize a dynamic vector tile server like Tippecanoe/PMTiles served via FastAPI).

QA/QC Logging: Check the logs/pipeline_qa.log file. Any API degradation, connection timeouts, or schema gaps encountered during the run are automatically captured here.
