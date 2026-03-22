FROM python:3.11-slim

# Install GDAL and C++ build tools
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Point GDAL to the right system headers
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p data/raw outputs

CMD ["sh", "-c", "python scripts/01_ingest.py && python scripts/02_analysis.py && python scripts/03_generate_map.py"]