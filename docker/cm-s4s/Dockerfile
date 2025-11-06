FROM sen4x/gdal-py:3.3.2

RUN apt-get update && \
    apt-get install python3-xlsxwriter && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY export-product-launcher.py /usr/local/bin
COPY export-product.py /usr/local/bin
COPY import-product-details.py /usr/local/bin
COPY crop-type-parcels2.py /usr/local/bin
COPY sample-selection.py /usr/local/bin
COPY crop-map-s4s.py /usr/local/bin
COPY product-report.py /usr/local/bin
