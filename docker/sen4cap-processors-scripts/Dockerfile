FROM osgeo/gdal:ubuntu-full-3.4.1

LABEL maintainer="cosmin@c-s.ro" \
    description="Sen4CAP processors scripts"

RUN apt-get update && \
    apt-get install -y python3-pip && apt-get -y install binutils libproj-dev libgdal-dev  gdal-bin &&  \
    apt-get install -y python3-lxml python3-pip python3-psycopg2 python3-toml && apt-get clean && \
    rm -rf /var/lib/apt/lists/* && python3 -m pip install docker

RUN python3 -m pip install pandas~=1.1.5 numpy~=1.20.3 scipy~=1.5.3 lxml~=4.6.2 pyarrow~=5.0.0 scikit-learn mlxtend seaborn tqdm
# RUN python3 -m pip install gdal pandas numpy scipy lxml

COPY heterogeneity/* /usr/bin/
COPY s4c_bare_soil/*.py /usr/bin
COPY s4c_change_detection/*.py /usr/bin
