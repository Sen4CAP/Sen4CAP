FROM osgeo/gdal:ubuntu-full-3.4.1

LABEL maintainer="cosmin@c-s.ro" \
    description="Sen4Stat scripts"

RUN apt-get update && \
    apt-get install -y python3-pip && apt-get -y install binutils libproj-dev libgdal-dev  gdal-bin &&  \
    apt-get install -y python3-lxml python3-pip python3-psycopg2 python3-toml && apt-get clean && \
    rm -rf /var/lib/apt/lists/* && python3 -m pip install docker

RUN python3 -m pip install pandas~=1.1.5 numpy~=1.19.5 scipy~=1.5.3 lxml~=4.6.2 pyarrow~=5.0.0 scikit-learn mlxtend
# RUN python3 -m pip install gdal pandas numpy scipy lxml

COPY s4s_perm_crops/* /usr/bin/
COPY s4s_yield/* /usr/bin/


