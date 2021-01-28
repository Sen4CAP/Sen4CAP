FROM centos:centos7

LABEL name="sen2agri-services" \
      maintainer="Laurentiu Nicola <laurentiu.nicola@c-s.ro>"

RUN yum -y install yum-utils epel-release https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm && \
    yum-config-manager --disable pgdg95 && \
    yum -y update && \
    yum -y install file postgresql12 python-psycopg2 python-dateutil R-devel libcurl-devel openssl-devel libxml2-devel && \
    echo -e 'packages <- c("e1071", "caret", "dplyr", "gsubfn", "ranger", "readr", "smotefamily", "caTools", "tidyverse", "data.table")\n\
             diff <- setdiff(packages, rownames(installed.packages()))\n\
             if (length(diff) > 0) {\n\
                 install.packages(diff, repos = c(CRAN = "https://cran.rstudio.com"))\n\
             }' | Rscript -

RUN yum -y install python3 python3-devel git cmake3 unzip gcc gcc-c++ make curl
RUN curl -LO https://github.com/apache/arrow/archive/apache-arrow-3.0.0.zip
RUN unzip apache-arrow-3.0.0.zip
RUN python3 -m pip install cython~=0.29 numpy~=1.19
RUN ln -s /usr/bin/cmake3 /usr/local/bin/cmake
RUN mkdir arrow-apache-arrow-3.0.0/cpp/build
RUN cd arrow-apache-arrow-3.0.0/cpp/build && \
    cmake3 -DCMAKE_BUILD_TYPE=Release \
           -DARROW_CSV=ON \
           -DARROW_PYTHON=ON \
           -DCMAKE_INSTALL_PREFIX=/usr \
           -DARROW_COMPUTE=ON \
           -DARROW_DATASET=ON \
           -DARROW_FILESYSTEM=ON \
           -DARROW_JEMALLOC=ON \
           -DARROW_JSON=ON \
           -DARROW_PARQUET=ON \
           -DCMAKE_BUILD_TYPE=Release \
           -DARROW_INSTALL_NAME_RPATH=OFF \
           .. && \
    make -j$(nproc)
RUN cd arrow-apache-arrow-3.0.0/cpp/build && \
    make install
RUN cd arrow-apache-arrow-3.0.0/python && \
    python3 setup.py install --prefix /usr

RUN echo -e 'packages <- c("bit64")\n\
             diff <- setdiff(packages, rownames(installed.packages()))\n\
             if (length(diff) > 0) {\n\
                 install.packages(diff, repos = c(CRAN = "https://cran.rstudio.com"))\n\
             }' | Rscript -
RUN cd arrow-apache-arrow-3.0.0/r && \
    R CMD INSTALL .
RUN rm -rf arrow-apache-arrow-3.0.0 apache-arrow-3.0.0.zip

COPY *.rpm /

# install gdal explicitly to prevent otb from pulling in libgeotiff-1.4
RUN yum install -y gdal /otb-*.rpm /sen2agri-*.rpm /gdal-*.rpm && \
    rm /*.rpm && \
    ldconfig
