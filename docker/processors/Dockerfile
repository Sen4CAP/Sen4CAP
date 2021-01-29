FROM centos:centos7

LABEL name='processors' \
    maintainer='Laurentiu Nicola <laurentiu.nicola@c-s.ro>'

RUN yum -y install \
    yum-utils \
    epel-release \
    https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm \
    https://apache.bintray.com/arrow/centos/7/apache-arrow-release-latest.rpm && \
    yum-config-manager --disable pgdg95 && \
    yum -y update && \
    yum -y install \
    gdal \
    file \
    postgresql12 \
    python-psycopg2 \
    python-dateutil \
    R \
    libcurl \
    openssl \
    libxml2 \
    libcurl-devel \
    openssl-devel \
    libxml2-devel \
    python3 \
    python3-devel \
    git \
    cmake3 \
    gcc-c++ \
    arrow-libs-3.0.0 \
    arrow-python-libs-3.0.0 \
    arrow-devel-3.0.0 \
    arrow-python-devel-3.0.0 \
    boost fftw gsl gstreamer opencv qt5-qtbase qt5-qtbase-postgresql swig tinyxml libmng libsvm muParser openjpeg2-tools proj-epsg qt-x11 gdal-python && \
    ln -s /usr/bin/cmake3 /usr/local/bin/cmake

RUN echo -e "packages <- c('arrow', 'e1071', 'caret', 'dplyr', 'gsubfn', 'ranger', 'readr', 'smotefamily', 'caTools', 'tidyverse', 'data.table')\n\
    diff <- setdiff(packages, rownames(installed.packages()))\n\
    if (length(diff) > 0) {\n\
    install.packages(diff, Ncpus = $(nproc), repos = c(CRAN = 'https://cran.rstudio.com'))\n\
    }" | LIBARROW_BINARY=true Rscript -

RUN python3 -m pip install cython~=0.29 pyarrow~=0.3

RUN yum -y remove python3-devel git openssl-devel libcurl-devel arrow-devel arrow-python-devel cmake3 && yum -y autoremove

COPY otb-*.rpm gdal-*.rpm /

RUN yum -y install /otb-*.rpm /gdal-*.rpm && \
    rm /*.rpm /usr/local/bin/gdal_edit.py

COPY sen2agri-processors-*.rpm /
RUN yum -y install /sen2agri-*.rpm && \
    rm /*.rpm && \
    ldconfig
