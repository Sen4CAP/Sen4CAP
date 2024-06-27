FROM rockylinux:9

LABEL name="sen2agri-build" \
    maintainer="Laurentiu Nicola <laurentiu.nicola@c-s.ro>"

ARG user
ARG group
ARG uid
ARG gid

RUN yum -y install epel-release && \
    yum -y update && \
    yum -y install \
    boost-devel \
    cmake \
    curl-devel \
    expat-devel \
    fftw-devel \
    gcc \
    gcc-c++ \
    git \
    geos-devel \
    gsl-devel \
    libgeotiff-devel \
    libjpeg-turbo-devel \
    libpng-devel \
    libsvm-devel \
    libtiff-devel \
    make \
    pcre-devel \
    proj-devel \
    python-devel \
    qt5-qtbase-devel \
    qt5-qtbase-postgresql \
    rpm-build \
    sqlite-devel \
    sudo \
    tinyxml-devel \
    yum-utils \
    ruby-devel

RUN gem install fpm

COPY entry.sh /
