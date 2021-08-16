# Filename: Dockerfile

#Ubuntu 20.04 Focal Fossa
FROM ubuntu:20.04

LABEL name="era5-weather" \
      maintainer="Alexandru Becheru <abecheru@c-s.ro>"

RUN apt-get update

#python & pip
RUN apt-get install -y software-properties-common
RUN apt-get install -y python3-pip

#python libraries
RUN pip install cdsapi
RUN pip install numpy
RUN pip install netCDF4

RUN apt-get clean

ENV HOME /var/lib/cdsapi
WORKDIR /usr/share/weather
COPY weather.py ./