FROM ubuntu:20.04

LABEL maintainer="sen4cap@c-s.ro" \
       description="Prepare a SNAP 8.0 EO processing environment with commonly used tools + extra cluster."

# Environment
ENV SNAP_VERSION        8.0
ENV DEBIAN_FRONTEND noninteractive
ENV LANG            en_US.UTF-8
ENV LC_ALL en_US.UTF-8

WORKDIR /opt/

RUN apt-get update && \
        apt-get upgrade -y && \
        apt-get autoremove -y && \
        apt-get install wget -y && \
        apt-get install unzip -y && \
        apt-get install xvfb -y && \
        apt-get install x11vnc -y

RUN     mkdir ~/.vnc
RUN     x11vnc -storepasswd 1234 ~/.vnc/passwd

# copy packages
COPY esa-snap_sentinel_unix_8_0.sh .
COPY extra-cluster.zip .

# Install SNAP 8.0
#COPY esa-snap_sentinel_unix_8_0.sh .
RUN chmod +x esa-snap_sentinel_unix_8_0.sh && \
    ./esa-snap_sentinel_unix_8_0.sh -q && \
    rm -f esa-snap_sentinel_unix_8_0.sh
#RUN snap --nosplash --nogui --modules --disable org.esa.snap.snap.product.library.v2

# Install additional tools
#RUN apt -y install gdal-bin python-gdal python3-gdal

RUN apt-get -y install libgfortran5
RUN ln -s /opt/snap/bin/gpt /usr/local/bin/gpt
RUN mkdir /home/.snap && \
    chmod 777 /home/.snap

# Snap file modifications:
# - Modify GPT arguments
RUN echo 'snap.userdir=/home/.snap' >> /opt/snap/etc/snap.properties
RUN sed -i '/^-Xmx/s/^/#/' /opt/snap/bin/gpt.vmoptions && \
        sed -i '$a-server' /opt/snap/bin/gpt.vmoptions && \
        sed -i '$a-Xms4G' /opt/snap/bin/gpt.vmoptions && \
        sed -i '$a-Xmx16G' /opt/snap/bin/gpt.vmoptions
# install extra cluster
RUN unzip extra-cluster.zip -d /opt/snap
RUN sed -i '$a-Dsnap.extraClusters=/opt/snap/extra' /opt/snap/bin/gpt.vmoptions

ENV DISPLAY :99
COPY update_snap.sh .
RUN chmod +x ./update_snap.sh
RUN ./update_snap.sh


# RUN  /opt/snap/bin/snap --nosplash --nogui --modules --refresh

#
# RUN mkdir -p ".snap/auxdata/dem/ASTER 1sec GDEM v3"
# RUN mkdir -p ".snap/auxdata/dem/SRTM 1Sec HGT"
