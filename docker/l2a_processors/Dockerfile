FROM ubuntu:20.04

LABEL name="sen2agri-l2a-processors" \
      maintainer="Laurentiu Nicola <laurentiu.nicola@c-s.ro>"

RUN apt-get update

#python libraries
RUN apt-get install -y python3-lxml
RUN apt-get install -y gdal-bin

#docker-ce-cli
RUN apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
RUN apt-key fingerprint 0EBFCD88
RUN add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
RUN apt-get update
RUN apt-get install -y docker-ce docker-ce-cli containerd.io

RUN apt-get clean

WORKDIR /usr/share/l2a_processors
COPY maja.py ./
COPY sen2cor.py ./
COPY l2a_commons.py ./
