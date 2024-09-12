FROM ubuntu:24.04

LABEL name="fmask_extractor" \
      maintainer="Laurentiu Nicola <laurentiu.nicola@c-s.ro>"

RUN apt-get update && \
    apt-get install -y \
    ca-certificates \
    curl \
    gdal-bin \
    gnupg-agent \
    software-properties-common && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - && \
    apt-key fingerprint 0EBFCD88 && \
    add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" && \
    apt-get update && \
    apt-get install -y docker-ce docker-ce-cli containerd.io && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/share/fmask
COPY fmask_extractor.py ./
COPY l2a_commons.py ./
