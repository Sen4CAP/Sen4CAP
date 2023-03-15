FROM centos:centos7

LABEL name="sen2agri-services" \
      maintainer="Laurentiu Nicola <laurentiu.nicola@c-s.ro>"

RUN yum -y update && \
    yum -y install epel-release && \
    yum -y update

COPY *.rpm /

RUN yum install -y /otb-*.rpm /sen2agri-*.rpm /slurm-*.rpm java-1.8.0-openjdk-headless && \
    rm /*.rpm

COPY slurmdbd.conf /etc/slurm
COPY slurm.conf /etc/slurm

RUN sed -i 's/HostName=localhost/HostName=db/;s/DatabaseName=sen2agri/DatabaseName=sen4cap/' /etc/sen2agri/sen2agri.conf

COPY start.sh /

ARG slurm_uid
ARG slurm_gid
ARG sen2agri_service_uid
ARG sen2agri_service_gid

RUN groupadd -g $slurm_gid slurm && \
    useradd -m -u $slurm_uid -g $slurm_gid slurm && \
    groupadd -g $sen2agri_service_gid sen2agri-service && \
    useradd -m -u $sen2agri_service_uid -g $sen2agri_service_gid sen2agri-service && \
    mkdir /var/log/slurm /var/spool/slurm && \
    chown -R slurm /var/log/slurm /var/spool/slurm && \
    dd if=/dev/urandom of=/etc/munge/munge.key bs=1024 count=1 && \
    chown munge: /etc/munge/munge.key && \
    chmod 400 /etc/munge/munge.key
