FROM rockylinux:9

LABEL name="sen2agri-app-build" \
    maintainer="Laurentiu Nicola <laurentiu.nicola@c-s.ro>"

RUN dnf -y --setopt=install_weak_deps=False update && \
    dnf -y --setopt=install_weak_deps=False install \
           gcc-c++ \
           make \
           qt5-qtbase-devel \
           rpm-build \
           ruby \
           ruby-devel \
           rubygem-json \
           rubygems && \
    dnf clean all && \
    gem install fpm && \
    dnf -y remove ruby-devel
