FROM docker.io/orfeotoolbox/otb:9.1.1_ubuntu24 AS builder
RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

FROM builder as build
COPY src /src
RUN mkdir /build && \
    cd /build && \
    cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo /src && \
    cmake --build . --parallel $(nproc) && \
    cmake --install . --prefix /install

FROM docker.io/orfeotoolbox/otb:9.1.1_ubuntu24
COPY --from=build /install/bin/* /app/otb/bin/
COPY --from=build /install/usr/lib/otb/applications/* /app/otb/lib/otb/applications/

# FIXME: drop this when OTB upgrades to OpenJPEG 2.5.3 or later
RUN curl -sLO https://github.com/uclouvain/openjpeg/releases/download/v2.5.3/openjpeg-v2.5.3-linux-x86_64.tar.gz && \
    tar xf openjpeg-v2.5.3-linux-x86_64.tar.gz && \
    mv openjpeg-v2.5.3-linux-x86_64/lib/libopenjp2.a openjpeg-v2.5.3-linux-x86_64/lib/libopenjp2.so.2.5.3 openjpeg-v2.5.3-linux-x86_64/lib/libopenjp2.so.7 /app/otb/lib/ && \
    rm -rf openjpeg-v2.5.3-linux-x86_64.tar.gz openjpeg-v2.5.3-linux-x86_64 /app/otb/lib/libopenjp2.so.2.3.1
