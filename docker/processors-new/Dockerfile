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
