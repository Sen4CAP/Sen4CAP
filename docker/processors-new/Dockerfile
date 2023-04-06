FROM docker.io/orfeotoolbox/otb:8.1.1 AS builder
RUN apt update && \
    apt install -y cmake g++ libegl1-mesa-dev libglu1-mesa-dev libglx-dev && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

FROM builder as build
COPY src /src
RUN mkdir /build && \
    mkdir /install && \
    cd /build && \
    cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX=/install /src && \
    cmake --build . --parallel $(nproc) && \
    cmake --install .

FROM build
COPY --from=build /install/bin/* /opt/otb/bin/
COPY --from=build /install/usr/lib/otb/applications/* /opt/otb/lib/otb/applications/
