FROM docker.io/sen4x/otb:9.1.1 AS builder
RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

FROM builder AS build
COPY src /src
RUN mkdir /build && \
    cd /build && \
    cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo /src && \
    cmake --build . --parallel $(nproc) && \
    cmake --install . --prefix /install

FROM docker.io/sen4x/otb:9.1.1
COPY --from=build /install/bin/* /app/otb/bin/
COPY --from=build /install/usr/lib/otb/applications/* /app/otb/lib/otb/applications/
