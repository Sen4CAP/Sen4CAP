## Building the new (ported) processors

### Inside a container

```sh
$ cd docker/processors-new
$ ./build.sh
```

### On the host machine

You will need to download and install Orfeo ToolBox (OTB) from https://www.orfeo-toolbox.org/download/. You will also need some dependencies like a C++ compiler and CMake.

```sh
$ source /opt/OTB-9.1.1-Linux/otbenv.profile # update the path to match your installation
$ cd sen2agri-processors2
$ mkdir build
$ cd build
$ cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local # update as desired
$ cmake --build . --parallel $(nproc)
```

Warning: if you switch from a host to a container build, make sure to delete the `build` directory.

## Building the old processors and rest of the system

### Inside a container

```sh
$ cd docker/build
$ ./build.sh
```
