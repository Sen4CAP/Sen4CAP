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

## Setting up the database

```sh
$ DBNAME=sen2agri # for Sen2-Agri
$ DBNAME=sen4cap # for Sen4CAP
$ DBNAME=sen4stat # for Sen4Stat

# for all
$ for f in 00-database/*.sql; psql -f $f $DBNAME; end
$ for f in 01-extensions/*.sql; psql -f $f $DBNAME; end
$ for f in 02-types/*.sql; psql -f $f $DBNAME; end
$ for f in 03-tables/*.sql; psql -f $f $DBNAME; end
$ for f in 04-views/*.sql; psql -f $f $DBNAME; end
$ for f in 05-functions/*.sql; psql -f $f $DBNAME; end
$ for f in 06-indexes/*.sql; psql -f $f $DBNAME; end
$ for f in 07-data/*.sql; psql -f $f $DBNAME; end
$ for f in 08-keys/*.sql; psql -f $f $DBNAME; end
$ for f in 09-privileges/*.sql; psql -f $f $DBNAME; end
$ for f in 10-triggers/*.sql; psql -f $f $DBNAME; end

# for Sen2-Agri
$ for f in 07-data/sen2agri/*.sql; psql -f $f $DBNAME; end

# for Sen4CAP
$ for f in 03-tables/sen4cap/*.sql; psql -f $f $DBNAME; end
$ for f in 07-data/sen4cap/*.sql; psql -f $f $DBNAME; end

# for Sen4Stat
$ for f in 03-tables/sen4stat/*.sql; psql -f $f $DBNAME; end
$ for f in 07-data/sen4stat/*.sql; psql -f $f $DBNAME; end
$ for f in 08-keys/sen4stat/*.sql; psql -f $f $DBNAME; end
```
