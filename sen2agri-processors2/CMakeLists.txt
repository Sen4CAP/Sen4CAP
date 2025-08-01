cmake_minimum_required(VERSION 3.11)
project(sen2agri-processors-re)

find_package(OTB CONFIG REQUIRED)
find_package(Boost CONFIG REQUIRED COMPONENTS filesystem serialization unit_test_framework)
find_package(CURL CONFIG REQUIRED)

add_subdirectory(Sen4CAP)
add_subdirectory(Sen4Stat)
