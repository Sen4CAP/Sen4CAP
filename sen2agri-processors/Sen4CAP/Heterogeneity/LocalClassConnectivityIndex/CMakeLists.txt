include_directories(../include)

otb_create_application(
  NAME           HeterogeneityLocalClassConnectivityIndex
  SOURCES        HeterogeneityLocalClassConnectivityIndex.cxx
                 engeLocalClassConnectivityIndexFilter.h
                 engeLocalClassConnectivityIndexFilter.txx
                 engeSimpleNeighborhoodIterator.h
                 ../include/lwTime.h
                 ../include/lwString.h

  LINK_LIBRARIES ${OTB_LIBRARIES}
)

install(TARGETS otbapp_HeterogeneityLocalClassConnectivityIndex DESTINATION usr/lib/otb/applications/)



