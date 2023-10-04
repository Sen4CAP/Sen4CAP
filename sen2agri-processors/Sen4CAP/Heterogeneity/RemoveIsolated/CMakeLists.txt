include_directories(../include)

otb_create_application(
  NAME           HeterogeneityRemoveIsolated
  SOURCES        HeterogeneityRemoveIsolated.cxx
                 engeRemoveIsolatedFilter.h
                 engeRemoveIsolatedFilter.txx
  LINK_LIBRARIES ${OTB_LIBRARIES}
)

install(TARGETS otbapp_HeterogeneityRemoveIsolated DESTINATION usr/lib/otb/applications/)



