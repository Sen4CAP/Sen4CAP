
otb_create_application(
  NAME           HeterogeneityRemoveIsolated
  SOURCES        HeterogeneityRemoveIsolated.cxx
                 engeRemoveIsolatedFilter.h
                 engeRemoveIsolatedFilter.txx
  LINK_LIBRARIES ${OTB_LIBRARIES}
)

otb_create_application(
  NAME           HeterogeneityConstantStepInterpolationMasked
  SOURCES        constant_step_interpolation_masked.cpp
                 engeStepInterpolationFilter.h
                 engeStepInterpolationFilter.txx
                 engeMaskSerieFilter.h
                 engeMaskSerieFilter.txx
                 lwTime.h
                 lwString.h

  LINK_LIBRARIES ${OTB_LIBRARIES}
)

otb_create_application(
  NAME           HeterogeneityLocalClassConnectivityIndex
  SOURCES        HeterogeneityLocalClassConnectivityIndex.cxx
                 engeLocalClassConnectivityIndexFilter.h
                 engeLocalClassConnectivityIndexFilter.txx
                 lwTime.h
                 lwString.h

  LINK_LIBRARIES ${OTB_LIBRARIES}
)

install(TARGETS otbapp_HeterogeneityRemoveIsolated DESTINATION usr/lib/otb/applications/)
install(TARGETS otbapp_HeterogeneityConstantStepInterpolationMasked DESTINATION usr/lib/otb/applications/)
install(TARGETS otbapp_HeterogeneityLocalClassConnectivityIndex DESTINATION usr/lib/otb/applications/)



