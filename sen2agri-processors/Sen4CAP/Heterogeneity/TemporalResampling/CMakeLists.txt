include_directories(../include)

otb_create_application(
  NAME           HeterogeneityConstantStepInterpolationMasked
  SOURCES        constant_step_interpolation_masked.cpp
                 engeStepInterpolationFilter.h
                 engeStepInterpolationFilter.txx
                 engeMaskSerieFilter.h
                 engeMaskSerieFilter.txx
                 ../include/lwTime.h
                 ../include/lwString.h

  LINK_LIBRARIES ${OTB_LIBRARIES}
)

install(TARGETS otbapp_HeterogeneityConstantStepInterpolationMasked DESTINATION usr/lib/otb/applications/)



