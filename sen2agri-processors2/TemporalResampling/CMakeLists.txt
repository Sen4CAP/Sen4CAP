find_package(OTB REQUIRED)
include(${OTB_USE_FILE})

add_executable(constant_step_interpolation_masked constant_step_interpolation_masked.cpp)
target_link_libraries(constant_step_interpolation_masked ${OTB_LIBRARIES})

otb_create_application(
    NAME TemporalResampling
    SOURCES TemporalResampling.cpp
    LINK_LIBRARIES ${OTB_LIBRARIES}
)

install(TARGETS otbapp_TemporalResampling DESTINATION usr/lib/otb/applications/)
