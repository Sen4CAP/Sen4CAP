find_package(OTB REQUIRED)
include(${OTB_USE_FILE})

otb_create_application(
    NAME TemporalResampling
    SOURCES TemporalResampling.cpp
    LINK_LIBRARIES ${OTB_LIBRARIES}
)

install(TARGETS otbapp_TemporalResampling DESTINATION usr/lib/otb/applications/)
