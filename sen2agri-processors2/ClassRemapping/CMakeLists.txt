find_package(OTB REQUIRED)
include(${OTB_USE_FILE})

otb_create_application(
	NAME ClassRemapping
	SOURCES ClassRemapping.cpp
	LINK_LIBRARIES ${OTB_LIBRARIES}
)

install(TARGETS otbapp_ClassRemapping DESTINATION usr/lib/otb/applications/)
