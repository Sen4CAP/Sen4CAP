find_package(OTB REQUIRED)
include(${OTB_USE_FILE})

otb_create_application(
	NAME           Composite2
	SOURCES        Composite2.cpp
	LINK_LIBRARIES ${OTB_LIBRARIES}
)

install(TARGETS otbapp_Composite2 DESTINATION usr/lib/otb/applications/)
