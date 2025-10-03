include(${OTB_USE_FILE})

otb_create_application(
	NAME           Composite
	SOURCES        Composite.cpp
	LINK_LIBRARIES ${OTB_LIBRARIES}
)

install(TARGETS otbapp_Composite DESTINATION usr/lib/otb/applications/)
