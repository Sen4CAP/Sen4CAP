include(${OTB_USE_FILE})

otb_create_application(
	NAME           MeanComposite
	SOURCES        MeanComposite.cpp
	LINK_LIBRARIES ${OTB_LIBRARIES}
)

install(TARGETS otbapp_MeanComposite DESTINATION usr/lib/otb/applications/)
