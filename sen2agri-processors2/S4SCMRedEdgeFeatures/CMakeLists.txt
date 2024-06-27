include(${OTB_USE_FILE})

otb_create_application(
	NAME           S4SCMRedEdgeFeatures
	SOURCES        S4SCMRedEdgeFeatures.cpp
	LINK_LIBRARIES ${OTB_LIBRARIES}
)

install(TARGETS otbapp_S4SCMRedEdgeFeatures DESTINATION usr/lib/otb/applications/)
