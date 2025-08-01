include(${OTB_USE_FILE})

otb_create_application(
	NAME           CoherenceMonthlyFeatures
	SOURCES        CoherenceMonthlyFeatures.cpp
	LINK_LIBRARIES ${OTB_LIBRARIES}
)

install(TARGETS otbapp_CoherenceMonthlyFeatures DESTINATION usr/lib/otb/applications/)
