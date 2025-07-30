include(${OTB_USE_FILE})

otb_create_application(
	NAME           StandardDeviation
	SOURCES        StandardDeviation.cpp
	LINK_LIBRARIES ${OTB_LIBRARIES}
)

install(TARGETS otbapp_StandardDeviation DESTINATION usr/lib/otb/applications/)
