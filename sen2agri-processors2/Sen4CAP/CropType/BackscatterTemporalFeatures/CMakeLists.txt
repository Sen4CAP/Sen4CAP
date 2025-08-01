include(${OTB_USE_FILE})

otb_create_application(
	NAME		BackscatterTemporalFeatures
    SOURCES		BackscatterTemporalFeatures.cpp
				../Filters/RatioFilterVector.hxx
	LINK_LIBRARIES ${OTB_LIBRARIES}
)

include_directories(../Filters)

install(TARGETS otbapp_BackscatterTemporalFeatures DESTINATION usr/lib/otb/applications/)
