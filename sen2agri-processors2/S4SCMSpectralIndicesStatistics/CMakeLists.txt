find_package(OTB REQUIRED)
include(${OTB_USE_FILE})

otb_create_application(
	NAME           S4SCMSpectralIndicesStatistics
	SOURCES        S4SCMSpectralIndicesStatistics.cpp
	LINK_LIBRARIES ${OTB_LIBRARIES}
)

install(TARGETS otbapp_S4SCMSpectralIndicesStatistics DESTINATION usr/lib/otb/applications/)
