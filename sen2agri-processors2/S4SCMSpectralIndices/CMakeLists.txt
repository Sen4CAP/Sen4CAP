find_package(OTB REQUIRED)
include(${OTB_USE_FILE})

otb_create_application(
	NAME           S4SCMSpectralIndices
	SOURCES        S4SCMSpectralIndices.cpp
	LINK_LIBRARIES ${OTB_LIBRARIES}
)

install(TARGETS otbapp_S4SCMSpectralIndices DESTINATION usr/lib/otb/applications/)
