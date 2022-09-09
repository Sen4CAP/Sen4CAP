otb_create_application(
	NAME           ComputeIntervalCounts
	SOURCES        ComputeIntervalCounts.cxx
		       otbStreamingHistogramImageFilter.h
		       otbStreamingHistogramImageFilter.hxx
	LINK_LIBRARIES ${OTB_LIBRARIES}
)

if(BUILD_TESTING)
  add_subdirectory(test)
endif()

install(TARGETS otbapp_ComputeIntervalCounts DESTINATION usr/lib/otb/applications/)
