include(${OTB_USE_FILE})

otb_create_application(
	NAME           ClassStatistics
	SOURCES        ClassStatistics.cpp
                 ../Filters/otbStreamingStatisticsMapFromLabelImageFilter2.h
                 ../Filters/otbStreamingStatisticsMapFromLabelImageFilter2.hxx
  LINK_LIBRARIES ${OTB_LIBRARIES}
)

include_directories(../Filters)

install(TARGETS otbapp_ClassStatistics DESTINATION usr/lib/otb/applications/)
