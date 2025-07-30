include(${OTB_USE_FILE})

otb_create_application(
	NAME           ClassStatisticsRatio
	SOURCES        ClassStatisticsRatio.cpp
                 ../Filters/otbStreamingStatisticsMapFromLabelImageFilter2.h
                 ../Filters/otbStreamingStatisticsMapFromLabelImageFilter2.hxx
                 ../Filters/RatioFilterVector.hxx
  LINK_LIBRARIES ${OTB_LIBRARIES}
)

include_directories(../Filters)

install(TARGETS otbapp_ClassStatisticsRatio DESTINATION usr/lib/otb/applications/)
