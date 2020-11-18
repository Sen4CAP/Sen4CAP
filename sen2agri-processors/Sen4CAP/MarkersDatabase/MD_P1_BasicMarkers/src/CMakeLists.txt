find_package(Boost REQUIRED COMPONENTS filesystem)

include_directories(../../../../Common/OTBExtensions)
include_directories(../../../../AgriculturalPractices/Common/include)
find_package(Boost REQUIRED COMPONENTS regex)

otb_create_application(
  NAME           Markers1Extractor
  SOURCES        otbMarkers1Extractor.cxx
                 otbPersistentSamplingFilterBase.h
                 otbPersistentSamplingFilterBase.txx
                 otbOGRDataToClassStatisticsFilter.h
                 otbOGRDataToClassStatisticsFilter.txx
                 otbMaskedIteratorDecorator.h
                 otbMaskedIteratorDecorator.txx
                 otbStopwatch.h
                 otbStopwatch.cxx
                 otbMarkers1CsvWriter.h
                 otbMarkers1CsvWriter.txx

  LINK_LIBRARIES ${OTBExtensions} ${OTB_LIBRARIES} ${OTBCommon_LIBRARIES} ${OTBITK_LIBRARIES} MACCSMetadata Sen2AgriPrdMetaWriters ${Boost_LIBRARIES} Sen2AgriCommonUtils
)

otb_create_application(
  NAME           Markers1CsvMerge
  SOURCES        otbMarkers1CsvMerge.cxx

  LINK_LIBRARIES ${OTBExtensions} ${OTB_LIBRARIES} ${OTBCommon_LIBRARIES} ${OTBITK_LIBRARIES}  ${Boost_LIBRARIES} Sen2AgriCommonUtils
)

otb_create_application(
  NAME           Markers2Extractor
  SOURCES        otbMarkers2Extractor.cxx

  LINK_LIBRARIES ${OTBExtensions} ${OTB_LIBRARIES} ${OTBCommon_LIBRARIES} ${OTBITK_LIBRARIES}  ${Boost_LIBRARIES} Sen2AgriCommonUtils
)


install(TARGETS otbapp_Markers1Extractor DESTINATION usr/lib/otb/applications/)
install(TARGETS otbapp_Markers1CsvMerge DESTINATION usr/lib/otb/applications/)
install(TARGETS otbapp_Markers2Extractor DESTINATION usr/lib/otb/applications/)

