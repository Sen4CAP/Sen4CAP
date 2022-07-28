# =========================================================================
# Program:   otb-bv
# Language:  C++
#
# Copyright (c) CESBIO. All rights reserved.
#
# See otb-bv-copyright.txt for details.
#
# This software is distributed WITHOUT ANY WARRANTY; without even
# the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
# PURPOSE.  See the above copyright notices for more information.
#
# =========================================================================

IF (WIN32)
    link_directories(C:/OTB-release-xdk-win32/lib)
ENDIF(WIN32)

set(GenericComposite_HEADERS
    ./include/otbCompositeFunctor.h
    ./include/otbMeanCompositeFunctor.h
    ./include/otbWeightedAverageCompositeFunctor.h
    ./include/otbMedianCompositeFunctor.h
    ./include/otbMinCompositeFunctor.h
    ./include/otbMaxCompositeFunctor.h
    ./include/otbLastValueCompositeFunctor.h
    ./include/otbCountValidPixelsFunctor.h
    ./include/otbImageListCompositeFilter.h)

otb_create_application(
  NAME           Sen4XGenericComposite
  SOURCES        ${GenericComposite_HEADERS} otbSen4XGenericComposite.cxx
  LINK_LIBRARIES Sen2AgriProductReaders;${OTB_LIBRARIES})

# otb_create_application(
#  NAME           Sen4XGenericComposite2
#  SOURCES        otbSen4XGenericComposite2.cxx
#  LINK_LIBRARIES ${OTB_LIBRARIES})

install(TARGETS otbapp_Sen4XGenericComposite DESTINATION usr/lib/otb/applications/)
# install(TARGETS otbapp_Sen4XGenericComposite2 DESTINATION usr/lib/otb/applications/)
