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

set(RadiometricIndices_HEADERS
    ./include/otbBandName.h
    ./include/otbIndicesStackFunctor.h
    ./include/otbRadiometricIndex.h
    ./include/otbSoilIndicesFunctor.h
    ./include/otbVegetationIndicesFunctor.h
    ./include/otbWaterIndicesFunctor.h)

otb_create_application(
  NAME           Sen4XRadiometricIndices
  SOURCES        ${RadiometricIndices_HEADERS} otbSen4XRadiometricIndices.cxx
  LINK_LIBRARIES Sen2AgriProductReaders;${OTB_LIBRARIES})

install(TARGETS otbapp_Sen4XRadiometricIndices DESTINATION usr/lib/otb/applications/)
