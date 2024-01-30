/*=========================================================================
  *
  * Program:      Sen2agri-Processors
  * Language:     C++
  * Copyright:    2015-2016, CS Romania, office@c-s.ro
  * See COPYRIGHT file for details.
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.

 =========================================================================*/
 
#include <limits>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/filesystem.hpp>

#include "otbMacro.h"

#include "SEN2CORMetadataReader.hpp"
#include "MetadataUtil.hpp"
#include "tinyxml_utils.hpp"
#include "string_utils.hpp"

// In old versions of L2A products we had the name called L2A_Product_Info and we want to support this too
static std::vector<std::string> PRODUCT_INFO_NODE_NAMES = {"Product_Info", "L2A_Product_Info"};
static std::vector<std::string> PRODUCT_ORGANISATION_NODE_NAMES = {"Product_Organisation", "L2A_Product_Organisation"};
static std::vector<std::string> PRODUCT_IMG_CHARACTERISTICS_NODE_NAMES = {"Product_Image_Characteristics", "L2A_Product_Image_Characteristics"};
static std::vector<std::string> QUANTIF_VALUES_NODE_NAMES = {"QUANTIFICATION_VALUES_LIST", "L1C_L2A_Quantification_Values_List"};
static std::vector<std::string> BOA_QUANTIF_VAL_NODE_NAMES = {"BOA_QUANTIFICATION_VALUE", "L2A_BOA_QUANTIFICATION_VALUE"};
static std::vector<std::string> AOT_QUANTIF_VAL_NODE_NAMES = {"AOT_QUANTIFICATION_VALUE", "L2A_AOT_QUANTIFICATION_VALUE"};
static std::vector<std::string> WVP_QUANTIF_VAL_NODE_NAMES = {"WVP_QUANTIFICATION_VALUE", "L2A_WVP_QUANTIFICATION_VALUE"};
static std::vector<std::string> QUALITY_IND_INFO_NODE_NAMES = {"n1:Quality_Indicators_Info", "n1:L2A_Quality_Indicators_Info"};
static std::vector<std::string> GRANULE_IMAGE_FILE_NODE_NAMES = {"IMAGE_FILE", "IMAGE_FILE_2A"};


namespace itk
{
std::unique_ptr<MACCSFileMetadata> SEN2COR_METADATA_READER_EXPORT SEN2CORMetadataReader::ReadMetadata(const std::string &path)
{
    TiXmlDocument doc(path);
    if (!doc.LoadFile()) {
        return nullptr;
    }

    auto metadata = ReadMetadataXml(doc);
    if (metadata) {
        metadata->ProductPath = path;
    }

    return metadata;
}

const TiXmlElement* FirstChildElement(const TiXmlElement * parent, const std::vector<std::string> &possibleValues)
{
    if (!parent) {
        return nullptr;
    }
    for (auto val: possibleValues) {
        auto el = parent->FirstChildElement(val.c_str());
        if (el) {
            return el;
        }
    }
    return nullptr;
}

const TiXmlElement* NextSiblingElement(const TiXmlElement * parent, const std::vector<std::string> &possibleValues)
{
    if (!parent) {
        return nullptr;
    }
    for (auto val: possibleValues) {
        auto el = parent->NextSiblingElement(val.c_str());
        if (el) {
            return el;
        }
    }
    return nullptr;
}

std::string GetChildText(const TiXmlElement *element, const std::vector<std::string> &possibleValues)
{
    if (!element) {
        return std::string();
    }
    for (auto childName: possibleValues) {
        if (auto el = element->FirstChildElement(childName)) {
            if (const char *text = el->GetText())
                return text;
        }
    }

    return std::string();
}

MACCSFixedHeader ReadSEN2CORGeneralInfo(const TiXmlElement *el)
{
    MACCSFixedHeader result;

    if (!el) {
        return result;
    }
    result.CreationDate = GetChildText(el, "PRODUCT_START_TIME");
    if (auto dataTakeEl = el->FirstChildElement("Datatake")) {
        result.Mission = GetChildText(dataTakeEl, "SPACECRAFT_NAME");
    }

    return result;
}

MACCSImageInformation ReadSEN2CORProductImageCharacteristics(const TiXmlElement *el)
{    
    //el should be Spectral_Information_List node in case of MTD_MSIl2A.xml
    MACCSImageInformation result;

    if (!el) {
        return result;
    }

    // find the No Data value
    bool noDataValueFound = false;
    for (auto specialValuesEl = el->FirstChildElement("Special_Values"); specialValuesEl && !noDataValueFound;
        specialValuesEl = specialValuesEl->NextSiblingElement("Special_Values")) {
        if (const TiXmlElement *specialValueTextEl = specialValuesEl->FirstChildElement("SPECIAL_VALUE_TEXT")) {
            if (GetText(specialValueTextEl).compare("NODATA") == 0) {
                noDataValueFound = true;
                result.NoDataValue = GetChildText(specialValuesEl, "SPECIAL_VALUE_INDEX");
                // ?? I can't find AOT No Data Value within the xml files, shall NoDataValue be used as a general "No Data" value for the others? (aot, boa, vap?)
                result.AOTNoDataValue = result.NoDataValue;
                result.VAPNoDataValue = result.NoDataValue;
            }
        }
    }
    //get the quantification values
    if (auto quantificationValueListEl = FirstChildElement(el, QUANTIF_VALUES_NODE_NAMES)) {
        result.AOTQuantificationValue = GetChildText(quantificationValueListEl, AOT_QUANTIF_VAL_NODE_NAMES);
        result.VAPQuantificationValue = GetChildText(quantificationValueListEl, WVP_QUANTIF_VAL_NODE_NAMES);
    }
    // get the bands
    if (auto spectralInformationListEl = el->FirstChildElement("Spectral_Information_List")) {
        for (auto spectralInfoEl = spectralInformationListEl->FirstChildElement("Spectral_Information"); spectralInfoEl;
             spectralInfoEl = spectralInfoEl->NextSiblingElement("Spectral_Information")) {
            CommonBand band;
            band.Id = GetAttribute(spectralInfoEl, "bandId");
            band.Name = GetAttribute(spectralInfoEl, "physicalBand");
            result.Bands.push_back(band);
        }
    }

    if (auto boaOffsetValueListEl = el->FirstChildElement("BOA_ADD_OFFSET_VALUES_LIST")) {
        for (auto boaOffsetEl = boaOffsetValueListEl->FirstChildElement("BOA_ADD_OFFSET"); boaOffsetEl;
             boaOffsetEl = boaOffsetEl->NextSiblingElement("BOA_ADD_OFFSET")) {
            const std::string &bandId = GetAttribute(boaOffsetEl, "band_id");
            auto it = std::find_if(result.Bands.begin(), result.Bands.end(), [&bandId](const CommonBand& obj) {return obj.Id == bandId;});
            if (it != result.Bands.end()) {
                it->boaAddOffset = GetText(boaOffsetEl);
            }
        }
    }

    return result;
}

MACCSProductInformation ReadSEN2CORProductInformation(const TiXmlElement *el) {
    //el should be Spectral_Information_List node in case of MTD_MSIl2A.xml
    MACCSProductInformation result;

    if (!el) {
        return result;
    }
    if (auto productInfoEl = FirstChildElement(el, PRODUCT_INFO_NODE_NAMES)) {
        result.AcquisitionDateTime = GetChildText(productInfoEl, "PRODUCT_START_TIME");
    }
    if(auto productImageCharaceristicsEl = FirstChildElement(el, PRODUCT_IMG_CHARACTERISTICS_NODE_NAMES)) {
        if (auto spectralInformationListEl = productImageCharaceristicsEl->FirstChildElement("Spectral_Information_List")) {
            for (auto spectralInfoEl = spectralInformationListEl->FirstChildElement("Spectral_Information"); spectralInfoEl;
                 spectralInfoEl = spectralInfoEl->NextSiblingElement("Spectral_Information")) {
                if (auto waveLengthEl = spectralInfoEl->FirstChildElement("Wavelength")) {
                    CommonBandWavelength bandWaveLength;
                    bandWaveLength.BandName = GetAttribute(spectralInfoEl, "physicalBand");
                    bandWaveLength.MinUnit = GetChildAttribute(waveLengthEl, "MIN", "unit");
                    bandWaveLength.MinWaveLength = GetChildText(waveLengthEl, "MIN");
                    bandWaveLength.MaxUnit = GetChildAttribute(waveLengthEl, "MAX", "unit");
                    bandWaveLength.MaxWaveLength = GetChildText(waveLengthEl, "MAX");
                    bandWaveLength.Unit = GetChildAttribute(waveLengthEl, "CENTRAL", "unit");
                    bandWaveLength.WaveLength = GetChildText(waveLengthEl, "CENTRAL");
                    result.BandWavelengths.push_back(bandWaveLength);
                }
                CommonBandResolution resolution;
                resolution.BandName = GetAttribute(spectralInfoEl, "physicalBand");
                resolution.Resolution = GetChildText(spectralInfoEl,"RESOLUTION");
                // no unit information within the xml file, so hardcode it
                resolution.Unit = "m";
                if(resolution.BandName.size() > 0 && resolution.Resolution.size() > 0) {
                    result.BandResolutions.push_back(resolution);
                }
            }
        }
        //get the reflectance quantification value (BOA)
        if (auto quantificationValueListEl = FirstChildElement(productImageCharaceristicsEl, QUANTIF_VALUES_NODE_NAMES)) {
            result.ReflectanceQuantificationValue = GetChildText(quantificationValueListEl, BOA_QUANTIF_VAL_NODE_NAMES);
        }
    }

    // get the angles, these are to be found within ./GRANULE/"tile_id.SAFE"/MTD_TL.xml file
    if (auto sunAnglesGridEl = el->FirstChildElement("Sun_Angles_Grid")) {
        result.SolarAngles = ReadSolarAngles(sunAnglesGridEl);
    }
    if (auto meanSunAngleEl = el->FirstChildElement("Mean_Sun_Angle")) {
        result.MeanSunAngle = ReadAnglePair(meanSunAngleEl,
                                        "ZENITH_ANGLE", "AZIMUTH_ANGLE");
    }
    if (auto meanViewingIncidenceAngleEl = el->FirstChildElement("Mean_Viewing_Incidence_Angle_List")) {
        result.MeanViewingIncidenceAngles = ReadMeanViewingIncidenceAngles(meanViewingIncidenceAngleEl);
    }
    if (el->FirstChildElement("Viewing_Incidence_Angles_Grids")) {
        result.ViewingAngles = ReadViewingAnglesGridList(el);
    }
    return result;
}

MACCSImageInformation ReadSEN2CORTile_Geocoding(const TiXmlElement *el)
{
    //el should be Tile_Geocoding node in case of  ./GRANULE/"tile_id.SAFE"/MTD_TL.xml file
    MACCSImageInformation result;

    if (!el) {
        return result;
    }
    // resolutions, they will be read from ./GRANULE/"tile_id.SAFE"/MTD_TL.xml file

    for (auto sizeEl = el->FirstChildElement("Size"); sizeEl;
         sizeEl = sizeEl->NextSiblingElement("Size")) {
        CommonResolution resolution;
        resolution.Id = GetAttribute(sizeEl, "resolution");
        resolution.Size.Lines = GetChildText(sizeEl, "NROWS");
        resolution.Size.Columns = GetChildText(sizeEl, "NCOLS");
        //find the geoposition for this resolution
        for (auto geopostionEl = el->FirstChildElement("Geoposition"); geopostionEl;
             geopostionEl = geopostionEl->NextSiblingElement("Geoposition")) {
            if (GetAttribute(geopostionEl, "resolution").compare(resolution.Id) == 0) {
                resolution.GeoPosition.UnitLengthX = GetChildText(geopostionEl, "ULX");
                resolution.GeoPosition.UnitLengthY = GetChildText(geopostionEl, "ULY");
                resolution.GeoPosition.DimensionX = GetChildText(geopostionEl, "XDIM");
                resolution.GeoPosition.DimensionY = GetChildText(geopostionEl, "YDIM");
                //can't find product sampling for SEN2COR processor, so hardcode one:
                resolution.ProductSampling.ByLineUnit = "m";
                resolution.ProductSampling.ByLineValue = resolution.Id;
                resolution.ProductSampling.ByColumnUnit = "m";
                resolution.ProductSampling.ByColumnValue = resolution.Id;
                result.Resolutions.push_back(resolution);
                break;
            }
        }
    }
    return result;
}

CommonAnnexInformation ReadSEN2CORAnnexInformation(const TiXmlElement *el)
{
    CommonAnnexInformation result;

    if (!el) {
        return result;
    }
    result.Id = GetAttribute(el, "bandId");
    // no BandNumber, BitNumber or GroupId in SEN2COR
    result.File.BandNumber = -1;
    result.File.BitNumber = -1;
    result.File.GroupId = "";

    result.File.Nature = GetAttribute(el, "type");
    result.File.FileLocation = GetText(el);
    if (result.File.FileLocation.size() > 2) {
        if (result.File.FileLocation.substr(0, 2).compare("./") != 0) {
            result.File.FileLocation.insert(0, "./");
        }
        result.File.LogicalName = GetLogicalFileName(result.File.FileLocation, false);
        /*
        size_t lastSlashPos = result.File.FileLocation.find_last_of("/");

        if (lastSlashPos != std::string::npos && lastSlashPos + 1 < result.File.FileLocation.size()) {
            unsigned short extLen = 0;
            if (result.File.FileLocation.find_last_of(".jp2") != std::string::npos ||
                    result.File.FileLocation.find_last_of(".gml") != std::string::npos) {
                extLen = 4;
            }
            result.File.LogicalName = result.File.FileLocation.substr(lastSlashPos + 1, result.File.FileLocation.size() - lastSlashPos - 1 - extLen);
        } else {
            result.File.LogicalName = result.File.FileLocation;
        }
        */
    }
    return result;
}

std::vector<CommonAnnexInformation> ReadSEN2CORAnnexFileInformation(const TiXmlElement *el)
{
    std::vector<CommonAnnexInformation> result;

    if (!el) {
        return result;
    }

    for (auto annexEl = el->FirstChildElement("MASK_FILENAME"); annexEl;
         annexEl = annexEl->NextSiblingElement("MASK_FILENAME")) {
        result.emplace_back(ReadSEN2CORAnnexInformation(annexEl));
    }
    return result;
}

std::vector<CommonFileInformation> ReadSEN2CORImageFileInformation(const TiXmlElement *el)
{
    std::vector<CommonFileInformation> result;

    if (!el) {
        return result;
    }
    const TiXmlElement *productOrganizationEl = FirstChildElement(el, PRODUCT_ORGANISATION_NODE_NAMES);
    if (!productOrganizationEl) {
        return result;
    }
    const TiXmlElement *firstGranuleListEl = productOrganizationEl->FirstChildElement("Granule_List");
    if (!firstGranuleListEl) {
        return result;
    }

    for (auto granuleListEl = firstGranuleListEl; granuleListEl;
         granuleListEl = granuleListEl->NextSiblingElement("Granule_List")) {

        // the structure of the xml files sugests that there may be more than 1 tile pre product. This isn't handled here,
        // so if there are more than 1 tile per product, only the first one will be handled.
        const TiXmlElement *granuleEl = granuleListEl->FirstChildElement("Granule");
        if (!granuleEl) {
            continue;
        }

        for (auto fileEl = FirstChildElement(granuleEl, GRANULE_IMAGE_FILE_NODE_NAMES); fileEl;
             fileEl = NextSiblingElement(fileEl, GRANULE_IMAGE_FILE_NODE_NAMES)) {
            CommonFileInformation imageFile;
            //no nature for the image files, so set one by default
            imageFile.Nature = "PIC";
            // no BandNumber, BitNumber or GroupId in SEN2COR
            imageFile.BandNumber = -1;
            imageFile.BitNumber = -1;
            imageFile.GroupId = "";

            imageFile.FileLocation = GetText(fileEl);
            if (imageFile.FileLocation.size() >= 2) {
                if (imageFile.FileLocation.substr(0, 2).compare("./") != 0) {
                    imageFile.FileLocation.insert(0, "./");
                }
                imageFile.FileLocation.append(".jp2");
                imageFile.LogicalName = GetLogicalFileName(imageFile.FileLocation, false);
                /*size_t lastSlashPos = imageFile.FileLocation.find_last_of("/");
                if (lastSlashPos != std::string::npos && lastSlashPos + 1 < imageFile.FileLocation.size()) {
                    imageFile.LogicalName = imageFile.FileLocation.substr(lastSlashPos + 1, imageFile.FileLocation.size() - lastSlashPos);
                } else {
                    imageFile.LogicalName = imageFile.FileLocation;
                }
                */
            }
            result.emplace_back(imageFile);
        }
    }

    return result;
}

std::unique_ptr<MACCSFileMetadata> SEN2CORMetadataReader::ReadMetadataXml(const TiXmlDocument &doc)
{
    TiXmlHandle hDoc(const_cast<TiXmlDocument *>(&doc));

    // BUG: TinyXML can't properly read stylesheet declarations, see
    // http://sourceforge.net/p/tinyxml/patches/37/
    // Our files start with one, but we can't read it in.
    const TiXmlElement* rootProductGeneralInfoElement = hDoc.FirstChildElement("n1:Level-2A_User_Product").ToElement();
    const TiXmlElement* generalInfoElement = nullptr;
    auto file = std::unique_ptr<MACCSFileMetadata>(new MACCSFileMetadata);

    if ( rootProductGeneralInfoElement && (generalInfoElement = rootProductGeneralInfoElement->FirstChildElement("n1:General_Info")) ) {

        auto productInfoEl = FirstChildElement(generalInfoElement, PRODUCT_INFO_NODE_NAMES);
        file->Header.SchemaLocation = GetAttribute(rootProductGeneralInfoElement, "xsi:schemaLocation");
        file->Header.FixedHeader = ReadSEN2CORGeneralInfo(productInfoEl);
        file->ImageInformation = ReadSEN2CORProductImageCharacteristics(FirstChildElement(generalInfoElement, PRODUCT_IMG_CHARACTERISTICS_NODE_NAMES));
        file->ProductOrganization.ImageFiles = ReadSEN2CORImageFileInformation(productInfoEl);
        file->ProductInformation = ReadSEN2CORProductInformation(generalInfoElement);
        file->InstanceId.AcquisitionDate = ExtractDateFromDateTime(file->ProductInformation.AcquisitionDateTime);
        return file;
    }
    if (auto rootTileInfoElement = hDoc.FirstChildElement("n1:Level-2A_Tile_ID").ToElement()) {

        if (auto geometricInfoElement = rootTileInfoElement->FirstChildElement("n1:Geometric_Info")) {
            file->ImageInformation = ReadSEN2CORTile_Geocoding(geometricInfoElement->FirstChildElement("Tile_Geocoding"));
            file->ProductInformation = ReadSEN2CORProductInformation(geometricInfoElement->FirstChildElement("Tile_Angles"));
        }
        if (auto qualityIndicatorElement = FirstChildElement(rootTileInfoElement, QUALITY_IND_INFO_NODE_NAMES)) {
            file->ProductOrganization.AnnexFiles = ReadSEN2CORAnnexFileInformation(qualityIndicatorElement->FirstChildElement("Pixel_Level_QI"));
        }

        return file;
    }
    return nullptr;
}

}
