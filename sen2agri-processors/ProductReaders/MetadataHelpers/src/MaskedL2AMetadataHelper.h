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

#ifndef MASKEDL2AMETADATAHELPER_H
#define MASKEDL2AMETADATAHELPER_H

#include "MetadataHelperFactory.h"
#include "MetadataHelper.h"

template <typename PixelType, typename MasksPixelType>
class MaskedL2AMetadataHelper : public MetadataHelper<PixelType, MasksPixelType>
{
public:
    typedef typename MetadataHelper<PixelType, MasksPixelType>::VectorImageType VectorImageType;
    typedef typename MetadataHelper<PixelType, MasksPixelType>::ImageListType   ImageListType;
    typedef typename MetadataHelper<PixelType, MasksPixelType>::SingleBandMasksImageType SingleBandMasksImageType;

public:
    MaskedL2AMetadataHelper() {}

    const char * GetNameOfClass() { return "MaskedL2AMetadataHelper"; }

    virtual bool DoLoadMetadata(const std::string &file)
    {
        std::ifstream fStream(file);
        if (!fStream.is_open()) {
            std::cout << "Error opennig file " << file << std::endl;
            return false;
        }
        std::string line;
        if (std::getline(fStream, line)) {
            if (line != "# MASKED L2A METADATA INFO") {
                return false;
            }
        }
        boost::filesystem::path pRefFile(file);
        while (std::getline(fStream, line)) {
            line.erase(std::remove_if(line.begin(), line.end(), isspace),line.end());
            if (line.size() == 0 || line[0] == '#') {
                continue;
            }
            auto delimiterPos = line.find("=");
            auto key = line.substr(0, delimiterPos);
            auto value = line.substr(delimiterPos + 1);
            if ( !boost::filesystem::exists(value) )
            {
                std::cout << "Cannot find L2A metadata file " << value << " described in " << file << std::endl;
                return false;
            }
            if (key == "L2A_META") {
                m_l2aMetaPath = value;
            } else if (key == "L2A_NAME") {
                m_l2aPrdName = value;
            } else {
                auto idx = key.find("MSK_");
                if (idx == 0) {
                    auto res = line.substr(strlen("MSK_"));
                    if (!boost::filesystem::exists(pRefFile.parent_path() / value)) {
                        std::cout << "Mask file " << value << " cannot be found in directory " << pRefFile.string() << std::endl;
                        return false;
                    }
                    m_MaskFiles[std::atoi(res.c_str())] = value;
                }
            }
        }
        if (m_l2aMetaPath.size() == 0) {
            return false;
        }
        // we should have at least 2 masks for 10 and 20 m but maximum 3 bands (with an additional one for native resolution)
        if (m_MaskFiles.size() != 2 && m_MaskFiles.size() != 3) {
            return false;
        }
        if (m_MaskFiles.find(10) == m_MaskFiles.end() || m_MaskFiles.find(20) == m_MaskFiles.end()) {
            return false;
        }
        auto factory = MetadataHelperFactory::New();
        m_pHelper = factory->GetMetadataHelper<PixelType, MasksPixelType>(m_l2aMetaPath);

        return true;
    }

    virtual std::string GetMissionName() {
        return m_pHelper->GetMissionName();
    }
    virtual std::string GetMissionShortName()
    {
        return m_pHelper->GetMissionShortName();
    }
    virtual std::string GetInstrumentName()
    {
        return m_pHelper->GetInstrumentName();
    }
    virtual typename VectorImageType::Pointer GetImage(const std::vector<std::string> &bandNames,
                                                       int outRes = -1)
    {
        return m_pHelper->GetImage(bandNames, outRes);
    }
    virtual typename VectorImageType::Pointer GetImage(const std::vector<std::string> &bandNames,
                                                       std::vector<int> *pRetRelBandIdxs,
                                                       int outRes = -1)
    {
        return m_pHelper->GetImage(bandNames, pRetRelBandIdxs, outRes);
    }
    virtual typename ImageListType::Pointer GetImageList(const std::vector<std::string> &bandNames,
                                                         typename ImageListType::Pointer outImgList,
                                                         int outRes = -1)
    {
        return m_pHelper->GetImageList(bandNames, outImgList, outRes);
    }
    virtual std::vector<std::string> GetBandNamesForResolution(int res)
    {
        return m_pHelper->GetBandNamesForResolution(res);
    }
    virtual std::vector<std::string> GetAllBandNames()
    {
        return m_pHelper->GetAllBandNames();
    }
    virtual std::vector<std::string> GetPhysicalBandNames()
    {
        return m_pHelper->GetPhysicalBandNames();
    }
    virtual int GetResolutionForBand(const std::string &bandName)
    {
        return m_pHelper->GetResolutionForBand(bandName);
    }
    virtual std::string GetRedBandName()
    {
        return m_pHelper->GetRedBandName();
    }
    virtual std::string GetBlueBandName()
    {
        return m_pHelper->GetBlueBandName();
    }
    virtual std::string GetGreenBandName()
    {
        return m_pHelper->GetGreenBandName();
    }
    virtual std::string GetNirBandName()
    {
        return m_pHelper->GetNirBandName();
    }
    virtual std::string GetNarrowNirBandName()
    {
        return m_pHelper->GetNarrowNirBandName();
    }
    virtual std::string GetSwirBandName()
    {
        return m_pHelper->GetSwirBandName();
    }
    virtual std::string GetSwir2BandName()
    {
        return m_pHelper->GetSwir2BandName();
    }
    virtual std::vector<std::string> GetRedEdgeBandNames()
    {
        return m_pHelper->GetRedEdgeBandNames();
    }
    virtual std::string GetNoDataValue()
    {
        return m_pHelper->GetNoDataValue();
    }
    // MASKS API
    // if binarizeResult is true, it returns a mask image having 0 as valid pixel and 1 for invalid pixels
    // if binarizeResult is false, the pixels are:
    // IMG_FLG_LAND=0, IMG_FLG_WATER=1, IMG_FLG_CLOUD_SHADOW=2, IMG_FLG_SNOW=3, IMG_FLG_CLOUD=4, IMG_FLG_SATURATION=5, IMG_FLG_NO_DATA=255
    virtual typename SingleBandMasksImageType::Pointer
    GetL2AMasksImage(MasksFlagType, bool binarizeResult, int resolution = -1)
    {
        typename otb::Wrapper::AppExternalMaskProvider<MasksPixelType>::Pointer appExtMskProvider = otb::Wrapper::AppExternalMaskProvider<MasksPixelType>::New();
        this->m_appExtMskProviders.push_back(appExtMskProvider);
        std::string maskPath;
        if (resolution == -1) {
            if (m_MaskFiles.size() == 3) {
                for ( const auto &pair : m_MaskFiles ) {
                    if (pair.first != 10 && pair.first != 20) {
                        maskPath = pair.second;
                    }
                }
            } else {
                maskPath = m_MaskFiles[10];
            }
        }
        return appExtMskProvider->GetExternalMask(resolution, binarizeResult, maskPath);
    }
    // returns the acquisition date in the format YYYYMMDD
    virtual std::string GetAcquisitionDate()
    {
        return m_pHelper->GetAcquisitionDate();
    }
    // Returns the acquisition datetime formatted as yyyyMMddThhmmss
    virtual std::string GetAcquisitionDateTime()
    {
        return m_pHelper->GetAcquisitionDateTime();
    }
    virtual int GetAcquisitionDateAsDoy()
    {
        return m_pHelper->GetAcquisitionDateAsDoy();
    }
    virtual std::string GetAotImageFileName(int res)
    {
        return m_pHelper->GetAotImageFileName(res);
    }
    virtual double GetReflectanceQuantificationValue()
    {
        return m_pHelper->GetReflectanceQuantificationValue();
    }
    virtual float GetAotQuantificationValue(int res)
    {
        return m_pHelper->GetAotQuantificationValue(res);
    }
    virtual float GetAotNoDataValue(int res)
    {
        return m_pHelper->GetAotNoDataValue(res);
    }
    virtual int GetAotBandIndex(int res)
    {
        return m_pHelper->GetAotBandIndex(res);
    }
    virtual bool HasGlobalMeanAngles()
    {
        return m_pHelper->HasGlobalMeanAngles();
    }
    virtual bool HasBandMeanAngles()
    {
        return m_pHelper->HasBandMeanAngles();
    }
    virtual MeanAngles_Type GetSolarMeanAngles()
    {
        return m_pHelper->GetSolarMeanAngles();
    }
    virtual MeanAngles_Type GetSensorMeanAngles()
    {
        return m_pHelper->GetSensorMeanAngles();
    }
    virtual double GetRelativeAzimuthAngle()
    {
        return m_pHelper->GetRelativeAzimuthAngle();
    }
    virtual MeanAngles_Type GetSensorMeanAngles(int nBand)
    {
        return m_pHelper->GetSensorMeanAngles(nBand);
    }
    virtual bool HasDetailedAngles()
    {
        return m_pHelper->HasDetailedAngles();
    }
    virtual int GetDetailedAnglesGridSize()
    {
        return m_pHelper->GetDetailedAnglesGridSize();
    }
    virtual MetadataHelperAngles GetDetailedSolarAngles()
    {
        return m_pHelper->GetDetailedSolarAngles();
    }
    virtual std::vector<MetadataHelperViewingAnglesGrid> GetDetailedViewingAngles(int res)
    {
        return m_pHelper->GetDetailedViewingAngles(res);
    }
    virtual std::vector<MetadataHelperViewingAnglesGrid> GetAllDetectorsDetailedViewingAngles()
    {
        return m_pHelper->GetAllDetectorsDetailedViewingAngles();
    }
    virtual std::vector<int> GetProductResolutions()
    {
        return m_pHelper->GetProductResolutions();
    }
    // the total number of bands that are physically present in the product
    // (for ex. for S2 the 60m resolution bands are not present in the product but
    //  are present in the metadata)
    virtual int GetBandsPresentInPrdTotalNo()
    {
        return m_pHelper->GetBandsPresentInPrdTotalNo();
    }
    // Extract the RGB band names
    virtual bool
    GetTrueColourBandNames(std::string &redIdx, std::string &greenIdx, std::string &blueIdx)
    {
        return m_pHelper->GetTrueColourBandNames(redIdx, greenIdx, blueIdx);
    }

protected:
    std::string m_l2aMetaPath;
    std::string m_l2aPrdName;
    std::unique_ptr<MetadataHelper<PixelType, MasksPixelType>> m_pHelper;
    std::map<int, std::string> m_MaskFiles;
};

#endif // MASKEDL2AMETADATAHELPER_H
