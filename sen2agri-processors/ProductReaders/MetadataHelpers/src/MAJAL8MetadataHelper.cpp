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

#include "MAJAL8MetadataHelper.h"
#include "ViewingAngles.hpp"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>
#include "MAJAMetadataReader.hpp"

typedef itk::MAJAMetadataReader                                    MAJAMetadataReaderType;

template <typename PixelType, typename MasksPixelType>
MAJAL8MetadataHelper<PixelType, MasksPixelType>::MAJAL8MetadataHelper()
{
    this->m_ReflQuantifVal = 1;
}

template <typename PixelType, typename MasksPixelType>
typename MetadataHelper<PixelType, MasksPixelType>::VectorImageType::Pointer MAJAL8MetadataHelper<PixelType, MasksPixelType>::GetImage(const std::vector<std::string> &bandNames, int outRes)
{
    return GetImage(bandNames, NULL, outRes);
}

template <typename PixelType, typename MasksPixelType>
typename MetadataHelper<PixelType, MasksPixelType>::VectorImageType::Pointer MAJAL8MetadataHelper<PixelType, MasksPixelType>::GetImage(const std::vector<std::string> &bandNames,
                                                                                 std::vector<int> *pRetRelBandIdxs, int outRes)
{
    std::vector<std::string> validBandNames;
    std::vector<int> retBandIdxs(bandNames.size());
    this->GetValidBandNames(bandNames, validBandNames, retBandIdxs, outRes);

    if (pRetRelBandIdxs != NULL) {
        (*pRetRelBandIdxs) = retBandIdxs;
    }

    // if we have only one band requested, then return only that band
    if (validBandNames.size() == 1) {
        typename MACCSMetadataHelperBase<PixelType, MasksPixelType>::ImageReaderType::Pointer reader = this->CreateReader(GetImageFileName(validBandNames[0]));
        typename MACCSMetadataHelperBase<PixelType, MasksPixelType>::VectorImageType::Pointer img = reader->GetOutput();
        img->UpdateOutputInformation();
        int curRes = img->GetSpacing()[0];
        // if no resampling, just return the raster
        if (outRes == curRes) {
            return reader->GetOutput();
        }
        float fMultiplicationFactor = ((float)curRes)/outRes;
        return this->m_ImageResampler.getResampler(reader->GetOutput(), fMultiplicationFactor)->GetOutput();
    }

    // if we have several bands
    typename MetadataHelper<PixelType, MasksPixelType>::ImageListType::Pointer imageList = this->CreateImageList();
    for (const std::string &bandName: validBandNames) {
        typename MetadataHelper<PixelType, MasksPixelType>::ImageReaderType::Pointer reader = this->CreateReader(GetImageFileName(bandName));
        typename MetadataHelper<PixelType, MasksPixelType>::VectorImageType::Pointer img = reader->GetOutput();
        img->UpdateOutputInformation();
        int curRes = img->GetSpacing()[0];
        // for MAJA we have only one band per reflectance raster
        this->m_bandsExtractor.ExtractImageBands(img, imageList, {0}, Interpolator_NNeighbor, curRes, outRes);
    }

    imageList->UpdateOutputInformation();

    typename MACCSMetadataHelperBase<PixelType, MasksPixelType>::ListConcatenerFilterType::Pointer concat = this->CreateConcatenner();
    concat->SetInput(imageList);
    return concat->GetOutput();
}

template <typename PixelType, typename MasksPixelType>
typename MetadataHelper<PixelType, MasksPixelType>::ImageListType::Pointer MAJAL8MetadataHelper<PixelType, MasksPixelType>::GetImageList(const std::vector<std::string> &bandNames,
                                                                           typename MetadataHelper<PixelType, MasksPixelType>::ImageListType::Pointer outImgList,
                                                                           int outRes)
{
    std::vector<std::string> validBandNames;
    std::vector<int> retBandIdxs(bandNames.size());
    this->GetValidBandNames(bandNames, validBandNames, retBandIdxs, outRes);

    typename MetadataHelper<PixelType, MasksPixelType>::ImageListType::Pointer imageList = outImgList;
    if (!imageList) {
        imageList = this->CreateImageList();
    }

    // if we have several bands
    for (const std::string &bandName: validBandNames) {
        typename MetadataHelper<PixelType, MasksPixelType>::ImageReaderType::Pointer reader = this->CreateReader(GetImageFileName(bandName));
        typename MetadataHelper<PixelType, MasksPixelType>::VectorImageType::Pointer img = reader->GetOutput();
        img->UpdateOutputInformation();
        int curRes = img->GetSpacing()[0];
        // for MAJA we have only one band per reflectance raster
        this->m_bandsExtractor.ExtractImageBands(img, imageList, {0}, Interpolator_NNeighbor, curRes, outRes);
    }

    imageList->UpdateOutputInformation();
    return imageList;
}

template <typename PixelType, typename MasksPixelType>
bool MAJAL8MetadataHelper<PixelType, MasksPixelType>::LoadAndCheckMetadata(const std::string &file)
{
    MAJAMetadataReaderType::Pointer majaMetadataReader = MAJAMetadataReaderType::New();
    // just check if the file is MACCS metadata file. In this case
    // the helper will return the hardcoded values from the constructor as these are not
    // present in the metadata
    if (this->m_metadata = majaMetadataReader->ReadMetadata(file)) {
        if (this->m_metadata->Header.FixedHeader.Mission.find(LANDSAT_MISSION_STR) != std::string::npos &&
                this->m_metadata->Header.FixedHeader.SourceSystem == "MUSCATE") {

            this->m_fAotQuantificationValue = std::stod(this->m_metadata->ImageInformation.AOTQuantificationValue);
            this->m_fAotNoDataVal = std::stod(this->m_metadata->ImageInformation.AOTNoDataValue);
            this->m_aotFileName = this->GetMACCSImageFileName(this->m_metadata->ProductOrganization.ImageFiles, "_ATB_XS");

            return true;
        }
    }
    return false;
}

template <typename PixelType, typename MasksPixelType>
float MAJAL8MetadataHelper<PixelType, MasksPixelType>::GetAotQuantificationValue(int)
{
    return this->m_fAotQuantificationValue;
}

template <typename PixelType, typename MasksPixelType>
float MAJAL8MetadataHelper<PixelType, MasksPixelType>::GetAotNoDataValue(int)
{
    return this->m_fAotNoDataVal;
}

template <typename PixelType, typename MasksPixelType>
int MAJAL8MetadataHelper<PixelType, MasksPixelType>::GetAotBandIndex(int)
{
    // 1 based index, according to specs
    return 2;
}

template <typename PixelType, typename MasksPixelType>
std::string MAJAL8MetadataHelper<PixelType, MasksPixelType>::GetImageFileName(const std::string &bandName) {
    return this->GetMACCSImageFileName(this->m_metadata->ProductOrganization.ImageFiles, "_FRE_" + bandName);
}

template <typename PixelType, typename MasksPixelType>
bool MAJAL8MetadataHelper<PixelType, MasksPixelType>::HasBandName(const std::vector<std::string> &bandNames, const std::string &bandName) {
    if (std::find(bandNames.begin(), bandNames.end(), bandName) != bandNames.end()) {
        return true;
    }
    return false;
}

template <typename PixelType, typename MasksPixelType>
bool MAJAL8MetadataHelper<PixelType, MasksPixelType>::GetValidBandNames(const std::vector<std::string> &bandNames, std::vector<std::string> &validBandNames,
                       std::vector<int> &relBandIndexes, int &outRes)
{
    relBandIndexes.resize(bandNames.size());
    std::fill(relBandIndexes.begin(), relBandIndexes.end(), -1);
    int valdBandIdx = 0;
    int i = 0;
    const std::vector<std::string> &allBands = this->GetAllBandNames();
    for(const std::string &bandName: bandNames) {
        if (this->HasBandName(allBands, bandName)) {
            relBandIndexes[i] = valdBandIdx++;
            validBandNames.push_back(bandName);
        }
        i++;
    }
    if (validBandNames.size() == 0) {
        itkExceptionMacro("No valid band was provided for extracting the image " << bandNames[0] << " ... ");
        return false;
    }

    // set the out resolution to a valid value
    if (outRes == -1) {
        outRes = 30;
    }
    return true;
}

template <typename PixelType, typename MasksPixelType>
bool MAJAL8MetadataHelper<PixelType, MasksPixelType>::BandAvailableForResolution(const std::string &bandName, int nRes) {
    // Sentinel 2
    for(const CommonBandResolution& bandResolution : this->m_metadata->ProductInformation.BandResolutions) {
        if (bandResolution.BandName == bandName) {
            int nBandRes = std::atoi(bandResolution.Resolution.c_str());
            if(nBandRes == nRes) {
                return true;
            }
        }
    }
    return false;
}

template <typename PixelType, typename MasksPixelType>
std::string MAJAL8MetadataHelper<PixelType, MasksPixelType>::getCloudFileName(int )
{
    return this->GetMACCSImageFileName(this->m_metadata->ProductOrganization.AnnexFiles, "_CLM_XS");
}

template <typename PixelType, typename MasksPixelType>
std::string MAJAL8MetadataHelper<PixelType, MasksPixelType>::getWaterFileName(int res)
{
    return getQualityFileName(res);
}

template <typename PixelType, typename MasksPixelType>
std::string MAJAL8MetadataHelper<PixelType, MasksPixelType>::getSaturationFileName(int )
{
    return this->GetMACCSImageFileName(this->m_metadata->ProductOrganization.AnnexFiles, "_SAT_XS");
}

template <typename PixelType, typename MasksPixelType>
std::string MAJAL8MetadataHelper<PixelType, MasksPixelType>::getEdgeFileName(int)
{
    return this->GetMACCSImageFileName(this->m_metadata->ProductOrganization.AnnexFiles, "_EDG_XS");
}

template <typename PixelType, typename MasksPixelType>
std::string MAJAL8MetadataHelper<PixelType, MasksPixelType>::getSnowFileName(int res)
{
    // the water and snow masks are in the same file
    return getQualityFileName(res);
}

template <typename PixelType, typename MasksPixelType>
std::string MAJAL8MetadataHelper<PixelType, MasksPixelType>::getQualityFileName(int)
{
    return this->GetMACCSImageFileName(this->m_metadata->ProductOrganization.AnnexFiles, "_MG2_XS");
}


template <typename PixelType, typename MasksPixelType>
typename MetadataHelper<PixelType, MasksPixelType>::SingleBandMasksImageType::Pointer
         MAJAL8MetadataHelper<PixelType, MasksPixelType>::GetL2AMasksImage(MasksFlagType nMaskFlags, bool binarizeResult, int resolution) {
    // We use:
    //  - MG2 for CloudsMask, WaterMask, SnowMask
    //  - EDG ofor edge masks
    //  - SAT for saturation masks
    std::vector< typename MetadataHelper<PixelType, MasksPixelType>::SingleBandMasksImageType::Pointer> vecImgs;
    if((nMaskFlags & MSK_CLOUD) != 0 || (nMaskFlags & MSK_SNOW) != 0 || (nMaskFlags & MSK_WATER) != 0) {
        vecImgs.push_back(this->m_maskFlagsBandsExtractor.ExtractResampledBand(getQualityFileName(resolution), 1, Interpolator_NNeighbor));
    }

    if((nMaskFlags & MSK_SAT) != 0) {
        vecImgs.push_back(this->m_maskFlagsBandsExtractor.ExtractResampledBand(getSaturationFileName(resolution), 1, Interpolator_NNeighbor));
    }
    if((nMaskFlags & MSK_VALID) != 0) {
        vecImgs.push_back(this->m_maskFlagsBandsExtractor.ExtractResampledBand(getEdgeFileName(resolution), 1, Interpolator_NNeighbor));
    }

    this->m_majaMaskHandlerFunctor.Initialize(nMaskFlags, binarizeResult);
    if (vecImgs.size() > 1) {
        this->m_majaNMaskHandlerFilter = MAJANaryFunctorImageFilterType::New();
        this->m_majaNMaskHandlerFilter->SetFunctor(this->m_majaMaskHandlerFunctor);
        for(size_t i = 0; i<vecImgs.size(); i++) {
            this->m_majaNMaskHandlerFilter->SetInput(i, vecImgs[i]);
        }
        return this->m_majaNMaskHandlerFilter->GetOutput();
    } else {
        this->m_majaSingleMaskHandlerFilter = MAJAUnaryFunctorImageFilterType::New();
        this->m_majaSingleMaskHandlerFilter->SetFunctor(this->m_majaMaskHandlerFunctor);
            this->m_majaSingleMaskHandlerFilter->SetInput(vecImgs[0]);
        return this->m_majaSingleMaskHandlerFilter->GetOutput();
    }
}
