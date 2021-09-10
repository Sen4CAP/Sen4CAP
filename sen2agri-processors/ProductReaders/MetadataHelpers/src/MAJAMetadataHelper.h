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

#ifndef MAJAMETADATAHELPER_H
#define MAJAMETADATAHELPER_H

#include "MACCSS2MetadataHelper.h"
#include <vector>

#include "ResamplingBandExtractor.h"
#include "itkUnaryFunctorImageFilter.h"

#include "MAJAMaskHandlerFunctor.h"

template <typename PixelType, typename MasksPixelType>
class MAJAMetadataHelper : public MACCSS2MetadataHelper<PixelType, MasksPixelType>
{
public:
    typedef MAJAMaskHandlerFunctor<typename MetadataHelper<PixelType, MasksPixelType>::SingleBandMasksImageType::PixelType,
                                        typename MetadataHelper<PixelType, MasksPixelType>::SingleBandMasksImageType::PixelType>    MAJAMaskHandlerFunctorType;
    typedef itk::NaryFunctorImageFilter< typename MetadataHelper<PixelType, MasksPixelType>::SingleBandMasksImageType,
                                        typename MetadataHelper<PixelType, MasksPixelType>::SingleBandMasksImageType,
                                        MAJAMaskHandlerFunctorType>                             MAJANaryFunctorImageFilterType;

    typedef itk::UnaryFunctorImageFilter< typename MetadataHelper<PixelType, MasksPixelType>::SingleBandMasksImageType,
                                        typename MetadataHelper<PixelType, MasksPixelType>::SingleBandMasksImageType,
                                        MAJAMaskHandlerFunctorType>                             MAJAUnaryFunctorImageFilterType;

public:
    MAJAMetadataHelper();

    const char * GetNameOfClass() { return "MAJAMetadataHelper"; }

    virtual typename MetadataHelper<PixelType, MasksPixelType>::VectorImageType::Pointer GetImage(const std::vector<std::string> &bandNames, int outRes = -1);
    virtual typename MetadataHelper<PixelType, MasksPixelType>::VectorImageType::Pointer GetImage(const std::vector<std::string> &bandNames,
                                                std::vector<int> *pRetRelBandIdxs, int outRes = -1);
    virtual typename MetadataHelper<PixelType, MasksPixelType>::ImageListType::Pointer GetImageList(const std::vector<std::string> &bandNames,
                                                typename MetadataHelper<PixelType, MasksPixelType>::ImageListType::Pointer outImgList, int outRes = -1);

    virtual std::string GetAotImageFileName(int res);
    virtual float GetAotQuantificationValue(int res);
    virtual float GetAotNoDataValue(int res);
    virtual int GetAotBandIndex(int res);

    virtual typename MetadataHelper<PixelType, MasksPixelType>::SingleBandMasksImageType::Pointer GetL2AMasksImage(MasksFlagType nMaskFlags, bool binarizeResult,
                                                                                                int resolution = -1);
protected:
    virtual bool LoadAndCheckMetadata(const std::string &file);
    virtual bool BandAvailableForResolution(const std::string &bandName, int nRes);
    virtual std::string GetRasterFileExtension() { return ".TIF"; }

    virtual std::string getCloudFileName(int res);
    virtual std::string getWaterFileName(int res);
    virtual std::string getSnowFileName(int res);
    virtual std::string getQualityFileName(int res);
    virtual std::string getSaturationFileName(int res);
    virtual std::string getEdgeFileName(int res);

private:
    std::string GetImageFileName(const std::string &bandName);
    bool HasBandName(const std::vector<std::string> &bandNames, const std::string &bandName);
    bool GetValidBandNames(const std::vector<std::string> &bandNames, std::vector<std::string> &validBandNames,
                           std::vector<int> &relBandIndexes, int &outRes);
    float m_AotQuantifVal;
    float m_AotNoDataVal;

    MAJAMaskHandlerFunctorType m_majaMaskHandlerFunctor;
    // We are using 2 filters here as Nary functor needs at least two inputs while
    // we might have only one (MG2 for example)
    typename MAJAUnaryFunctorImageFilterType::Pointer m_majaSingleMaskHandlerFilter;
    typename MAJANaryFunctorImageFilterType::Pointer m_majaNMaskHandlerFilter;
};

#include "MAJAMetadataHelper.cpp"

#endif // MACCSMETADATAHELPERNEW_H
