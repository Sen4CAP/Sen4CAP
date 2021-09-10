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

#include "otbWrapperApplication.h"
#include "otbWrapperApplicationFactory.h"

#include <vector>
#include "MetadataHelperFactory.h"
#include "otbStreamingResampleImageFilter.h"
#include "ImageResampler.h"
#include "GenericRSImageResampler.h"
#include "itkBinaryFunctorImageFilter.h"
#include <boost/algorithm/string/replace.hpp>

#include "AppExternalMaskProvider.h"

namespace otb
{

namespace Wrapper
{

class ValidityMaskExtractor : public Application
{
public:

    typedef ValidityMaskExtractor Self;
    typedef Application Superclass;
    typedef itk::SmartPointer<Self> Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;
    itkNewMacro(Self)

    itkTypeMacro(ValidityMaskExtractor, otb::Application)

    template< class TInput, class TInput2, class TOutput>
    class MaskImagesCombineFunctor
    {
    public:
        MaskImagesCombineFunctor() {}
        ~MaskImagesCombineFunctor() {}
        void Initialize(int prdMskValidVal, int extMskValidVal)
        {
            m_nPrdMskValidVal = prdMskValidVal;
            m_nExtMskValidVal = extMskValidVal;
        }
        bool operator!=( const MaskImagesCombineFunctor &) const
        {
          return false;
        }
        bool operator==( const MaskImagesCombineFunctor & other ) const
        {
          return !(*this != other);
        }

        inline TOutput operator()( const TInput & A, const TInput2 & B ) const
        {
            if (A != m_nPrdMskValidVal) {
                return A;
            }
            if (B != m_nExtMskValidVal) {
                return B;
            }
            return A;
        }
    private:
        int m_nPrdMskValidVal;
        int m_nExtMskValidVal;
    };

    typedef MetadataHelper<short>::SingleBandMasksImageType MaskImageType;
    typedef otb::ImageFileReader<MaskImageType>             ExternaMaskImageReaderType;
    typedef otb::ImageFileWriter<MaskImageType> WriterType;
    typedef otb::StreamingResampleImageFilter<MaskImageType, MaskImageType, double>     ResampleFilterType;

    typedef itk::BinaryFunctorImageFilter<MaskImageType,MaskImageType,MaskImageType,
                    MaskImagesCombineFunctor<
                        MaskImageType::PixelType, MaskImageType::PixelType,
                        MaskImageType::PixelType> > MaskImagesCombineFilterType;

private:
    void DoInit()
    {
        SetName("ValidityMaskExtractor");
        SetDescription("Extracts the binary validity mask for the provided L2A product.");

        SetDocName("ValidityMaskExtractor");
        SetDocLongDescription("Extracts the binary validity mask for the provided L2A product.");
        SetDocLimitations("None");
        SetDocAuthors("CIU");
        SetDocSeeAlso(" ");
        AddDocTag(Tags::Vector);
        AddParameter(ParameterType_String,  "xml",   "Input metadata for the L2A product");

        // Add also the external mask parameters
        AddParameter(ParameterType_String,  "extmask",   "Input additional external mask.");
        MandatoryOff("extmask");

        AddParameter(ParameterType_Int,  "extmaskvalidval",   "External mask validity value.");
        MandatoryOff("extmaskvalidval");
        SetDefaultParameterInt("extmaskvalidval", 0);

        AddParameter(ParameterType_StringList,  "out",   "The output(s) mask flags image corresponding extracted from the provided L2A product. "
                                                          "If multiple values are provided, then the outres parameter is also mandatory."
                                                         "The output validity map will return a value = 0 for valid pixels and 1 for invalid pixels.");

        AddParameter(ParameterType_Int, "compress", "If set to a value different of 0, the output is compressed");
        MandatoryOff("compress");
        SetDefaultParameterInt("compress", 0);

        AddParameter(ParameterType_Int, "cog", "If set to a value different of 0, the output is created in cloud optimized geotiff and compressed.");
        MandatoryOff("cog");
        SetDefaultParameterInt("cog", 0);

        AddParameter(ParameterType_StringList, "outres", "Output resolutions. If provided, a value for each output will be expected. "
                                                         "If not provided, only one element is expected in outs parameter."
                                                         "Providing -1 as value, the corresponding output will be in the native resolution of the product.");
        MandatoryOff("outres");
    }

    void DoUpdateParameters()
    {
      // Nothing to do.
    }

    void DoExecute()
    {
        const std::string &inXml = GetParameterAsString("xml");
        std::vector<std::string> outImgs = GetParameterStringList("out");
        const std::vector<std::string> &outResStrs = GetParameterStringList("outres");
        std::vector<int> outRes;
        for(const std::string &resStr: outResStrs) {
            int res = std::atoi(resStr.c_str());
            if (res != -1 && res != 10 && res != 20) {
                itkExceptionMacro("Values supported for resolutions are -1, 10 and 20 but was found " << resStr);
            }
            outRes.push_back(res);
        }
        if (outRes.size() > 0) {
            if (outImgs.size() != outRes.size()) {
                itkExceptionMacro("Invalid number of output resolutions specified " << outRes.size() <<
                                  ". It should be equal with the number of output files" << outImgs.size());
            }
        } else {
            if (outImgs.size() != 1) {
                itkExceptionMacro("Only one output image expected if no outres provided but where given a number of outputs equal with" << outImgs.size());
            }
        }

        otbAppLogINFO("Using metadata file " << inXml);
        auto factory = MetadataHelperFactory::New();
        std::unique_ptr<MetadataHelper<short>> pHelper = factory->GetMetadataHelper<short>(inXml);
        MaskImageType::Pointer imgMsk = pHelper->GetMasksImage(ALL, false);
        imgMsk->UpdateOutputInformation();

        // remove the native resolution (set with -1) if the resolution is also requested explicitly (to avoid generating it twice)
        std::vector<int> indicesToRem;
        int curRes = imgMsk->GetSpacing()[0];
        for (size_t i = 0; i<outRes.size(); i++) {
            if (outRes[i] == -1) {
                std::ptrdiff_t pos = std::find(outRes.begin(), outRes.end(), curRes) - outRes.begin();
                if(pos < (int)outRes.size()) {
                    // we add this position to the removed resolutions
                    indicesToRem.push_back(i);
                }
            }
        }
        // Now remove the duplicated resolutions
        for (int i = (int)(indicesToRem.size()-1); i>= 0; i--) {
            outRes.erase(outRes.begin() + indicesToRem[i]);
            outImgs.erase(outImgs.begin() + indicesToRem[i]);
        }
        // Now adapt the output names if we have placeholders
        for(size_t i = 0; i<outImgs.size(); i++) {
            boost::replace_all(outImgs[i], "###RES###", std::to_string((int)imgMsk->GetSpacing()[0]));
        }

        for(size_t i = 0; i<outImgs.size(); i++) {
            if(outRes.size() > 0) {
                WriteOutput(imgMsk, outImgs[i], outRes[i]);
            } else {
                WriteOutput(imgMsk, outImgs[i], -1);
            }
        }
    }

    void WriteOutput(MaskImageType::Pointer imgMsk, const std::string &outImg, int nRes) {

        MaskImageType::Pointer mskImg = imgMsk;

        boost::filesystem::path dirPath(outImg);
        boost::filesystem::create_directories(dirPath.parent_path());

        std::string fileName(outImg);

        bool bCompress = (GetParameterInt("compress") != 0);
        bool bClodOptimizedGeotiff = (GetParameterInt("cog") != 0);

        if (bClodOptimizedGeotiff) {
            fileName += "?gdal:co:TILED=YES&gdal:co:COPY_SRC_OVERVIEWS=YES&gdal:co:COMPRESS=DEFLATE";
        } else if (bCompress) {
            fileName += std::string("?gdal:co:COMPRESS=DEFLATE");
        }

        // Create an output parameter to write the current output image
        OutputImageParameter::Pointer paramOut = OutputImageParameter::New();
        // Set the filename of the current output image
        paramOut->SetFileName(fileName);
        if(nRes != -1) {
            // resample the image at the given resolution
            imgMsk->UpdateOutputInformation();
            int curRes = imgMsk->GetSpacing()[0];
            mskImg = GetResampledImage(curRes, nRes, imgMsk);
        }
        UpdateRequiredImageSize(mskImg);

        // Create the provider for the external mask and extract the mask from the
        // App parameters
        AppExternalMaskProvider<short>::Pointer appExtMskProvider = AppExternalMaskProvider<short>::New();
        m_appExtMskProviders.push_back(appExtMskProvider);
        appExtMskProvider->SetLogger(this->GetLogger());
        appExtMskProvider->SetDefaultExtMaskLocation(GetParameterString("extmask"));
        MaskImageType::Pointer extMskImg = appExtMskProvider->GetExternalMask(nRes);

        // If an external mask was provided, then use this one too
        if (extMskImg.IsNotNull()) {
            MaskImageType::Pointer cutImg = CutImage(extMskImg);

            int extMskValidVal = GetParameterInt("extmaskvalidval");
            m_combineMasksFilter = MaskImagesCombineFilterType::New();
            m_combineMasksFilter->GetFunctor().Initialize(IMG_FLG_LAND, extMskValidVal);
            m_combineMasksFilter->SetInput1(mskImg);
            m_combineMasksFilter->SetInput2(cutImg);
            mskImg = m_combineMasksFilter->GetOutput();
            mskImg->UpdateOutputInformation();
        }
        paramOut->SetValue(mskImg);
        paramOut->SetPixelType(ImagePixelType_uint8);
        // Add the current level to be written
        paramOut->InitializeWriters();
        std::ostringstream osswriter;
        osswriter<< "Wrinting flags "<< outImg;
        AddProcess(paramOut->GetWriter(), osswriter.str());
        paramOut->Write();
    }

    MaskImageType::Pointer GetResampledImage(int nCurRes, int nDesiredRes,
                                                 MaskImageType::Pointer inImg) {
        if(nCurRes == nDesiredRes)
            return inImg;
        float fMultiplicationFactor = ((float)nCurRes)/nDesiredRes;
        ResampleFilterType::Pointer resampler = m_Resampler.getResampler(inImg, fMultiplicationFactor, Interpolator_NNeighbor);
        return resampler->GetOutput();
    }

    void UpdateRequiredImageSize(MaskImageType::Pointer inImg) {
        m_curMask = inImg;
        inImg->UpdateOutputInformation();

        m_curImgWidth = inImg->GetLargestPossibleRegion().GetSize()[0];
        m_curImgHeight = inImg->GetLargestPossibleRegion().GetSize()[1];

        m_nCurImgRes = inImg->GetSpacing()[0];

        MaskImageType::PointType origin = inImg->GetOrigin();
        m_curImgOrigin[0] = origin[0];
        m_curImgOrigin[1] = origin[1];

        m_strCurImgProjRef = inImg->GetProjectionRef();
        m_GenericRSImageResampler.SetOutputProjection(m_strCurImgProjRef);
    }

    MaskImageType::Pointer CutImage(const MaskImageType::Pointer &img) {
        MaskImageType::Pointer retImg = img;
        float imageWidth = img->GetLargestPossibleRegion().GetSize()[0];
        float imageHeight = img->GetLargestPossibleRegion().GetSize()[1];

        MaskImageType::PointType origin = img->GetOrigin();
        MaskImageType::PointType imageOrigin;
        imageOrigin[0] = origin[0];
        imageOrigin[1] = origin[1];
        int curImgRes = img->GetSpacing()[0];
        const float scale = (float)m_nCurImgRes / curImgRes;

        if((imageWidth != m_curImgWidth) || (imageHeight != m_curImgHeight) ||
                (m_curImgOrigin[0] != imageOrigin[0]) || (m_curImgOrigin[1] != imageOrigin[1])) {

            Interpolator_Type interpolator = Interpolator_NNeighbor;
            std::string imgProjRef = img->GetProjectionRef();
            // if the projections are equal
            if(imgProjRef == m_strCurImgProjRef) {
                // use the streaming resampler
                retImg = m_Resampler.getResampler(img, scale,m_curImgWidth,
                            m_curImgHeight,m_curImgOrigin, interpolator)->GetOutput();
            } else {
                // use the generic RS resampler that allows reprojecting
                retImg = m_GenericRSImageResampler.getResampler(img, scale,m_curImgWidth,
                            m_curImgHeight,m_curImgOrigin, interpolator)->GetOutput();
            }
            retImg->UpdateOutputInformation();
        }

        return retImg;
    }

private:
    ExternaMaskImageReaderType::Pointer m_reader;
    ImageResampler<MaskImageType, MaskImageType> m_Resampler;

    MaskImageType::Pointer m_curMask;
    std::string m_strCurImgProjRef;
    GenericRSImageResampler<MaskImageType, MaskImageType>  m_GenericRSImageResampler;
    double                                m_curImgWidth;
    double                                m_curImgHeight;
    MaskImageType::PointType              m_curImgOrigin;
    int                                   m_nCurImgRes;

    MaskImagesCombineFilterType::Pointer m_combineMasksFilter;

    std::vector<AppExternalMaskProvider<short>::Pointer> m_appExtMskProviders;
};

}
}
OTB_APPLICATION_EXPORT(otb::Wrapper::ValidityMaskExtractor)



