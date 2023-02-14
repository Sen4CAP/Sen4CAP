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

#include "DirectionalCorrection.h"
#include "ResampleAtS2Res.h"
#include "ComputeNDVI.h"
#include "CreateS2AnglesRaster.h"

namespace otb
{
namespace Wrapper
{
class CompositePreprocessing : public Application
{

    template< class TInput, class TOutput>
    class NoDataNormalizationFunctor
    {
    public:
        NoDataNormalizationFunctor() {}
        NoDataNormalizationFunctor& operator =(const NoDataNormalizationFunctor& copy)
        {
            m_fReflNoDataValue = copy.m_fReflNoDataValue;
            return *this;
        }
        bool operator!=( const NoDataNormalizationFunctor & ) const { return true; }
        bool operator==( const NoDataNormalizationFunctor & other ) const { return !(*this != other); }
        void Initialize(float fReflNoDataValue) {m_fReflNoDataValue = fReflNoDataValue;}
        const char * GetNameOfClass() { return "NoDataNormalizationFunctor"; }
        inline TOutput operator()( const TInput & A )
        {
            return HandleMultiSizeInput(A,
                   std::integral_constant<bool, HasSizeMethod<TInput>::Has>());
        }

        inline TOutput HandleMultiSizeInput(const TInput & A, std::true_type)
        {
            TOutput ret(A.Size());
            for(int i = 0; i<A.Size(); i++) {
                 if(fabs(A[i] - m_fReflNoDataValue) < NO_DATA_EPSILON) {
                     ret[i] = NO_DATA_VALUE;
                 } else {
                      ret[i] = A[i];
                 }
            }

            return ret;
        }

        inline TOutput HandleMultiSizeInput(const TInput & A, std::false_type)
        {
              return (fabs(A - m_fReflNoDataValue) < NO_DATA_EPSILON) ? NO_DATA_VALUE : A;
        }
    private:
        float m_fReflNoDataValue;
    };
public:
    typedef CompositePreprocessing Self;
    typedef Application Superclass;
    typedef itk::SmartPointer<Self> Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;
    itkNewMacro(Self)

    itkTypeMacro(CompositePreprocessing, otb::Application)

    typedef otb::Wrapper::FloatVectorImageType                    InputImageType;
    typedef otb::Wrapper::FloatVectorImageType                    ImageType1;
    typedef float                                                 PixelType;
    typedef otb::Image<PixelType, 2>                              ImageType2;
    typedef NoDataNormalizationFunctor <ImageType1::PixelType, ImageType1::PixelType> NoDataNormalizationFunctorType;
    typedef itk::UnaryFunctorImageFilter<ImageType1, ImageType1, NoDataNormalizationFunctorType> NoDataNormalizationFilterType;


private:

    void DoInit()
    {
        SetName("CompositePreprocessing");
        SetDescription("Resample the corresponding bands from LANDSAT or SPOT to S2 resolution");

        SetDocName("CompositePreprocessing");
        SetDocLongDescription("long description");
        SetDocLimitations("None");
        SetDocAuthors("CIU");
        SetDocSeeAlso(" ");

        AddParameter(ParameterType_String, "xml", "Input general L2A XML");
        AddParameter(ParameterType_Int, "res", "The resolution to be processed");
        SetDefaultParameterInt("res", -1);
        MandatoryOff("res");

        // Directional Correction parameters
        AddParameter(ParameterType_InputFilename, "scatcoef", "File containing coefficients for scattering function");
        MandatoryOff("scatcoef");
        AddParameter(ParameterType_String, "msk", "Image with 3 bands with cloud, water and snow masks");
        AddParameter(ParameterType_InputFilename, "bmap", "Master to secondary bands mapping");

        AddParameter(ParameterType_OutputImage, "outres", "Out Image at the original resolution");
        MandatoryOff("outres");
        AddParameter(ParameterType_OutputImage, "outcmres", "Out cloud mask image at the original  resolution");
        MandatoryOff("outcmres");
        AddParameter(ParameterType_OutputImage, "outwmres", "Out water mask image at the original  resolution");
        MandatoryOff("outwmres");
        AddParameter(ParameterType_OutputImage, "outsmres", "Out snow mask image at the original  resolution");
        MandatoryOff("outsmres");
        AddParameter(ParameterType_OutputImage, "outaotres", "Out snow mask image at the original  resolution");
        MandatoryOff("outaotres");

        AddParameter(ParameterType_String, "masterinfo", "Information about the product (created only if is master)");
        MandatoryOff("masterinfo");

        AddParameter(ParameterType_String, "pmxml", "A primary mission xml whose origin, dimensions and projection will be used");
        MandatoryOff("pmxml");

        SetDocExampleParameterValue("xml", "/path/to/L2Aproduct_maccs.xml");
        SetDocExampleParameterValue("msk", "/path/to/msks.tif");
        SetDocExampleParameterValue("allinone", "1");

        SetDocExampleParameterValue("outres", "/path/to/output_image.tif");
        SetDocExampleParameterValue("outcmres", "/path/to/output_image_cloud.tif");
        SetDocExampleParameterValue("outwmres", "/path/to/output_image_water.tif");
        SetDocExampleParameterValue("outsmres", "/path/to/output_image_snow.tif");
        SetDocExampleParameterValue("outaotres", "/path/to/output_image_aot.tif");

    }

    void DoUpdateParameters()
    {
      // Nothing to do.
    }

    void DoExecute()
    {
        std::string inXml = GetParameterAsString("xml");
        int res = GetParameterInt("res");
        std::string strBandsMappingFileName = GetParameterAsString("bmap");
        std::string mskImg = GetParameterAsString("msk");

        auto factory = MetadataHelperFactory::New();
        auto pHelper = factory->GetMetadataHelper<short>(inXml);

        std::string masterInfoFileName;
        if(HasValue("masterinfo")) {
            masterInfoFileName = GetParameterString("masterinfo");
        }
        std::string primaryMissionXml;
        if(HasValue("pmxml")) {
            primaryMissionXml = GetParameterString("pmxml");
        }
        m_resampleAtS2Res.Init(inXml, mskImg, strBandsMappingFileName,
                               res, masterInfoFileName, primaryMissionXml);
        m_resampleAtS2Res.DoExecute();

        // if we have detailded angles, then we apply the directional correction
        if(pHelper->HasDetailedAngles() && HasValue("scatcoef")) {
            otbAppLogINFO( "Running using scattering coefficients ..." );
            std::string scatCoeffsFile = GetParameterAsString("scatcoef");
            m_computeNdvi.DoInit(inXml, res);
            m_creatAngles.DoInit(res, inXml);
            ImageType1::Pointer anglesImg = m_creatAngles.DoExecute();
            ImageType2::Pointer ndviImg = m_computeNdvi.DoExecute();
            ImageType2::Pointer cldImg = m_resampleAtS2Res.GetResampledCloudMaskImg().GetPointer();
            ImageType2::Pointer watImg = m_resampleAtS2Res.GetResampledWaterMaskImg().GetPointer();
            ImageType2::Pointer snowImg = m_resampleAtS2Res.GetResampledSnowMaskImg().GetPointer();
            m_dirCorr.Init(res, inXml, scatCoeffsFile, cldImg, watImg, snowImg, anglesImg, ndviImg);
            m_dirCorr.DoExecute();
            SetParameterOutputImage("outres", m_dirCorr.GetCorrectedImg().GetPointer());
        } else {
            otbAppLogINFO( "Running without scattering coefficients ..." );
            float fReflNoDataValue = NO_DATA_VALUE;
            const std::string &reflNoDataVal = pHelper->GetNoDataValue();
            if (reflNoDataVal.size() > 0) {
                fReflNoDataValue = std::atoi(reflNoDataVal.c_str());
            }
            m_NoDataNormalizationFunctor.Initialize(fReflNoDataValue);
            m_NoDataNormalizationFilter = NoDataNormalizationFilterType::New();
            m_NoDataNormalizationFilter->SetFunctor(m_NoDataNormalizationFunctor);
            m_NoDataNormalizationFilter->SetInput(m_resampleAtS2Res.GetResampledMainImg());
            m_NoDataNormalizationFilter->UpdateOutputInformation();
            SetParameterOutputImage("outres", m_NoDataNormalizationFilter->GetOutput());

            //SetParameterOutputImage("outres", m_resampleAtS2Res.GetResampledMainImg());
        }
        SetParameterOutputImage("outcmres", m_resampleAtS2Res.GetResampledCloudMaskImg().GetPointer());
        SetParameterOutputImage("outwmres", m_resampleAtS2Res.GetResampledWaterMaskImg().GetPointer());
        SetParameterOutputImage("outsmres", m_resampleAtS2Res.GetResampledSnowMaskImg().GetPointer());
        SetParameterOutputImage("outaotres", m_resampleAtS2Res.GetResampledAotImg().GetPointer());
    }

private:
    ComputeNDVI             m_computeNdvi;
    CreateS2AnglesRaster    m_creatAngles;
    DirectionalCorrection   m_dirCorr;
    ResampleAtS2Res        m_resampleAtS2Res;
    NoDataNormalizationFunctorType  m_NoDataNormalizationFunctor;
    NoDataNormalizationFilterType::Pointer m_NoDataNormalizationFilter;

};
}
}

OTB_APPLICATION_EXPORT(otb::Wrapper::CompositePreprocessing)



