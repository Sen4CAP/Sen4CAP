#include "otbWrapperApplication.h"
#include "otbWrapperApplicationFactory.h"
#include "otbWrapperInputImageListParameter.h"

#include "otbFunctorImageFilter.h"
#include "otbVectorImage.h"
#include "otbWrapperTypes.h"

#include "otbFunctorImageFilter.h"

namespace otb
{

namespace Wrapper
{
class S4SCMRedEdgeFeatures : public Application
{
public:
    typedef S4SCMRedEdgeFeatures Self;
    typedef Application Superclass;
    typedef itk::SmartPointer<Self> Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    itkNewMacro(Self);
    itkTypeMacro(Composite, otb::Application);

private:
    void DoInit() override
    {
        SetName("S4SCMRedEdgeFeatures");
        SetDescription("Computes a red-edge features time series");

        SetDocLongDescription("Computes NDRE, REPI, PSRI, CIRE series.");
        SetDocLimitations("None");
        SetDocAuthors("LN");
        SetDocSeeAlso(" ");

        AddDocTag(Tags::Raster);

        AddParameter(ParameterType_InputImage, "b2", "Input band 2");
        SetParameterDescription("b2", "Input band 2.");

        AddParameter(ParameterType_InputImage, "b4", "Input band 4");
        SetParameterDescription("b4", "Input band 4.");

        AddParameter(ParameterType_InputImage, "b5", "Input band 5");
        SetParameterDescription("b5", "Input band 5.");

        AddParameter(ParameterType_InputImage, "b6", "Input band 6");
        SetParameterDescription("b6", "Input band 6.");

        AddParameter(ParameterType_InputImage, "b7", "Input band 7");
        SetParameterDescription("b7", "Input band 7.");

        AddParameter(ParameterType_InputImage, "b8", "Input band 8");
        SetParameterDescription("b8", "Input band 8.");

        AddParameter(ParameterType_Int, "bv", "Background value");
        SetParameterDescription("bv", "Background value to ignore in computation.");
        SetDefaultParameterInt("bv", 0);
        MandatoryOff("bv");

        AddParameter(ParameterType_OutputImage, "outndre", "Output NDRE image");
        AddParameter(ParameterType_OutputImage, "outrepi", "Output REPI image");
        AddParameter(ParameterType_OutputImage, "outpsri", "Output PSRI image");
        AddParameter(ParameterType_OutputImage, "outcire", "Output CIRE image");

        AddRAMParameter();

        SetDocExampleParameterValue("b2", "b2.tif");
        SetDocExampleParameterValue("b4", "b4.tif");
        SetDocExampleParameterValue("b5", "b5.tif");
        SetDocExampleParameterValue("b6", "b6.tif");
        SetDocExampleParameterValue("b7", "b7.tif");
        SetDocExampleParameterValue("b8", "b8.tif");
        SetDocExampleParameterValue("bv", "-10000");
        SetDocExampleParameterValue("outndre", "ndre.tif");
        SetDocExampleParameterValue("outrepi", "repi.tif");
        SetDocExampleParameterValue("outpsri", "psri.tif");
        SetDocExampleParameterValue("outcire", "cire.tif");

        SetMultiWriting(true);
    }

    void DoUpdateParameters() override {}

    void DoExecute() override
    {
        const auto b2Image = GetParameterInt16VectorImage("b2");
        const auto b4Image = GetParameterInt16VectorImage("b4");
        const auto b5Image = GetParameterInt16VectorImage("b5");
        const auto b6Image = GetParameterInt16VectorImage("b6");
        const auto b7Image = GetParameterInt16VectorImage("b7");
        const auto b8Image = GetParameterInt16VectorImage("b8");

        const auto bv = GetParameterInt("bv");

        // TODO: check these
        const auto ndreFunc = [bv](int16_t b6, int16_t b8) -> int16_t {
            if (b6 == bv || b8 == bv || b6 + b8 == 0) {
                return 0;
            } else {
                auto ndreFloat = static_cast<float>(b8 - b6) / (b8 + b6);
                if (ndreFloat < -5) {
                    ndreFloat = -5;
                } else if (ndreFloat > 5) {
                    ndreFloat = 5;
                }
                ndreFloat = round(ndreFloat * 10000);
                return static_cast<int16_t>(ndreFloat);
            }
        };

        const auto repiFunc = [bv](int16_t b4, int16_t b5, int16_t b6, int16_t b7) -> int16_t {
            if (b4 == bv || b5 == bv || b6 == bv || b7 == bv || b6 - b5 == 0) {
                return 0;
            } else {
                auto repiFloat = 705.0f + 35.0f * ((b7 + b4) * 0.5f - b5) / (b6 + b5);
                repiFloat = round(repiFloat);
                return static_cast<int16_t>(repiFloat);
            }
        };

        const auto psriFunc = [bv](int16_t b2, int16_t b4, int16_t b5) -> int16_t {
            if (b2 == bv || b4 == bv || b5 == bv || b5 == 0) {
                return 0;
            } else {
                auto psriFloat = (b4 - b2) / b5;
                if (psriFloat < -5) {
                    psriFloat = -5;
                } else if (psriFloat > 5) {
                    psriFloat = 5;
                }
                psriFloat = round(psriFloat * 10000);
                return static_cast<int16_t>(psriFloat);
            }
        };

        const auto cireFunc = [bv](int16_t b5, int16_t b8) -> int16_t {
            if (b5 == bv || b8 == bv || b8 == 0) {
                return 0;
            } else {
                auto cireFloat = b5 / b8;
                if (cireFloat < -5) {
                    cireFloat = -5;
                } else if (cireFloat > 5) {
                    cireFloat = 5;
                }
                cireFloat = round(cireFloat * 10000);
                return static_cast<int16_t>(cireFloat);
            }
        };

        const auto ndreFunctor = [ndreFunc, bv](itk::VariableLengthVector<int16_t> &ndre,
                                                const itk::VariableLengthVector<int16_t> &b6,
                                                const itk::VariableLengthVector<int16_t> &b8) {
            for (size_t i = 0; i < ndre.GetSize(); i++) {
                ndre[i] = ndreFunc(b6[i], b8[i]);
            }
        };

        const auto repiFunctor = [repiFunc, bv](itk::VariableLengthVector<int16_t> &repi,
                                                const itk::VariableLengthVector<int16_t> &b2,
                                                const itk::VariableLengthVector<int16_t> &b5,
                                                const itk::VariableLengthVector<int16_t> &b6,
                                                const itk::VariableLengthVector<int16_t> &b7) {
            for (size_t i = 0; i < repi.GetSize(); i++) {
                repi[i] = repiFunc(b2[i], b5[i], b6[i], b7[i]);
            }
        };

        const auto psriFunctor = [psriFunc, bv](itk::VariableLengthVector<int16_t> &psri,
                                                const itk::VariableLengthVector<int16_t> &b2,
                                                const itk::VariableLengthVector<int16_t> &b4,
                                                const itk::VariableLengthVector<int16_t> &b5) {
            for (size_t i = 0; i < psri.GetSize(); i++) {
                psri[i] = psriFunc(b2[i], b4[i], b5[i]);
            }
        };

        const auto cireFunctor = [cireFunc, bv](itk::VariableLengthVector<int16_t> &cire,
                                                const itk::VariableLengthVector<int16_t> &b5,
                                                const itk::VariableLengthVector<int16_t> &b8) {
            for (size_t i = 0; i < cire.GetSize(); i++) {
                cire[i] = cireFunc(b5[i], b8[i]);
            }
        };

        auto components = b2Image->GetNumberOfComponentsPerPixel();

        const auto ndreFilter = NewFunctorFilter(ndreFunctor, components);
        ndreFilter->SetInputs(b6Image, b8Image);

        const auto repiFilter = NewFunctorFilter(repiFunctor, components);
        repiFilter->SetInputs(b2Image, b5Image, b6Image, b7Image);

        const auto psriFilter = NewFunctorFilter(psriFunctor, components);
        psriFilter->SetInputs(b2Image, b4Image, b5Image);

        const auto cireFilter = NewFunctorFilter(cireFunctor, components);
        cireFilter->SetInputs(b5Image, b8Image);

        SetParameterOutputImage("outndre", ndreFilter->GetOutput());
        SetParameterOutputImage("outrepi", repiFilter->GetOutput());
        SetParameterOutputImage("outpsri", psriFilter->GetOutput());
        SetParameterOutputImage("outcire", cireFilter->GetOutput());

        SetParameterOutputImagePixelType("outndre", ImagePixelType_int16);
        SetParameterOutputImagePixelType("outrepi", ImagePixelType_int16);
        SetParameterOutputImagePixelType("outpsri", ImagePixelType_int16);
        SetParameterOutputImagePixelType("outcire", ImagePixelType_int16);

        RegisterPipeline();
    }
};
} // namespace Wrapper
} // namespace otb

OTB_APPLICATION_EXPORT(otb::Wrapper::S4SCMRedEdgeFeatures)
