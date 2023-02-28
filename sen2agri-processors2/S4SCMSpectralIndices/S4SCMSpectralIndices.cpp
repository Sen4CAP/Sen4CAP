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
class S4SCMSpectralIndices : public Application
{
public:
    typedef S4SCMSpectralIndices Self;
    typedef Application Superclass;
    typedef itk::SmartPointer<Self> Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    itkNewMacro(Self);
    itkTypeMacro(Composite, otb::Application);

private:
    void DoInit() override
    {
        SetName("S4SCMSpectralIndices");
        SetDescription("Computes NDVI, NDWI and brightness time series");

        SetDocLongDescription("Computes NDVI, NDWI and brightness time series.");
        SetDocLimitations("None");
        SetDocAuthors("LN");
        SetDocSeeAlso(" ");

        AddDocTag(Tags::Raster);

        AddParameter(ParameterType_InputImage, "b3", "Input band 3");
        SetParameterDescription("b3", "Input band 3.");

        AddParameter(ParameterType_InputImage, "b4", "Input band 4");
        SetParameterDescription("b4", "Input band 4.");

        AddParameter(ParameterType_InputImage, "b8", "Input band 8");
        SetParameterDescription("b8", "Input band 8.");

        AddParameter(ParameterType_InputImage, "b11", "Input band 11");
        SetParameterDescription("b11", "Input band 11.");

        AddParameter(ParameterType_Int, "bv", "Background value");
        SetParameterDescription("bv", "Background value to ignore in computation.");
        SetDefaultParameterInt("bv", 0);
        MandatoryOff("bv");

        AddParameter(ParameterType_OutputImage, "outndvi", "Output NDVI image");
        SetParameterDescription("outndvi", "Output NDVI image.");

        AddParameter(ParameterType_OutputImage, "outndwi", "Output NDWI image");
        SetParameterDescription("outndwi", "Output NDWI image.");

        AddParameter(ParameterType_OutputImage, "outbrightness", "Output brightness image");
        SetParameterDescription("outbrightness", "Output brightness image.");

        AddRAMParameter();

        SetDocExampleParameterValue("b3", "b3.tif");
        SetDocExampleParameterValue("b4", "b4.tif");
        SetDocExampleParameterValue("b8", "b8.tif");
        SetDocExampleParameterValue("b11", "b11.tif");
        SetDocExampleParameterValue("bv", "-10000");
        SetDocExampleParameterValue("outndvi", "ndvi.tif");
        SetDocExampleParameterValue("outndwi", "ndwi.tif");
        SetDocExampleParameterValue("outbrightness", "brightness.tif");

        SetMultiWriting(true);
    }

    void DoUpdateParameters() override
    {
    }

    void DoExecute() override
    {
        const auto b3Image = GetParameterInt16VectorImage("b3");
        const auto b4Image = GetParameterInt16VectorImage("b4");
        const auto b8Image = GetParameterInt16VectorImage("b8");
        const auto b11Image = GetParameterInt16VectorImage("b11");

        const auto bv = GetParameterInt("bv");

        const auto ndviFunc = [bv](int16_t red, int16_t nir) -> int16_t {
            if (red == bv || nir == bv || nir + red == 0) {
                return 0;
            } else {
                auto ndviFloat = static_cast<float>(nir - red) / (nir + red);
                if (ndviFloat < -5) {
                    ndviFloat = -5;
                } else if (ndviFloat > 5) {
                    ndviFloat = 5;
                }
                ndviFloat = round(ndviFloat * 10000);
                return static_cast<int16_t>(ndviFloat);
            }
        };

        const auto ndwiFunc = [bv](int16_t nir, int16_t swir) -> int16_t {
            if (nir == bv || swir == bv || nir + swir == 0) {
                return 0;
            } else {
                auto ndwiFloat = static_cast<float>(nir - swir) / (nir + swir);
                if (ndwiFloat < -1) {
                    ndwiFloat = -1;
                } else if (ndwiFloat > 1) {
                    ndwiFloat = 1;
                }
                ndwiFloat = round(ndwiFloat * 10000);
                return static_cast<int16_t>(ndwiFloat);
            }
        };

        const auto brightnessFunc = [bv](int16_t green, int16_t red, int16_t nir,
                                         int16_t swir) -> int16_t {
            if (green == bv || red == bv || nir == bv || swir == bv) {
                return 0;
            } else {
                auto greenFloat = static_cast<float>(green);
                auto redFloat = static_cast<float>(red);
                auto nirFloat = static_cast<float>(nir);
                auto swirFloat = static_cast<float>(swir);

                auto brightnessFloat = sqrt(green * green + red * red + nir * nir + swir * swir);
                brightnessFloat = round(brightnessFloat);

                return static_cast<int16_t>(brightnessFloat);
            }
        };

        const auto ndviFunctor = [ndviFunc, bv](itk::VariableLengthVector<int16_t> &ndvi,
                                                const itk::VariableLengthVector<int16_t> &red,
                                                const itk::VariableLengthVector<int16_t> &nir) {
            for (size_t i = 0; i < ndvi.GetSize(); i++) {
                ndvi[i] = ndviFunc(red[i], nir[i]);
            }
        };

        const auto ndwiFunctor = [ndwiFunc, bv](itk::VariableLengthVector<int16_t> &ndwi,
                                                const itk::VariableLengthVector<int16_t> &nir,
                                                const itk::VariableLengthVector<int16_t> &swir) {
            for (size_t i = 0; i < ndwi.GetSize(); i++) {
                ndwi[i] = ndwiFunc(nir[i], swir[i]);
            }
        };

        const auto brightnessFunctor = [brightnessFunc,
                                        bv](itk::VariableLengthVector<int16_t> &brightness,
                                            const itk::VariableLengthVector<int16_t> &green,
                                            const itk::VariableLengthVector<int16_t> &red,
                                            const itk::VariableLengthVector<int16_t> &nir,
                                            const itk::VariableLengthVector<int16_t> &swir) {
            for (size_t i = 0; i < brightness.GetSize(); i++) {
                brightness[i] = brightnessFunc(green[i], red[i], nir[i], swir[i]);
            }
        };

        auto components = b3Image->GetNumberOfComponentsPerPixel();

        const auto ndviFilter = NewFunctorFilter(ndviFunctor, components, { { 0, 0 } });
        ndviFilter->SetInputs(b4Image, b8Image);

        const auto ndwiFilter = NewFunctorFilter(ndwiFunctor, components, { { 0, 0 } });
        ndwiFilter->SetInputs(b8Image, b11Image);

        const auto brightnessFilter = NewFunctorFilter(brightnessFunctor, components, { { 0, 0 } });
        brightnessFilter->SetInputs(b3Image, b4Image, b8Image, b11Image);

        SetParameterOutputImage("outndvi", ndviFilter->GetOutput());
        SetParameterOutputImage("outndwi", ndwiFilter->GetOutput());
        SetParameterOutputImage("outbrightness", brightnessFilter->GetOutput());

        SetParameterOutputImagePixelType("outndvi", ImagePixelType_int16);
        SetParameterOutputImagePixelType("outndwi", ImagePixelType_int16);
        SetParameterOutputImagePixelType("outbrightness", ImagePixelType_int16);

        RegisterPipeline();
    }
};
} // namespace Wrapper
} // namespace otb

OTB_APPLICATION_EXPORT(otb::Wrapper::S4SCMSpectralIndices)
