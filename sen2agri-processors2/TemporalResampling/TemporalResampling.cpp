#include "otbWrapperApplication.h"
#include "otbWrapperApplicationFactory.h"
#include "otbWrapperInputImageListParameter.h"

#include "otbFunctorImageFilter.h"
#include "otbVectorImage.h"
#include "otbWrapperTypes.h"

#include "otbFunctorImageFilter.h"

#include "engeMaskSerieFilter.h"
#include "engeStepInterpolationFilter.h"

#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>

namespace otb
{

namespace Wrapper
{
class TemporalResampling : public Application
{
public:
    typedef TemporalResampling Self;
    typedef Application Superclass;
    typedef itk::SmartPointer<Self> Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    itkNewMacro(Self);
    itkTypeMacro(Composite, otb::Application);

private:
    void DoInit() override
    {
        SetName("TemporalResampling");
        SetDescription("Resamples a temporal series");

        SetDocLongDescription("Resamples and gap-fills a temporal series.");
        SetDocLimitations("None");
        SetDocAuthors("LN");
        SetDocSeeAlso(" ");

        AddDocTag(Tags::Raster);

        AddParameter(ParameterType_InputImage, "in", "Input image");
        SetParameterDescription("in", "Input image.");

        AddParameter(ParameterType_InputImage, "mask", "Input validity mask");
        SetParameterDescription("mask", "Input validity mask.");

        AddParameter(ParameterType_StringList, "indates", "Input dates");
        SetParameterDescription("indates", "Input dates.");

        AddParameter(ParameterType_OutputImage, "out", "Output image");
        SetParameterDescription("out", "Output image.");

        AddParameter(ParameterType_StringList, "outdates", "Output dates");
        SetParameterDescription("outdates", "Output dates.");

        AddParameter(ParameterType_Int, "bv", "Masked value");
        SetParameterDescription("bv", "Masked value.");

        AddParameter(ParameterType_Int, "nan", "NaN value");
        SetParameterDescription("nan", "NaN value.");
        MandatoryOff("nan");

        AddParameter(ParameterType_Double, "maxdist", "Maximum distance between two input dates");
        SetParameterDescription("maxdist", "Maximum distance between two input dates.");
        MandatoryOff("maxdist");

        AddParameter(ParameterType_Double, "winradius", "Window radius");
        SetParameterDescription("winradius", "Window radius.");
        MandatoryOff("winradius");

        AddRAMParameter();

        SetDocExampleParameterValue("in", "in.tif");
        SetDocExampleParameterValue("mask", "mask.tif");
        SetDocExampleParameterValue("out", "out.tif");
    }

    void DoUpdateParameters() override
    {
    }

    void DoExecute() override
    {
        const auto inDatesStr = GetParameterStringList("indates");
        std::vector<double> inDates;
        for (const auto &d : inDatesStr) {
            inDates.emplace_back(std::stod(d));
        }

        const auto outDatesStr = GetParameterStringList("outdates");
        std::vector<double> outDates;
        for (const auto &d : outDatesStr) {
            outDates.emplace_back(std::stod(d));
        }

        auto maskedValue = static_cast<int16_t>(GetParameterInt("bv"));

        int16_t nan = 0;
        auto maxDist = 0;
        auto windowRadius = 0;

        if (HasValue("nan")) {
            nan = static_cast<int16_t>(GetParameterInt("nan"));
        }
        if (HasValue("maxdist")) {
            maxDist = GetParameterDouble("maxdist");
        }
        if (HasValue("winradius")) {
            windowRadius = GetParameterDouble("winRadius");
        }

        const auto inImage = GetParameterInt16VectorImage("in");
        const auto maskImage = GetParameterInt16VectorImage("mask");

        std::vector<int16_t> replacingValues = { nan };

        auto maskFilter = engeMaskSerieFilter<Int16VectorImageType, Int16VectorImageType,
                                              Int16VectorImageType>::New();
        maskFilter->SetInput(0, inImage);
        maskFilter->SetInput(1, maskImage);
        maskFilter->setMasks({ maskedValue }, replacingValues);
        maskFilter->setInvertedMode(true);

        auto interpolationFilter =
            engeStepInterpolationFilter<Int16VectorImageType, Int16VectorImageType>::New();

        interpolationFilter->SetInput(maskFilter->GetOutput());
        interpolationFilter->setInputTimes(inDates);
        interpolationFilter->setOutputTimes(outDates);
        interpolationFilter->setMaxDist(maxDist);
        interpolationFilter->setWindowRadius(windowRadius);
        interpolationFilter->setNaN(nan);

        SetParameterOutputImage("out", interpolationFilter->GetOutput());
        SetParameterOutputImagePixelType("out", ImagePixelType_int16);

        RegisterPipeline();
    }
};
} // namespace Wrapper
} // namespace otb

OTB_APPLICATION_EXPORT(otb::Wrapper::TemporalResampling)
