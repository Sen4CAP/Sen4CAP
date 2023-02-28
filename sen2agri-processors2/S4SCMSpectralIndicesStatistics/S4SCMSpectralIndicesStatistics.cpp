#include "otbWrapperApplication.h"
#include "otbWrapperApplicationFactory.h"
#include "otbWrapperInputImageListParameter.h"

#include "otbFunctorImageFilter.h"
#include "otbVectorImage.h"
#include "otbWrapperTypes.h"

#include "otbFunctorImageFilter.h"

#include <itkFixedArray.h>

namespace otb
{

namespace Wrapper
{
class S4SCMSpectralIndicesStatistics : public Application
{
public:
    typedef S4SCMSpectralIndicesStatistics Self;
    typedef Application Superclass;
    typedef itk::SmartPointer<Self> Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    itkNewMacro(Self);
    itkTypeMacro(Composite, otb::Application);

private:
    void DoInit() override
    {
        SetName("S4SCMSpectralIndicesStatistics");
        SetDescription("Computes minimum, maximum, mean, median and standard deviation statistics");

        SetDocLongDescription(
            "Computes minimum, maximum, mean, median and standard deviation statistics.");
        SetDocLimitations("None");
        SetDocAuthors("LN");
        SetDocSeeAlso(" ");

        AddDocTag(Tags::Raster);

        AddParameter(ParameterType_InputImage, "in", "Input image");
        SetParameterDescription("in", "Input image.");

        // AddParameter(ParameterType_Int, "bv", "Background value");
        // SetParameterDescription("bv", "Background value to ignore in computation.");
        // SetDefaultParameterInt("bv", 0);
        // MandatoryOff("bv");

        AddParameter(ParameterType_OutputImage, "out", "Output image");
        SetParameterDescription("out", "Output image.");

        AddRAMParameter();

        SetDocExampleParameterValue("in", "ndvi.tif");
        SetDocExampleParameterValue("out", "ndvi-statistics.tif");
    }

    void DoUpdateParameters() override
    {
    }

    void DoExecute() override
    {
        const auto inputImage = GetParameterInt16VectorImage("in");

        // const auto bv = GetParameterInt("bv");

        const auto statistics = [](itk::VariableLengthVector<int16_t> &out,
                                   itk::VariableLengthVector<int16_t> in) {
            auto size = in.GetSize();
            const auto begin = &in[0];
            const auto end = &in[size];
            std::sort(begin, end);
            auto sum = 0.0f;
            for (size_t i = 0; i < size; i++) {
                sum += in[i];
            }

            auto mean = 0.0f;
            if (size > 0) {
                mean = sum / size;
            }

            auto min = 0.0f;
            if (size > 2) {
                min = (static_cast<float>(in[0]) + in[1] + in[2]) / 3.0f;
            } else if (size == 2) {
                min = (static_cast<float>(in[0]) + in[1]) / 2.0f;
            } else if (size == 1) {
                min = in[0];
            }

            auto max = 0.0f;
            if (size > 2) {
                max = (static_cast<float>(in[size - 1]) + in[size - 2] + in[size - 3]) / 3.0f;
            } else {
                max = min;
            }

            auto median = 0.0f;
            if (size % 2 == 1) {
                median = in[size / 2];
            } else {
                median = (static_cast<float>(in[size / 2 - 1]) + in[size / 2]) / 2.0f;
            }

            auto stddev = 0.0f;
            if (size > 0) {
                for (size_t i = 0; i < size; i++) {
                    stddev += (in[i] - mean) * (in[i] - mean);
                }
                stddev /= size;
                stddev = sqrt(stddev);
            }

            mean = round(mean);
            min = round(min);
            max = round(max);
            median = round(median);
            stddev = round(stddev);

            out[0] = min;
            out[1] = max;
            out[2] = mean;
            out[3] = median;
            out[4] = stddev;
        };

        const auto statisticsFilter = NewFunctorFilter(statistics, 5, { { 0, 0 } });
        statisticsFilter->SetInputs(inputImage);

        SetParameterOutputImage("out", statisticsFilter->GetOutput());
        SetParameterOutputImagePixelType("out", ImagePixelType_int16);

        RegisterPipeline();
    }
};
} // namespace Wrapper
} // namespace otb

OTB_APPLICATION_EXPORT(otb::Wrapper::S4SCMSpectralIndicesStatistics)
