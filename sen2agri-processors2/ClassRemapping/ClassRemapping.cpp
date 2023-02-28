#include "otbWrapperApplication.h"
#include "otbWrapperApplicationFactory.h"
#include "otbWrapperInputImageListParameter.h"

#include "otbFunctorImageFilter.h"
#include "otbVectorImage.h"
#include "otbWrapperTypes.h"

#include "otbFunctorImageFilter.h"

#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>

namespace otb
{

namespace Wrapper
{
class ClassRemapping : public Application
{
public:
    typedef ClassRemapping Self;
    typedef Application Superclass;
    typedef itk::SmartPointer<Self> Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    itkNewMacro(Self);
    itkTypeMacro(Composite, otb::Application);

    std::unordered_map<uint16_t, uint16_t> mapping;

private:
    void DoInit() override
    {
        SetName("ClassRemapping");
        SetDescription("Remaps values in a labelled raster");

        SetDocLongDescription("Remaps values in a labelled raster.");
        SetDocLimitations("None");
        SetDocAuthors("LN");
        SetDocSeeAlso(" ");

        AddDocTag(Tags::Raster);

        AddParameter(ParameterType_InputImage, "in", "Input image");
        SetParameterDescription("in", "Input image.");

        AddParameter(ParameterType_OutputImage, "out", "Output image");
        SetParameterDescription("out", "Output image.");

        AddParameter(ParameterType_InputFilename, "table", "Remapping table");
        SetParameterDescription("table", "Remapping table as headerless CSV.");

        AddRAMParameter();

        SetDocExampleParameterValue("in", "in.tif");
        SetDocExampleParameterValue("out", "out.tif");
        SetDocExampleParameterValue("table", "remapping-table.csv");
    }

    void DoUpdateParameters() override
    {
    }

    void DoExecute() override
    {
        const auto remappingTableName = GetParameterString("table");
        std::ifstream stream(remappingTableName);

        std::string line;
        std::string token;
        while (std::getline(stream, line)) {
            std::istringstream ss(line);
            std::getline(ss, token, ',');
            auto original_code = static_cast<uint16_t>(std::stoi(token));
            std::getline(ss, token, ',');
            auto remapped_code = static_cast<uint16_t>(std::stoi(token));
            mapping[original_code] = remapped_code;
        }

        const auto inImage = GetParameterInt16Image("in");

        for (auto const &pair : mapping) {
            std::cout << "{" << pair.first << ": " << pair.second << "}\n";
        }

        const auto remappingFunctor = [this](int16_t &out, int16_t in) { out = mapping[in]; };

        const auto remappingFilter = NewFunctorFilter(remappingFunctor);
        remappingFilter->SetInput(inImage);

        SetParameterOutputImage("out", remappingFilter->GetOutput());

        SetParameterOutputImagePixelType("out", ImagePixelType_int16);

        RegisterPipeline();
    }
};
} // namespace Wrapper
} // namespace otb

OTB_APPLICATION_EXPORT(otb::Wrapper::ClassRemapping)
