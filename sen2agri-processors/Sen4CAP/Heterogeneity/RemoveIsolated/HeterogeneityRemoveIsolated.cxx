#include "otbWrapperApplication.h"
#include "otbWrapperApplicationFactory.h"
#include <cstdlib>
#include <vector>
#include "lwTime.h"
#include "lwString.h"
#include "otbImageFileReader.h"
#include "otbImageFileWriter.h"
#include "otbImage.h"
#include "engeRemoveIsolatedFilter.h"

#define SCALAR_INPUT unsigned char
#define SCALAR_OUTPUT unsigned char

namespace otb
{
namespace Wrapper
{
class HeterogeneityRemoveIsolated : public Application
{
public:
    typedef HeterogeneityRemoveIsolated Self;
    typedef Application Superclass;
    typedef itk::SmartPointer<Self> Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    itkNewMacro(Self)

    itkTypeMacro(HeterogeneityRemoveIsolated, otb::Application)

    typedef otb::Image<SCALAR_INPUT, 2> inputImageType;
    typedef otb::Image<SCALAR_OUTPUT, 2> outputImageType;
    typedef engeRemoveIsolatedFilter<inputImageType, outputImageType> majFilter;

private:
    HeterogeneityRemoveIsolated()
    {
    }

    void DoInit()
    {
        SetName("HeterogeneityRemoveIsolated");
        SetDescription("TBD");

        SetDocName("HeterogeneityRemoveIsolated");
        SetDocLongDescription("TBD");
        SetDocLimitations("None");
        SetDocAuthors("CUU");
        SetDocSeeAlso(" ");
        AddDocTag(Tags::Vector);

        AddParameter(ParameterType_InputImage,  "in",   "Input image");
        SetParameterDescription("in", "Input image");

        AddParameter(ParameterType_Int, "threshold", "threshold (replace by majority if nb of neighbors with same value < threshold; 1 pixel by default=actually alone in the window)");
        MandatoryOff("threshold");
        SetDefaultParameterInt("threshold", 1);

        AddParameter(ParameterType_Int, "radius", "radius of search (1 pixel by default = 3x3 window)");
        MandatoryOff("radius");
        SetDefaultParameterInt("radius", 1);

        AddParameter(ParameterType_Int, "nodata", "no-data value");
        MandatoryOff("nodata");
        SetDefaultParameterInt("nodata", 0);

        AddParameter(ParameterType_String, "classes", "list of classes (comma-separated without space; all by default)");
        MandatoryOff("classes");

        AddParameter(ParameterType_OutputImage, "out", "Out image file");
    }

    void DoUpdateParameters()
    {
    }

    void DoExecute()
    {
        int radius = 1;
        int minNeighbors = 1;
        int nan = 0;

        if(HasValue("threshold")) {
            minNeighbors = GetParameterInt("threshold");
        }
        if(HasValue("radius")) {
            radius = GetParameterInt("radius");
        }
        if(HasValue("nodata")) {
            nan = GetParameterInt("nodata");
        }
        std::vector<SCALAR_INPUT> classes;
        if(HasValue("classes")) {
            classes = lwString::splitNum<SCALAR_INPUT>(GetParameterString("classes"), ',');
        }

        otbAppLogINFO("Starting with a " << radius*2+1 << "x" << radius*2+1 << " window and min neighbors = " << minNeighbors);

        majF = majFilter::New();
        majF->SetInput(0, this->GetParameterImage<inputImageType>("in") );
        majF->setRadius( radius );
        majF->setMinNeighbors( minNeighbors );
        majF->setNaN( nan );
        majF->setClasses( classes );
        SetParameterOutputImage("out", majF->GetOutput());
    }

    private:
        majFilter::Pointer majF;

};

} // end of namespace Wrapper
} // end of namespace otb

OTB_APPLICATION_EXPORT(otb::Wrapper::HeterogeneityRemoveIsolated)
