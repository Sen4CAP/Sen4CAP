#include "otbWrapperApplication.h"
#include "otbWrapperApplicationFactory.h"

#include <cstdlib>
#include <vector>
#include "lwTime.h"
#include "lwString.h"
#include "otbImageFileReader.h"
#include "otbImageFileWriter.h"
#include "otbImage.h"
#include "engeLocalClassConnectivityIndexFilter.h"

#define SCALAR_INPUT short int
#define SCALAR_OUTPUT short int

namespace otb
{
namespace Wrapper
{
class HeterogeneityLocalClassConnectivityIndex : public Application
{
public:
    typedef HeterogeneityLocalClassConnectivityIndex Self;
    typedef Application Superclass;
    typedef itk::SmartPointer<Self> Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    itkNewMacro(Self)

    itkTypeMacro(HeterogeneityLocalClassConnectivityIndex, otb::Application)

    typedef otb::Image<SCALAR_INPUT, 2> inputImageType;
    typedef otb::Image<SCALAR_OUTPUT, 2> outputImageType;
    typedef engeLocalClassConnectivityIndexFilter<inputImageType, outputImageType> connIndexFilter;

private:
    HeterogeneityLocalClassConnectivityIndex()
    {
    }

    void DoInit()
    {
        SetName("HeterogeneityLocalClassConnectivityIndex");
        SetDescription("TBD");

        SetDocName("HeterogeneityRemoveIsolated");
        SetDocLongDescription("TBD");
        SetDocLimitations("None");
        SetDocAuthors("CUU");
        SetDocSeeAlso(" ");
        AddDocTag(Tags::Vector);

        AddParameter(ParameterType_InputImage,  "in",   "Input image");
        SetParameterDescription("in", "Input image");

        AddParameter(ParameterType_Int, "fullconnectivity", "Connectivity with diagonal pixels (on/off = 1/0; off by default)");
        MandatoryOff("fullconnectivity");
        SetDefaultParameterInt("fullconnectivity", 0);

        AddParameter(ParameterType_Int, "radius", "Radius of search (5 pixels by default)");
        MandatoryOff("radius");
        SetDefaultParameterInt("radius", 5);

        AddParameter(ParameterType_OutputImage, "out", "Out image file");
    }

    void DoUpdateParameters()
    {
    }

    void DoExecute()
    {
        int radius = 5;
        bool fullConnectivity = false;

        if(HasValue("fullconnectivity")) {
            fullConnectivity = (GetParameterInt("fullconnectivity") != 0);
        }
        if(HasValue("radius")) {
            radius = GetParameterInt("radius");
        }

        connIndexF = connIndexFilter::New();
        connIndexF->SetInput(0, this->GetParameterImage<inputImageType>("in") );
        connIndexF->setRadius( radius );
        connIndexF->setFullConnection( fullConnectivity );
        SetParameterOutputImage("out", connIndexF->GetOutput());
    }

    private:
        connIndexFilter::Pointer connIndexF;

};

} // end of namespace Wrapper
} // end of namespace otb

OTB_APPLICATION_EXPORT(otb::Wrapper::HeterogeneityLocalClassConnectivityIndex)
