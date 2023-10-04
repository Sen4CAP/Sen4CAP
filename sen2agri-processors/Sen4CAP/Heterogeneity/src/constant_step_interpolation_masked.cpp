#include "otbWrapperApplication.h"
#include "otbWrapperApplicationFactory.h"
#include <cstdlib>
#include <vector>
#include "lwTime.h"
#include "lwString.h"
#include "otbImageFileReader.h"
#include "otbImageFileWriter.h"
#include "otbImage.h"
#include "engeStepInterpolationFilter.h"
#include "engeMaskSerieFilter.h"

#ifndef SCALAR_MC
#define SCALAR_MC float
#endif

namespace otb
{
namespace Wrapper
{
class ConstantStepInterpolationMasked : public Application
{
public:
    typedef ConstantStepInterpolationMasked Self;
    typedef Application Superclass;
    typedef itk::SmartPointer<Self> Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    itkNewMacro(Self)

    itkTypeMacro(ConstantStepInterpolationMasked, otb::Application)

    typedef otb::VectorImage<SCALAR_MC, 2> inputImageType;
    typedef otb::VectorImage<SCALAR_MC, 2> outputImageType;

private:
    ConstantStepInterpolationMasked()
    {
    }

    void DoInit()
    {
        SetName("ConstantStepInterpolationMasked");
        SetDescription("TBD");

        SetDocName("ConstantStepInterpolationMasked");
        SetDocLongDescription("TBD");
        SetDocLimitations("None");
        SetDocAuthors("CUU");
        SetDocSeeAlso(" ");
        AddDocTag(Tags::Vector);

        AddParameter(ParameterType_InputImage,  "in",   "Input image (multiband)");
        SetParameterDescription("in", "Input image (multiband)");

        AddParameter(ParameterType_InputImage,  "msk",   "Mask input image (multiband)");
        SetParameterDescription("msk", "Mask input image (multiband)");

        AddParameter(ParameterType_String, "times", "Data times corresponding to input bands (floats; eg. 0.1#0.6#3#18)");
        MandatoryOff("times");

        AddParameter(ParameterType_String, "stepper", "begin#interval#end (float; eg 0#1#15) for output bands times");
        MandatoryOff("stepper");

        AddParameter(ParameterType_String, "unmasked_values", "no-masked values: val1#val2#val3");
        MandatoryOff("unmasked_values");

        AddParameter(ParameterType_Float, "nodata", "Output nan value");
        MandatoryOff("nodata");
        SetDefaultParameterInt("nodata", 0.0);

        AddParameter(ParameterType_Float, "max_dist", "maximum distance (default: 0 = no max): if a hole length is > max, it remains a hole");
        MandatoryOff("max_dist");
        SetDefaultParameterInt("max_dist", 0.0);

        AddParameter(ParameterType_String, "min_max", "min#max valid data range");
        MandatoryOff("min_max");

        AddParameter(ParameterType_OutputImage, "out", "Out image file (multiband)");
    }

    void DoUpdateParameters()
    {
    }

    void DoExecute()
    {
        SCALAR_MC nan = lwScalar::getNaN<SCALAR_MC>();
        std::vector<double> times;
        std::vector<double> stepper;
        std::vector<SCALAR_MC> unmasked_values;
        double max_dist = 0;
        std::vector<double> min_max;

        if(HasValue("times")) {
            times = lwString::splitNum<double>( GetParameterString("times"), '#');
        }
        if(HasValue("stepper")) {
            stepper = lwString::splitNum<double>( GetParameterString("stepper"), '#');
        }
        if(HasValue("unmasked_values")) {
            unmasked_values = lwString::splitNum<SCALAR_MC>( GetParameterString("unmasked_values"), '#');
        }
        if(HasValue("nodata")) {
            nan = static_cast<SCALAR_MC>(GetParameterFloat("nodata"));
        }
        if(HasValue("max_dist")) {
            max_dist = GetParameterFloat("max_dist");
        }
        if(HasValue("min_max")) {
            min_max = lwString::splitNum<double>( GetParameterString("min_max"), '#');
        }

        std::vector<SCALAR_MC> replacing_values(unmasked_values.size());
        for(size_t i = 0; i < replacing_values.size(); ++i) {
            replacing_values[i] = nan;
        }

        if(stepper.size() != 3) {
            printf("The begin#interval#end argument must have three parts\n");
            for(size_t i = 0 ; i < stepper.size(); ++i)
                printf("%g ", stepper[i]);
            printf("\n");
            exit(EXIT_FAILURE);
        }
        if(stepper[1] <= 0.) {
            otbAppLogFATAL("The interval argument must be a positive number");
        }
        if(max_dist < 0) {
            otbAppLogFATAL("The begin#interval#end argument must have three parts");
        }
        if(min_max.size() != 0 && min_max.size() != 2) {
            otbAppLogFATAL("The min#max argument must have two parts");
        }

        printf("begin constant step interpolation (start=%g, step=%g, end=%g)\n", stepper[0], stepper[1], stepper[2]);
        if(lwScalar::isInteger<SCALAR_MC>())
            printf("nan: %d, ", (int)nan);
        else
            printf("nan: %g, ", nan);
        printf("max_dist: %g, no-masked values: ", max_dist);
        for(size_t i = 0 ; i < unmasked_values.size(); ++i)
            if(lwScalar::isInteger<SCALAR_MC>())
                printf("%d ", (int)unmasked_values[i]);
            else
                printf("%g ", unmasked_values[i]);
        printf("\n");
        printf("input times: %g ... %g\n", times.front(), times.back());
        if(min_max.size() == 2)
            printf("min: %g, max: %g\n", min_max[0], min_max[1]);

        maskF->SetInput(0, this->GetParameterImage<inputImageType>("in") );
        maskF->SetInput(1, this->GetParameterImage<inputImageType>("msk") );
        maskF->setMasks(unmasked_values, replacing_values);
        maskF->setInvertedMode(true);

        interpF->SetInput( maskF->GetOutput() );
        interpF->setDataTimes(times);
        interpF->setStepper(stepper[0], stepper[1], stepper[2]);
        interpF->setMaxDist(max_dist);
        interpF->setNaN( nan );
        if(min_max.size() == 2)
            interpF->setMinMax(min_max[0], min_max[1]);

        // modify the name if we have compression
        SetParameterString("out", GetOutFileName("out"));

        SetParameterOutputImage("out", interpF->GetOutput());
    }

    std::string GetOutFileName(const std::string &outParamName) {
        std::string ofname = GetParameterString(outParamName);
        std::ostringstream fileNameStream;
        fileNameStream << ofname;
        fileNameStream << "?&gdal:co:TILED=YES&gdal:co:INTERLEAVE=BAND&gdal:co:BIGTIFF=YES&gdal:co:COMPRESS=LZW";
        return fileNameStream.str();
    }

private:
    engeStepInterpolationFilter<inputImageType, outputImageType>::Pointer interpF;
    engeMaskSerieFilter<inputImageType, inputImageType, outputImageType>::Pointer maskF;
};

} // end of namespace Wrapper
} // end of namespace otb

OTB_APPLICATION_EXPORT(otb::Wrapper::ConstantStepInterpolationMasked)
