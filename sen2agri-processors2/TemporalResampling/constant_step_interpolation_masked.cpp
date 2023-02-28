#include <cstdlib> /* atoi */
#include <limits>
#include <sstream>
#include <stdio.h>  /* printf, fopen */
#include <stdlib.h> /* exit, EXIT_FAILURE */
#include <string>

// #include "lwTime.h"
#include "engeMaskSerieFilter.h"
#include "engeStepInterpolationFilter.h"
// #include "lwScalar.h"
#include "otbImage.h"
#include "otbImageFileReader.h"
#include "otbImageFileWriter.h"
#include "otbVectorImage.h"
#include <cstdlib> /* putenv */

#ifndef SCALAR_MC
#define SCALAR_MC short
#endif

template <typename T>
T lexical_cast(const std::string &str)
{
    T var;
    std::istringstream iss;
    iss.str(str);
    iss >> var;
    // deal with any error bits that may have been set on the stream
    return var;
}

template <typename T>
std::vector<T> splitNum(const std::string &s, char delim)
{
    std::stringstream ss(s);
    std::string item;
    std::vector<T> res;
    while (std::getline(ss, item, delim)) {
        res.emplace_back(lexical_cast<T>(item));
    }
    return res;
}

int main(int argc, char *argv[])
{
    putenv((char *) "GDAL_CACHEMAX=2000");

    // input arguments
    if (argc < 7) {
        printf("Min six args are mandatory: input file (multiband), mask file (multiband), new "
               "output file (multiband),\n"
               "                            data times corresponding to input bands (floats; eg. "
               "0.1#0.6#3#18),\n"
               "                            data times corresponding to output bands (floats; eg. "
               "0#10#20#30),\n"
               "                            no-masked values: val1#val2#val3\n");
        printf("optional args: output nan value\n"
               "               maximum distance (default: 0 = no max): if a hole length is > max, "
               "it remains a hole\n"
               "               window radius (default: 0 = none): only look for valid pixels in "
               "that radius \n");
        exit(EXIT_FAILURE);
    }
    std::string inputFilename = argv[1];
    std::string maskFilename = argv[2];
    std::string outputFilename = argv[3];
    // sprintf(
    //     outputFilename,
    //     "%s"
    //     "%s?&gdal:co:TILED=YES&gdal:co:INTERLEAVE=BAND&gdal:co:BIGTIFF=YES&gdal:co:COMPRESS=LZW",
    //     argv[3]);
    std::vector<double> input_times = splitNum<double>(std::string(argv[4]), '#');
    std::vector<double> output_times = splitNum<double>(std::string(argv[5]), '#');
    std::vector<SCALAR_MC> unmasked_values = splitNum<SCALAR_MC>(std::string(argv[6]), '#');

    // SCALAR_MC nan = lwScalar::getNaN<SCALAR_MC>();
    auto nan = std::numeric_limits<SCALAR_MC>::quiet_NaN();
    double max_dist = 0;
    double window_radius = 0;
    if (argc > 7)
        nan = static_cast<SCALAR_MC>(atof(argv[7]));
    if (argc > 8) {
        max_dist = atof(argv[8]);
        printf("Maximum distance: %f\n", max_dist);
    }
    if (argc > 9) {
        window_radius = atof(argv[9]);
        printf("Window radius: %f\n", window_radius);
    }
    int verbose = 2;

    std::vector<SCALAR_MC> replacing_values(unmasked_values.size());
    for (int i = 0; i < replacing_values.size(); ++i) {
        replacing_values[i] = nan;
    }

    if (max_dist < 0) {
        printf("The max_dist argument must have three parts\n");
        exit(EXIT_FAILURE);
    }

    printf("begin constant step interpolation\n");
    // if (lwScalar::isInteger<SCALAR_MC>())
    //     printf("nan: %d, ", nan);
    // else
    // printf("nan: %g, ", nan);
    // printf("max_dist: %g, no-masked values: ", max_dist);
    // for (size_t i = 0; i < unmasked_values.size(); ++i)
    //     if (lwScalar::isInteger<SCALAR_MC>())
    //         printf("%d ", unmasked_values[i]);
    //     else
    //     printf("%g ", unmasked_values[i]);
    printf("\n");
    printf("input times: %g ... %g\n", input_times.front(), input_times.back());
    printf("output times: %g ... %g\n", output_times.front(), output_times.back());

    // double timeBegin = lwTime::get();

    //  itk::MultiThreader::SetGlobalMaximumNumberOfThreads(6);

    // define the pipeline
    typedef otb::VectorImage<SCALAR_MC, 2> inputImageType;
    typedef otb::VectorImage<SCALAR_MC, 2> outputImageType;
    otb::ImageFileReader<inputImageType>::Pointer readerF =
        otb::ImageFileReader<inputImageType>::New();
    otb::ImageFileReader<inputImageType>::Pointer readerMaskF =
        otb::ImageFileReader<inputImageType>::New();
    otb::ImageFileWriter<outputImageType>::Pointer writerF =
        otb::ImageFileWriter<outputImageType>::New();
    engeStepInterpolationFilter<inputImageType, outputImageType>::Pointer interpF =
        engeStepInterpolationFilter<inputImageType, outputImageType>::New();
    engeMaskSerieFilter<inputImageType, inputImageType, outputImageType>::Pointer maskF =
        engeMaskSerieFilter<inputImageType, inputImageType, outputImageType>::New();

    readerF->SetFileName(inputFilename);
    if (!maskFilename.empty()) {
        printf("Using mask\n");
        readerMaskF->SetFileName(maskFilename);

        maskF->SetInput(0, readerF->GetOutput());
        maskF->SetInput(1, readerMaskF->GetOutput());
        maskF->setMasks(unmasked_values, replacing_values);
        maskF->setInvertedMode(true);

        interpF->SetInput(maskF->GetOutput());
    } else {
        printf("No mask\n");
        interpF->SetInput(readerF->GetOutput());
    }

    interpF->setInputTimes(input_times);
    interpF->setOutputTimes(output_times);
    interpF->setMaxDist(max_dist);
    interpF->setNaN(nan);

    writerF->SetInput(interpF->GetOutput());
    writerF->SetFileName(outputFilename);

    // execute the pipeline
    writerF->Update();

    // double totalTime = lwTime::get() - timeBegin; -

    // if(verbose > 1)
    //   printf("\n\nThe end !   Time elapsed = %s\n\n", lwTime::toString(totalTime).c_str() );
    return 0;
}
