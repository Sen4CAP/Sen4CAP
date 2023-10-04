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

#include "otbOGRDataToClassStatisticsFilter.h"
#include "otbStatisticsXMLFileWriter.h"
#include "otbGeometriesProjectionFilter.h"
#include "otbGeometriesSet.h"
#include "otbWrapperElevationParametersHandler.h"
#include "otbConcatenateVectorImagesFilter.h"
#include "otbBandMathImageFilter.h"
#include "otbVectorImageToImageListFilter.h"
#include <boost/filesystem.hpp>
#include "otbMarkers1CsvWriter.h"
#include <boost/regex.hpp>

#include "ImageResampler.h"
#include "GenericRSImageResampler.h"
#include "MetadataHelperFactory.h"
//#include "../../Common/include/GSAAAttributesTablesReaderFactory.h"
//#include "DeclarationsInfo.h"

#include "otbMarkersFromLabelImageFilter.h"

#define MEAN_COL_NAME "mean"
#define STDEV_COL_NAME "stdev"
#define MIN_COL_NAME "min"
#define MAX_COL_NAME "max"
#define VALD_PIX_CNT_COL_NAME "valid_pixels_cnt"
#define INVALD_PIX_CNT_COL_NAME "invalid_pixels_cnt"
#define MEDIAN_COL_NAME "median"
#define P25_COL_NAME "p25"
#define P75_COL_NAME "p75"

namespace otb
{
namespace Wrapper
{

/** Utility function to negate std::isalnum */
bool IsNotAlphaNum(char c)
{
    return !std::isalnum(c);
}

class MdbFromLabelImage : public Application
{
    template< class TInput, class TOutput>
    class IntensityToDecibelsFunctor
    {
    public:
        IntensityToDecibelsFunctor() {}
        ~IntensityToDecibelsFunctor() {}

      bool operator!=( const IntensityToDecibelsFunctor &a) const
      {
        return false;
      }
      bool operator==( const IntensityToDecibelsFunctor & other ) const
      {
        return !(*this != other);
      }
      inline TOutput operator()( const TInput & A ) const
      {
          TOutput ret(A.GetSize());
          for (int i = 0; i<A.GetSize(); i++) {
              ret[i] = (10 * log10(A[i]));
          }
          return ret;
      }
    };

public:
    /** Standard class typedefs. */
    typedef MdbFromLabelImage        Self;
    typedef Application                   Superclass;
    typedef itk::SmartPointer<Self>       Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    /** Standard macro */
    itkNewMacro(Self);

    itkTypeMacro(MdbFromLabelImage, otb::Application);

    /** Filters typedef */
    typedef float                                           PixelType;
    typedef otb::Image<PixelType, 2>                        SimpleImageType;

    typedef FloatVectorImageType                            ImageType;
    typedef UInt8ImageType                                  MaskImageType;

    typedef otb::Markers1CsvWriter<ImageType::PixelType> Markers1CsvWriterType;

    typedef otb::GeometriesSet GeometriesType;

    typedef otb::GeometriesProjectionFilter ProjectionFilterType;

    typedef otb::ConcatenateVectorImagesFilter<ImageType>               ConcatenateImagesFilterType;
    typedef otb::ImageFileReader<ImageType>                             ImageReaderType;
    typedef otb::ObjectList<ImageReaderType>                            ReadersListType;

    typedef std::map<std::string , std::string>           GenericMapType;
    typedef std::map<std::string , GenericMapType>        GenericMapContainer;

    typedef FloatVectorImageType                                                     FeatureImageType;
    typedef otb::ImageFileReader<MaskImageType>                             MaskImageReaderType;

    typedef Int32ImageType                                                           ClassImageType;
    typedef otb::MarkersFromLabelImageFilter<FeatureImageType, ClassImageType, UInt8ImageType> FilterType;
    typedef otb::ImageFileReader<ClassImageType>                             ClassImageReaderType;

    typedef itk::UnaryFunctorImageFilter<FeatureImageType,FeatureImageType,
                    IntensityToDecibelsFunctor<
                        FeatureImageType::PixelType,
                        FeatureImageType::PixelType> > IntensityToDbFilterType;

    typedef otb::StreamingResampleImageFilter<MetadataHelper<float, uint8_t>::SingleBandMasksImageType,
                    MetadataHelper<float, uint8_t>::SingleBandMasksImageType, double>     ResampleFilterType;

    typedef struct {
        std::string prdType;
        bool bConvToDB; // convert to decibels
        int noDataValue;
    } PrdInfoType;

    typedef struct {
        std::string inputImagePath;
        std::string labelsImagePath;
        std::string mskImagePath;
        FloatVectorImageType::Pointer inputImage;
        ClassImageType::Pointer labelsImage;
        int inputImgBandIdx;
        MaskImageType::Pointer mskImage;
        int mskImgValidityValue;
    } InputFileInfoType;

    typedef struct {
        std::string s2BandName;
        std::string secondaryBandName;
        int secondaryBandIdx;
    } BandsMappingType;

private:
    MdbFromLabelImage()
    {
        m_bFieldNameIsInteger = true;

        m_concatenateImagesFilter = ConcatenateImagesFilterType::New();
        m_bConvToDb = false;
        m_noDataValue = 0;
        m_csvWriterSeparator = ',';
        m_prdTypeInfos = {
            {"L2A", false, NO_DATA_VALUE},
            {"L3B", false, NO_DATA_VALUE},
            {"AMP", true, 0},
            {"COHE", false, 0}
        };
        m_l2aBandMappings = {
            {
                Satellite::Landsat8,
                {
                    {"B02", "B2", 1},
                    {"B03", "B3", 2},
                    {"B04", "B4", 3},
                    {"B08", "B5", 4},
                    {"B8A", "B5", 1},
                    {"B10", "B6", 1},
                    {"B11", "B7", 1},
                    {"B12", "B8", 1}
                }
            }
        };
    }

    void DoInit() override
    {
        SetName("MdbFromLabelImage");
        SetDescription("Markers 1 set extractor.");

        // Documentation
        SetDocName("Markers 1 set extractor");
        SetDocLongDescription("Markers 1 set extractor");
        SetDocLimitations("None");
        SetDocAuthors("OTB-Team");
        SetDocSeeAlso(" ");

        AddDocTag(Tags::Learning);

        AddParameter(ParameterType_InputFilename, "img", "Input Image ");
        SetParameterDescription("img", "Input image");

        AddParameter(ParameterType_InputFilename, "imglbl", "Input labels image");
        SetParameterDescription("imglbl","Input labels image");

        AddParameter(ParameterType_InputFilename,  "msk",   "Input validity mask corresponding to the input image");
        SetParameterDescription("msk", "Validity mask (only pixels corresponding to a mask value equals with mskval will be used for statistics)");
        MandatoryOff("msk");

        AddParameter(ParameterType_String, "outfile", "Output file. If not provided, the outdir will be used and file name generated");
        SetParameterDescription("outfile","Output file. If not provided, the outdir will be used and file name generated");
        MandatoryOff("outfile");

        AddParameter(ParameterType_String, "outdir", "Output directory for writing agricultural practices data extractin files");
        SetParameterDescription("outdir","Output directory to store agricultural practices data extractin files");
        MandatoryOff("outdir");

        AddParameter(ParameterType_String, "prdtype", "Input product type (AMP/COHE/L3B/L2A)");
        SetParameterDescription("prdtype", "Input product type (AMP/COHE/L3B/L2A)");

        AddParameter(ParameterType_String,  "banddiscr",   "Band discriminator in case of multiple bands in the input raster");
        SetParameterDescription("banddiscr", "Band discriminator in case of multiple bands in the input raster");
        MandatoryOff("banddiscr");

        AddParameter(ParameterType_Int,  "mskval",   "If mask is provided in msk, represents the value of valid mask values");
        SetParameterDescription("mskval", "If mask is provided in msk, represents the value of valid mask values");
        MandatoryOff("mskval");
        SetDefaultParameterInt("mskval",0);

        AddParameter(ParameterType_Int, "stdev", "Extract the stdev statistics.");
        SetParameterDescription("stdev", "Extracts also the stdev for each parcel");
        MandatoryOff("stdev");
        SetDefaultParameterInt("stdev",1);

        AddParameter(ParameterType_Int, "minmax", "Extract the min and max statistics.");
        SetParameterDescription("minmax", "Extracts also the minimum and maximum for each parcel");
        MandatoryOff("minmax");
        SetDefaultParameterInt("minmax",0);

        AddParameter(ParameterType_Int, "validpixelscnt", "Extract the number of valid pixels for each parcel statistics.");
        SetParameterDescription("validpixelscnt", "Extract the number of valid pixels for each parcel statistics");
        MandatoryOff("validpixelscnt");
        SetDefaultParameterInt("validpixelscnt",0);

        AddParameter(ParameterType_Int, "invalidpixelscnt", "Extract the number of invalid pixels for each parcel statistics.");
        SetParameterDescription("invalidpixelscnt", "Extract the number of invalid pixels for each parcel statistics");
        MandatoryOff("invalidpixelscnt");
        SetDefaultParameterInt("invalidpixelscnt",0);

        AddParameter(ParameterType_Int, "median", "Extract the median for each parcel statistics.");
        SetParameterDescription("median", "Extract the median for each parcel statistics");
        MandatoryOff("median");
        SetDefaultParameterInt("median",0);

        AddParameter(ParameterType_Int, "p25", "Extract the P25 for each parcel statistics.");
        SetParameterDescription("p25", "Extract the P25 for each parcel statistics");
        MandatoryOff("p25");
        SetDefaultParameterInt("p25",0);

        AddParameter(ParameterType_Int, "p75", "Extract the P75 for each parcel statistics.");
        SetParameterDescription("p75", "Extract the P75 for each parcel statistics");
        MandatoryOff("p75");
        SetDefaultParameterInt("p75",0);

        AddParameter(ParameterType_Int, "sep", "Output CSV separator.");
        SetParameterDescription("sep", "Output CSV separator");
        MandatoryOff("sep");

        //ElevationParametersHandler::AddElevationParameters(this, "elev");

        AddRAMParameter();

        // Doc example parameter settings
        SetDocExampleParameterValue("in", "support_image.tif");
        SetDocExampleParameterValue("vec", "variousVectors.sqlite");
        SetDocExampleParameterValue("field", "label");
        SetDocExampleParameterValue("outdir","/path/to/output/");

        //SetOfficialDocLink();
    }

    void DoUpdateParameters() override
    {
    }

    void DoExecute() override
    {
        if (HasValue("sep")) {
            const std::string &sep = GetParameterAsString("sep");
            if (sep.length() > 0) {
                m_csvWriterSeparator = sep[0];
            }
        }

        m_maskValidValue = GetParameterInt("mskval");
        m_bandDiscr = GetParameterString("banddiscr");

        // TODO: This should be moved inside the loop and extracted dynamically the product type
        // (to not force outside app to determine it as we can do this here)
        // => See if (!GetFileInfosFromName(fileName, fileType, polarisation, orbit, fileDate, additionalFileDate))
        m_prdType = GetParameterAsString("prdtype");
        std::vector<PrdInfoType>::const_iterator it = std::find_if(m_prdTypeInfos.begin(), m_prdTypeInfos.end(),
            [&](const PrdInfoType& val){ return val.prdType == m_prdType; } );
        if (it != m_prdTypeInfos.end()) {
            m_bConvToDb = it->bConvToDB;
            m_noDataValue = it->noDataValue;
        }

        // Initializes the internal image infos
        InitializeInputImageInfos();

        std::string outFile = this->GetParameterString("outfile");
        if (outFile.size() == 0) {
            const std::string &outDir = this->GetParameterString("outdir");
            otbAppLogINFO("The outputs will be written to folder " << outDir);
            otbAppLogINFO("Handling file " << m_InputFilesInfo.inputImagePath);
            outFile = BuildUniqueFileName(outDir, m_InputFilesInfo.inputImagePath);
        }
        // create a new writer for this image
        Markers1CsvWriterType::Pointer writer = CreateWriter(m_InputFilesInfo, outFile);
        HandleImageUsingS2TilesParcelInfos(m_InputFilesInfo, writer);

        // write the entries for this image
        otbAppLogINFO("Writing outputs to file " << writer->GetTargetFileName());
        writer->Update();
        otbAppLogINFO("Writing outputs to file done!");
    }

    void HandleImageUsingS2TilesParcelInfos(const InputFileInfoType &imgInfos,
                                            Markers1CsvWriterType::Pointer writer) {
        FilterType::Pointer filter = GetStatisticsFilter(imgInfos);

        const FilterType::PixeMeanStdDevlValueMapType &meanStdValues = filter->GetMeanStdDevValueMap();
        const FilterType::PixelValueMapType &minValues = filter->GetMinValueMap();
        const FilterType::PixelValueMapType &maxValues = filter->GetMaxValueMap();
        const FilterType::PixelValueMapType &medianValues = filter->GetMedianValuesMap();
        const FilterType::PixelValueMapType &p25Values = filter->GetP25ValuesMap();
        const FilterType::PixelValueMapType &p75Values = filter->GetP75ValuesMap();
        const FilterType::PixelValueMapType &validPixelsCntValues = filter->GetValidPixelsCntMap();
        const FilterType::PixelValueMapType &invalidPixelsCntValues = filter->GetInvalidPixelsCntMap();
        std::map<std::string, const FilterType::PixeMeanStdDevlValueMapType*> mapMeanStd;
        std::map<std::string, const FilterType::PixelValueMapType*> mapOptionals;
        mapMeanStd[MEAN_COL_NAME] = &meanStdValues;
        mapMeanStd[STDEV_COL_NAME] = &meanStdValues;
        mapOptionals[MIN_COL_NAME] = &minValues;
        mapOptionals[MAX_COL_NAME] = &maxValues;
        mapOptionals[MEDIAN_COL_NAME] = &medianValues;
        mapOptionals[P25_COL_NAME] = &p25Values;
        mapOptionals[P75_COL_NAME] = &p75Values;
        mapOptionals[VALD_PIX_CNT_COL_NAME] = &validPixelsCntValues;
        mapOptionals[INVALD_PIX_CNT_COL_NAME] = &invalidPixelsCntValues;

        writer->AddInputMap<FilterType::PixeMeanStdDevlValueMapType,
                 FilterType::PixelValueMapType>(mapMeanStd, mapOptionals);
    }

    ClassImageType::Pointer GetClassImage(const std::string &imgPath) {
        ClassImageReaderType::Pointer imageReader = ClassImageReaderType::New();
        //m_Readers->PushBack(imageReader);
        m_classImageReader = imageReader;
        imageReader->SetFileName(imgPath);
        imageReader->UpdateOutputInformation();
        ClassImageType::Pointer retImg = imageReader->GetOutput();
        retImg->UpdateOutputInformation();
        return retImg;
    }

    FilterType::Pointer GetStatisticsFilter(const InputFileInfoType &imgInfos)
    {
        FilterType::Pointer filter = FilterType::New();
        FilterType::Pointer statisticsFilter = FilterType::New();
        // cut the input image according to the class image
        otbAppLogINFO("Extracting statistics for input image " << imgInfos.inputImagePath << " and class file " << imgInfos.labelsImagePath);
        const FloatVectorImageType::Pointer &cutInputImage = CutImage(imgInfos.inputImage, imgInfos.labelsImage);
        if (m_bConvToDb) {
            m_IntensityToDbFunctor = IntensityToDbFilterType::New();
            m_IntensityToDbFunctor->SetInput(cutInputImage);
            m_IntensityToDbFunctor->UpdateOutputInformation();
            statisticsFilter->SetInput(m_IntensityToDbFunctor->GetOutput());
        } else {
            statisticsFilter->SetInput(cutInputImage);
        }
        statisticsFilter->SetInputLabelImage(imgInfos.labelsImage);

        if(imgInfos.mskImage.IsNotNull()) {
            statisticsFilter->SetMaskInputImage(imgInfos.mskImage);
            statisticsFilter->SetMaskValidValue(imgInfos.mskImgValidityValue);
        }

        if (GetParameterInt("minmax") != 0) {
            statisticsFilter->SetComputeMinMax(true);
        }
        if (GetParameterInt("validpixelscnt") != 0) {
            statisticsFilter->SetComputeValidPixelsCnt(true);
        }
        if (GetParameterInt("invalidpixelscnt") != 0) {
            statisticsFilter->SetComputeInvalidPixelsCnt(true);
        }
        if (GetParameterInt("median") != 0) {
            statisticsFilter->SetComputeMedian(true);
        }
        if (GetParameterInt("p25") != 0) {
            statisticsFilter->SetComputeP25(true);
        }
        if (GetParameterInt("p75") != 0) {
            statisticsFilter->SetComputeP75(true);
        }

        AddProcess(statisticsFilter->GetStreamer(), "Computing features...");
        statisticsFilter->Update();

        return statisticsFilter;
    }

    FloatVectorImageType::Pointer GetInputImage(const std::string &imgPath) {
        ImageReaderType::Pointer imageReader = ImageReaderType::New();
        //m_Readers->PushBack(imageReader);
        m_ImageReader = imageReader;
        imageReader->SetFileName(imgPath);
        imageReader->UpdateOutputInformation();
        FloatVectorImageType::Pointer retImg = imageReader->GetOutput();
        retImg->UpdateOutputInformation();
        return retImg;
    }

    MaskImageType::Pointer GetMaskImage(const std::string &mskPath) {
        MaskImageReaderType::Pointer imageReader = MaskImageReaderType::New();
        m_maskReader = imageReader;
        imageReader->SetFileName(mskPath);
        imageReader->UpdateOutputInformation();
        MaskImageType::Pointer retImg = imageReader->GetOutput();
        retImg->UpdateOutputInformation();
        return retImg;
    }

    Markers1CsvWriterType::Pointer CreateWriter(const InputFileInfoType &infoFile, const std::string &outFile) {
        Markers1CsvWriterType::Pointer agricPracticesDataWriter = Markers1CsvWriterType::New();
        agricPracticesDataWriter->SetTargetFileName(outFile);
        std::vector<std::string> header = {SEQ_UNIQUE_ID, MEAN_COL_NAME};
        if ((GetParameterInt("stdev") != 0)) {
            header.push_back(STDEV_COL_NAME);
        }
        if (GetParameterInt("minmax") != 0) {
            header.push_back(MIN_COL_NAME);
            header.push_back(MAX_COL_NAME);
        }
        if (GetParameterInt("median") != 0) {
            header.push_back(MEDIAN_COL_NAME);
        }
        if (GetParameterInt("p25") != 0) {
            header.push_back(P25_COL_NAME);
        }
        if (GetParameterInt("p75") != 0) {
            header.push_back(P75_COL_NAME);
        }
        if (GetParameterInt("validpixelscnt") != 0) {
            header.push_back(VALD_PIX_CNT_COL_NAME);
        }
        if (GetParameterInt("invalidpixelscnt") != 0) {
            header.push_back(INVALD_PIX_CNT_COL_NAME);
        }

        agricPracticesDataWriter->SetDefaultProductType(m_prdType);
        agricPracticesDataWriter->SetCsvSeparator(m_csvWriterSeparator);
        agricPracticesDataWriter->SetHeaderFields(infoFile.inputImagePath, header, SEQ_UNIQUE_ID,
                m_bFieldNameIsInteger);
        if (infoFile.inputImgBandIdx >= 0) {
            agricPracticesDataWriter->SetMapValuesIndex(infoFile.inputImgBandIdx);
        }

        return agricPracticesDataWriter;
    }

    void InitializeInputImageInfos() {
        m_InputFilesInfo.inputImagePath = this->GetParameterString("img");
        m_InputFilesInfo.labelsImagePath = this->GetParameterString("imglbl");
        if (IsParameterEnabled("msk") && HasValue("msk"))
        {
            m_InputFilesInfo.mskImagePath = this->GetParameterString("msk");
        }
        if ( !boost::filesystem::exists(m_InputFilesInfo.inputImagePath) ) {
            otbAppLogFATAL("File " << m_InputFilesInfo.inputImagePath << " does not exist on disk!");
        }

        if ( !boost::filesystem::exists(m_InputFilesInfo.labelsImagePath) ) {
            otbAppLogFATAL("File " << m_InputFilesInfo.labelsImagePath << " does not exist on disk!");
        }
        if (m_InputFilesInfo.mskImagePath.size() > 0 && !boost::filesystem::exists(m_InputFilesInfo.mskImagePath)) {
            otbAppLogFATAL("File " << m_InputFilesInfo.mskImagePath << " does not exist on disk!");
        }


        m_InputFilesInfo.inputImage = GetInputImage(m_InputFilesInfo.inputImagePath);
        m_InputFilesInfo.labelsImage = GetClassImage(m_InputFilesInfo.labelsImagePath);
        m_InputFilesInfo.inputImage->UpdateOutputInformation();
        m_InputFilesInfo.labelsImage->UpdateOutputInformation();
        int imgRasterRes = m_InputFilesInfo.inputImage->GetSpacing()[0];
        m_InputFilesInfo.inputImgBandIdx = 0;
        if (m_InputFilesInfo.inputImage->GetNumberOfComponentsPerPixel() > 1) {
            if (m_bandDiscr.size() == 0) {
                otbAppLogFATAL("File " << m_InputFilesInfo.inputImagePath << " has more than 1 bands and no band discrimination was given. It will be ignored!");
            }
            m_InputFilesInfo.inputImgBandIdx = GetInputRasterBandIdx(m_InputFilesInfo.inputImagePath, m_prdType, m_bandDiscr);
            if (m_InputFilesInfo.inputImgBandIdx == -1) {
                otbAppLogFATAL("S2 Band " << m_bandDiscr << "does not has any correspondence in file " << m_InputFilesInfo.inputImagePath << ". Stopping now!");
            }
        }
        m_InputFilesInfo.mskImgValidityValue = m_maskValidValue;
        if (m_InputFilesInfo.mskImagePath.size() > 0) {
            m_InputFilesInfo.mskImage = GetMaskImage(m_InputFilesInfo.mskImagePath);
        } else {
            // if no masks were provided from outside, we can try determine it from the input product type
            bool maskOk = false;
            MaskImageType::Pointer imgPtr = GetProductMaskImage(m_prdType, m_InputFilesInfo.inputImagePath, imgRasterRes, m_InputFilesInfo.mskImgValidityValue, maskOk);
            if (maskOk) {
                m_InputFilesInfo.mskImage = imgPtr;
            }
        }
    }

    FeatureImageType::Pointer CutImage(const FeatureImageType::Pointer &img, const ClassImageType::Pointer &clsImg) {
        FeatureImageType::Pointer retImg = img;

        double clsImgWidth = clsImg->GetLargestPossibleRegion().GetSize()[0];
        double clsImgHeight = clsImg->GetLargestPossibleRegion().GetSize()[1];

        //ImageType::SpacingType spacing = reader->GetOutput()->GetSpacing();
        float clsImgRes = static_cast<float>(clsImg->GetSpacing()[0]);
        ImageType::PointType  clsImgOrigin;
        clsImgOrigin = clsImg->GetOrigin();

        std::string clsImgProjRef = clsImg->GetProjectionRef();

        float imageWidth = img->GetLargestPossibleRegion().GetSize()[0];
        float imageHeight = img->GetLargestPossibleRegion().GetSize()[1];

        ImageType::PointType origin = img->GetOrigin();
        ImageType::PointType imageOrigin;
        imageOrigin[0] = origin[0];
        imageOrigin[1] = origin[1];

        if((imageWidth != clsImgWidth) || (imageHeight != clsImgHeight) ||
                (clsImgOrigin[0] != imageOrigin[0]) || (clsImgOrigin[1] != imageOrigin[1])) {

            Interpolator_Type interpolator = Interpolator_Linear;
            std::string imgProjRef = img->GetProjectionRef();
            // if the projections are equal
            if(imgProjRef == clsImgProjRef) {
                float curImgRes = static_cast<float>(img->GetSpacing()[0]);
                const float scale = (float)clsImgRes / curImgRes;
                // use the streaming resampler
                m_ImageResampler.SetNoDataValue(m_noDataValue);
                retImg = m_ImageResampler.getResampler(img, scale,clsImgWidth,
                            clsImgHeight,clsImgOrigin, interpolator)->GetOutput();
            } else {
                // use the generic RS resampler that allows reprojecting
                m_genericRSImageResampler.SetNoDataValue(m_noDataValue);
                retImg = m_genericRSImageResampler.getResampler(img, clsImg, interpolator)->GetOutput();
            }
            retImg->UpdateOutputInformation();
        }

        return retImg;
    }


    std::string BuildUniqueFileName(const std::string &targetDir, const std::string &refFileName) {
        bool bOutputCsv = true; // TODO : Here we can have an adapter for .ipc?
        boost::filesystem::path rootFolder(targetDir);
        boost::filesystem::path pRefFile(refFileName);
        std::string fileName = pRefFile.stem().string() + (bOutputCsv ? ".csv" : ".ipc");
        return (rootFolder / fileName).string();
    }

    int GetInputRasterBandIdx(const std::string &rasterPath, const std::string &prdType, const std::string &bandDiscr) {
        if (prdType == "L2A") {
            boost::filesystem::path pRefFile(rasterPath);
            // check if starts with L8
            if(pRefFile.stem().string().find("L8") == 0) {
                return GetInputRasterBandIdx(Satellite::Landsat8, bandDiscr);
            } else {
                otbAppLogWARNING("Product with multiple bands not supported " << rasterPath);
            }
        }
        return 0;
    }
    int GetInputRasterBandIdx(const Satellite &sat, const std::string &bandDiscr) {
        std::map<Satellite, std::vector<BandsMappingType>>::const_iterator it = m_l2aBandMappings.find(sat);
        if (it != m_l2aBandMappings.end()) {
            for (const BandsMappingType &mapping: it->second) {
                if (mapping.s2BandName == bandDiscr) {
                    return mapping.secondaryBandIdx;
                }
            }
        }
        return -1;
    }

    MaskImageType::Pointer GetProductMaskImage(const std::string &prdType, const std::string &rasterPath, int imgRes, int &maskValidValue, bool &bOk) {
        boost::filesystem::path pRefFile(rasterPath);
        std::string fileName = pRefFile.filename().string();
        if (prdType == "L2A") {
            std::string mtdFile;
            if (boost::algorithm::ends_with(fileName, "_FRE.DBL.TIF")) {
                // we have an MACCS L8 format product - extract the HDR and get the mask from it
                mtdFile = GetFileFromDir(pRefFile.parent_path().parent_path().string(), R"(L8_.*_L8C_L2VALD_.*\.HDR)");
            } else if (boost::algorithm::contains(fileName, "_FRE_B")) {
                // We have a MAJA L2A product. Get the MTD_ALL.xml file from the same dir
                mtdFile = GetFileFromDir(pRefFile.parent_path().string(), R"(.*MTD_ALL.xml)");
            } else if (fileName.at(0) == 'T' && boost::algorithm::contains(fileName, "_B") && boost::algorithm::ends_with(fileName, ".jp2")) {
                // We have an Sen2Cor L2A product. Get the mtd xml file 2 levels above
                boost::filesystem::path prdRootDir = pRefFile.parent_path().parent_path().parent_path().parent_path().parent_path();
                mtdFile = GetFileFromDir(prdRootDir.string(), R"(MTD_MSIL2A\.xml)");
            }
            if (mtdFile.size() > 0) {
                auto factory = MetadataHelperFactory::New();
                m_metadataHelpers.push_back(factory->GetMetadataHelper<float, uint8_t>(mtdFile));
                MetadataHelper<float, uint8_t>::SingleBandMasksImageType::Pointer maskImg = m_metadataHelpers.at(m_metadataHelpers.size()-1)->GetMasksImage(ALL, false, imgRes);
                maskValidValue = IMG_FLG_LAND;
                maskImg->UpdateOutputInformation();
                bOk = true;
                return maskImg;
            }
        } else if (prdType == "L3B") {
            // Get the mask raster from the ../QI_DATA/
            const std::string &maskRaster = GetFileFromDir((pRefFile.parent_path().parent_path() / "QI_DATA").string(), R"(S2AGRI_L3B_MMONODFLG.*\.TIF)");
            maskValidValue = IMG_FLG_LAND;
            return GetMaskImage(maskRaster);
        }
        maskValidValue = m_maskValidValue;
        bOk = false;
        return MaskImageType::New();
    }

    std::string GetFileFromDir(const std::string &dir, const std::string &filePattern) {
        boost::filesystem::directory_iterator end_itr; // Default ctor yields past-the-end
        for( boost::filesystem::directory_iterator i( dir ); i != end_itr; ++i )
        {
            // Skip if not a file
            if (!boost::filesystem::is_regular_file( i->status() ) )  {
                continue;
            }
            if (filePattern.size() > 0) {
                boost::regex regexExp(filePattern);
                boost::smatch matches;
                if (boost::regex_match(i->path().filename().string(),matches,regexExp)) {
                    return i->path().string();
                }
            }
        }

        return "";
    }

    private:
        bool m_bFieldNameIsInteger;

        std::string m_prdType;
        std::string m_bandDiscr;
        std::vector<PrdInfoType> m_prdTypeInfos;
        bool m_bConvToDb;
        int m_noDataValue;
        int m_maskValidValue;
        otb::ogr::DataSource::Pointer m_vectors;
        ConcatenateImagesFilterType::Pointer m_concatenateImagesFilter;
        //ReadersListType::Pointer m_Readers;
        ImageReaderType::Pointer m_ImageReader;
        MaskImageReaderType::Pointer m_maskReader;
        GenericMapContainer         m_GenericMapContainer;

        Markers1CsvWriterType::Pointer m_agricPracticesDataWriter;

        InputFileInfoType m_InputFilesInfo;
        IntensityToDbFilterType::Pointer        m_IntensityToDbFunctor;

        ImageResampler<FeatureImageType, FeatureImageType>  m_ImageResampler;
        ClassImageReaderType::Pointer m_classImageReader;

        char m_csvWriterSeparator;
        std::map<Satellite, std::vector<BandsMappingType>> m_l2aBandMappings;

        std::vector<std::unique_ptr<MetadataHelper<float, uint8_t>>> m_metadataHelpers;

        GenericRSImageResampler<FeatureImageType, FeatureImageType, ClassImageType>  m_genericRSImageResampler;

};

} // end of namespace Wrapper
} // end of namespace otb

OTB_APPLICATION_EXPORT(otb::Wrapper::MdbFromLabelImage)
