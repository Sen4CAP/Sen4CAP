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

#include "../../../Common/Filters/otbStreamingStatisticsMapFromLabelImageFilter.h"

#define MEAN_COL_NAME "mean"
#define STDEV_COL_NAME "stdev"
#define MIN_COL_NAME "min"
#define MAX_COL_NAME "max"
#define VALD_PIX_CNT_COL_NAME "valid_pixels_cnt"
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

class Markers1Extractor : public Application
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
    typedef Markers1Extractor        Self;
    typedef Application                   Superclass;
    typedef itk::SmartPointer<Self>       Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    /** Standard macro */
    itkNewMacro(Self);

    itkTypeMacro(Markers1Extractor, otb::Application);

    /** Filters typedef */
    typedef float                                           PixelType;
    typedef otb::Image<PixelType, 2>                        SimpleImageType;

    typedef FloatVectorImageType                            ImageType;
    typedef UInt8ImageType                                  MaskImageType;

    typedef otb::OGRDataToClassStatisticsFilter<ImageType,UInt8ImageType> FilterType;

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
        std::string mskImagePath;
        FloatVectorImageType::Pointer inputImage;
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
    Markers1Extractor()
    {
        m_fieldName = SEQ_UNIQUE_ID;
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
        SetName("Markers1Extractor");
        SetDescription("Markers 1 set extractor.");

        // Documentation
        SetDocName("Markers 1 set extractor");
        SetDocLongDescription("Markers 1 set extractor");
        SetDocLimitations("None");
        SetDocAuthors("OTB-Team");
        SetDocSeeAlso(" ");

        AddDocTag(Tags::Learning);

        AddParameter(ParameterType_StringList, "il", "Input Images or file containing the list");
        SetParameterDescription("il", "Support images that will be classified or a file containing these images");

        AddParameter(ParameterType_InputFilename, "vec", "Input vectors");
        SetParameterDescription("vec","Input geometries to analyze");
        MandatoryOff("vec");

        AddParameter(ParameterType_InputFilename, "filterids", "File containing ids to be filtered");
        SetParameterDescription("filterids","File containing ids to be filtered i.e. the monitorable parcels ids");
        MandatoryOff("filterids");

        AddParameter(ParameterType_String, "outdir", "Output directory for writing agricultural practices data extractin files");
        SetParameterDescription("outdir","Output directory to store agricultural practices data extractin files (txt format)");

        AddParameter(ParameterType_ListView, "field", "Field Name");
        SetParameterDescription("field","Name of the field carrying the class name in the input vectors.");
        //SetListViewSingleSelectionMode("field",true);

        AddParameter(ParameterType_Int, "layer", "Layer Index");
        SetParameterDescription("layer", "Layer index to read in the input vector file.");
        MandatoryOff("layer");
        SetDefaultParameterInt("layer",0);

        AddParameter(ParameterType_String, "prdtype", "Input product type (AMP/COHE/L3B/L2A)");
        SetParameterDescription("prdtype", "Input product type (AMP/COHE/L3B/L2A)");

        AddParameter(ParameterType_String,  "banddiscr",   "Band discriminator in case of multiple bands in the input raster");
        SetParameterDescription("banddiscr", "Band discriminator in case of multiple bands in the input raster");
        MandatoryOff("banddiscr");

        AddParameter(ParameterType_StringList,  "ilmsk",   "Input validity masks corresponding to the input images");
        SetParameterDescription("ilmsk", "Validity masks (only pixels corresponding to a mask value greater than 0 will be used for statistics)");
        MandatoryOff("ilmsk");

        AddParameter(ParameterType_Int,  "mskval",   "If mask are provided in ilmsk represents the value of valid mask in the masks list");
        SetParameterDescription("mskval", "If mask are provided in ilmsk represents the value of valid mask in the masks list");
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

        AddParameter(ParameterType_Int, "validity", "Extract the number of valid and invalid pixels for each parcel statistics.");
        SetParameterDescription("validity", "Extract the number of valid and invalid pixels for each parcel statistics");
        MandatoryOff("validity");
        SetDefaultParameterInt("validity",0);

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
        if ( HasValue("vec") )
        {
            std::string vectorFile = GetParameterString("vec");
            ogr::DataSource::Pointer ogrDS = ogr::DataSource::New(vectorFile, ogr::DataSource::Modes::Read);
            ogr::Layer layer = ogrDS->GetLayer(this->GetParameterInt("layer"));
            ogr::Feature feature = layer.ogr().GetNextFeature();

            ClearChoices("field");

            for(int iField=0; iField<feature.ogr().GetFieldCount(); iField++)
            {
                std::string key, item = feature.ogr().GetFieldDefnRef(iField)->GetNameRef();
                key = item;
                std::string::iterator end = std::remove_if(key.begin(),key.end(),IsNotAlphaNum);
                std::transform(key.begin(), end, key.begin(), tolower);

                OGRFieldType fieldType = feature.ogr().GetFieldDefnRef(iField)->GetType();

                if(fieldType == OFTString || fieldType == OFTInteger || fieldType == OFTReal /* || ogr::version_proxy::IsOFTInteger64(fieldType)*/)
                {
                    std::string tmpKey="field."+key.substr(0, end - key.begin());
                    AddChoice(tmpKey,item);
                }
            }
        }
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

        // Retrieve the field name
        if (HasValue("vec")) {
            const std::vector<int> &selectedCFieldIdx = GetSelectedItems("field");
            if(selectedCFieldIdx.empty())
            {
                otbAppLogWARNING(<<"No field has been selected for data labelling! Using " << SEQ_UNIQUE_ID);
            } else {
                const std::vector<std::string> &cFieldNames = GetChoiceNames("field");
                m_fieldName = cFieldNames[selectedCFieldIdx.front()];
                //m_bFieldNameIsInteger = false;
            }
        }

        // Load the file containing the parcel ids filters, if specified
        LoadMonitorableParcelIdsFilters();

        // Initializes the internal image infos
        InitializeInputImageInfos();

        const std::string &outDir = this->GetParameterString("outdir");
        int i = 1;
        // Reproject geometries
        const std::string &vectFile = this->GetParameterString("vec");
        if (vectFile.size() > 0) {
            otbAppLogINFO("Loading vectors from file " << vectFile);
            m_vectors = otb::ogr::DataSource::New(vectFile);
        }
        otb::ogr::DataSource::Pointer reprojVector = m_vectors;

        otbAppLogINFO("The outputs will be written to folder " << outDir);

        for (std::vector<InputFileInfoType>::const_iterator itInfos = m_InputFilesInfos.begin();
             itInfos != m_InputFilesInfos.end(); ++itInfos)
        {
            otbAppLogINFO("Handling file " << itInfos->inputImagePath);
            // create a new writer for this image
            Markers1CsvWriterType::Pointer writer = CreateWriter(*itInfos, outDir);

            HandleImageUsingShapefile(*itInfos, reprojVector, writer);

            // write the entries for this image
            otbAppLogINFO("Writing outputs to file " << writer->GetTargetFileName());
            writer->Update();
            otbAppLogINFO("Writing outputs to file done!");

            otbAppLogINFO("Processed " << i << " products. Remaining products = " << m_InputFilesInfos.size() - i <<
                          ". Percent completed = " << (int)((((double)i) / m_InputFilesInfos.size()) * 100) << "%");
            i++;
        }
    }

    void HandleImageUsingShapefile(const InputFileInfoType &infoImg,
                                   otb::ogr::DataSource::Pointer &reprojVector,
                                   Markers1CsvWriterType::Pointer writer) {
        if (NeedsReprojection(reprojVector, infoImg.inputImage)) {
            otbAppLogINFO("Reprojecting vectors needed for file " << infoImg.inputImagePath);
            reprojVector = GetVector(m_vectors, infoImg.inputImage);
        } else {
            otbAppLogINFO("No need to reproject vectors for " << infoImg.inputImagePath);
        }
        FilterType::Pointer filter = GetStatisticsFilter(infoImg, reprojVector, m_fieldName);
        const FilterType::PixeMeanStdDevlValueMapType &meanStdValues = filter->GetMeanStdDevValueMap();
        const FilterType::PixelValueMapType &minValues = filter->GetMinValueMap();
        const FilterType::PixelValueMapType &maxValues = filter->GetMaxValueMap();
        const FilterType::PixelValueMapType &medianValues = filter->GetMedianValuesMap();
        const FilterType::PixelValueMapType &p25Values = filter->GetP25ValuesMap();
        const FilterType::PixelValueMapType &p75Values = filter->GetP75ValuesMap();
        const FilterType::PixelValueMapType &validPixelsCntValues = filter->GetValidPixelsCntMap();
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

        writer->AddInputMap<FilterType::PixeMeanStdDevlValueMapType,
                 FilterType::PixelValueMapType>(mapMeanStd, mapOptionals);

//        writer->AddInputMap<FilterType::PixeMeanStdDevlValueMapType,
//                FilterType::PixelValueMapType>(meanStdValues, minValues, maxValues,
//                                               validPixelsCntValues);

        otbAppLogINFO("Extracted a number of " << meanStdValues.size() << " values for file " << infoImg.inputImagePath);

        filter->GetFilter()->Reset();
    }

    FilterType::Pointer GetStatisticsFilter(const InputFileInfoType &infoImg,
                                            const otb::ogr::DataSource::Pointer &reprojVector,
                                            const std::string &fieldName)
    {
        FilterType::Pointer filter = FilterType::New();
        if (m_bConvToDb) {
            filter->SetConvertValuesToDecibels(m_bConvToDb);
            filter->SetInput(infoImg.inputImage);
            // TODO: This can be switch as we get the same results
//            m_IntensityToDbFunctor = IntensityToDbFilterType::New();
//            m_IntensityToDbFunctor->SetInput(inputImg);
//            m_IntensityToDbFunctor->UpdateOutputInformation();
//            filter->SetInput(m_IntensityToDbFunctor->GetOutput());
        } else {
            filter->SetInput(infoImg.inputImage);
        }
        if(infoImg.mskImage.IsNotNull()) {
            filter->SetMask(infoImg.mskImage);
            filter->SetMaskValidValue(infoImg.mskImgValidityValue);
        }

        if (GetParameterInt("minmax") != 0) {
            filter->SetComputeMinMax(true);
        }
        if (GetParameterInt("validity") != 0) {
            filter->SetComputeValidityPixelsCnt(true);
        }
        if (GetParameterInt("median") != 0) {
            filter->SetComputeMedian(true);
        }
        if (GetParameterInt("p25") != 0) {
            filter->SetComputeP25(true);
        }
        if (GetParameterInt("p75") != 0) {
            filter->SetComputeP75(true);
        }

        filter->SetFieldValueFilterIds(m_FilterIdsMap);
        filter->SetOGRData(reprojVector);
        filter->SetFieldName(fieldName);
        filter->SetLayerIndex(this->GetParameterInt("layer"));
        filter->GetStreamer()->SetAutomaticAdaptativeStreaming(GetParameterInt("ram"));

        AddProcess(filter->GetStreamer(),"Analyze polygons...");
        filter->Update();

        return filter;
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

    Markers1CsvWriterType::Pointer CreateWriter(const InputFileInfoType &infoFile, const std::string &outDir) {
        Markers1CsvWriterType::Pointer agricPracticesDataWriter = Markers1CsvWriterType::New();
        agricPracticesDataWriter->SetTargetFileName(BuildUniqueFileName(outDir, infoFile.inputImagePath));
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
        if (GetParameterInt("validity") != 0) {
            header.push_back(VALD_PIX_CNT_COL_NAME);
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
//        if (HasValue("s2il")) {
//            const std::vector<std::string> &s2MsksPaths = this->GetParameterStringList("s2il");
//            if (HasValue("s2ilcnts")) {
//                const std::vector<std::string> &s2MsksCnts = this->GetParameterStringList("s2ilcnts");
//                if (imagesPaths.size() != s2MsksCnts.size()) {
//                    otbAppLogFATAL(<<"Invalid number of S2 masks given for the number of input images list! Expected: " <<
//                                   imagesPaths.size() << " but got " << s2MsksCnts.size());
//                }
//                // first perform a validation of the counts
//                size_t expectedS2Imgs = 0;
//                for (size_t i = 0; i<s2MsksCnts.size(); i++) {
//                    expectedS2Imgs += std::atoi(s2MsksCnts[i].c_str());
//                }
//                if (expectedS2Imgs != s2MsksPaths.size()) {
//                    otbAppLogFATAL(<<"The S2 masks number and the provided S2 files do not match! Expected " <<
//                                   expectedS2Imgs << " but got " << s2MsksPaths.size());
//                }
//                int curS2IlIdx = 0;
//                for (size_t i = 0; i<imagesPaths.size(); i++) {
//                    InputFileInfoType info;
//                    info.inputImage = imagesPaths[i];
//                    int curImgCnt = std::atoi(s2MsksCnts[i].c_str());
//                    for(int j = 0; j<curImgCnt; j++) {
//                        info.s2MsksFiles.emplace_back(s2MsksPaths[curS2IlIdx+j]);
//                    }
//                    m_InputFilesInfos.emplace_back(info);
//                    curS2IlIdx += curImgCnt;
//                }
//            } else {
//                otbAppLogWARNING("WARNING: Using all s2 mask files for each input image!!!");
//                for (const auto &inputImg: imagesPaths) {
//                    InputFileInfoType info;
//                    info.inputImage = inputImg;
//                    info.s2MsksFiles = s2MsksPaths;
//                    m_InputFilesInfos.emplace_back(info);
//                }
//            }
        int i = 0;
        const std::vector<std::string> &imagesPaths = this->GetParameterStringList("il");
        if(imagesPaths.size() == 0) {
            otbAppLogFATAL(<<"No image was given as input!");
        }

        std::vector<std::string> maskPaths;
        if (IsParameterEnabled("ilmsk") && HasValue("ilmsk"))
        {
            maskPaths = this->GetParameterStringList("ilmsk");
            if(maskPaths.size() != imagesPaths.size()) {
                otbAppLogFATAL(<<"Number of masks " << maskPaths.size() << " differ from the number of input rasters"
                               << imagesPaths.size() << " !!!");
            }
        }

        for (std::vector<std::string>::const_iterator itImages = imagesPaths.begin();
             itImages != imagesPaths.end(); ++itImages) {
            if ( !boost::filesystem::exists(*itImages) ) {
                otbAppLogWARNING("File " << *itImages << " does not exist on disk!");
                continue;
            }

            InputFileInfoType info;
            info.inputImagePath = (*itImages);
            info.inputImage = GetInputImage(info.inputImagePath);
            info.inputImage->UpdateOutputInformation();
            int imgRasterRes = info.inputImage->GetSpacing()[0];
            info.inputImgBandIdx = 0;
            if (info.inputImage->GetNumberOfComponentsPerPixel() > 1) {
                if (m_bandDiscr.size() == 0) {
                    otbAppLogWARNING("File " << info.inputImagePath << " has more than 1 bands and no band discrimination was given. It will be ignored!");
                    continue;
                }
                info.inputImgBandIdx = GetInputRasterBandIdx(info.inputImagePath, m_prdType, m_bandDiscr);
                if (info.inputImgBandIdx == -1) {
                    otbAppLogWARNING("S2 Band " << m_bandDiscr << "does not has any correspondence in file " << info.inputImagePath << ". Product will be ignored!");
                    continue;
                }
            }
            info.mskImgValidityValue = m_maskValidValue;
            if (maskPaths.size() > 0) {
                info.mskImagePath = maskPaths[i];
            }
            if (info.mskImagePath.size() > 0) {
                info.mskImage = GetMaskImage(info.mskImagePath);
            } else {
                // if no masks were provided from outside, we can try determine it from the input product type
                bool maskOk = false;
                MaskImageType::Pointer imgPtr = GetProductMaskImage(m_prdType, info.inputImagePath, imgRasterRes, info.mskImgValidityValue, maskOk);
                if (maskOk) {
                    info.mskImage = imgPtr;
                }
            }
            m_InputFilesInfos.emplace_back(info);
            i++;
        }
    }

    otb::ogr::DataSource::Pointer GetVector(const otb::ogr::DataSource::Pointer &vectors, const FloatVectorImageType::Pointer &inputImg) {
        const std::string &imageProjectionRef = inputImg->GetProjectionRef();
        FloatVectorImageType::ImageKeywordlistType imageKwl = inputImg->GetImageKeywordlist();
        const std::string &vectorProjectionRef = vectors->GetLayer(GetParameterInt("layer")).GetProjectionRef();

        const OGRSpatialReference imgOGRSref = OGRSpatialReference( imageProjectionRef.c_str() );
        const OGRSpatialReference vectorOGRSref = OGRSpatialReference( vectorProjectionRef.c_str() );
        bool doReproj = true;
        // don't reproject for these cases
        if (  vectorProjectionRef.empty() || imgOGRSref.IsSame( &vectorOGRSref )
            || ( imageProjectionRef.empty() && imageKwl.GetSize() == 0) ) {
            doReproj = false;
        }

        GeometriesType::Pointer inputGeomSet;
        GeometriesType::Pointer outputGeomSet;
        ProjectionFilterType::Pointer geometriesProjFilter;
        if (doReproj)
        {
            inputGeomSet = GeometriesType::New(vectors);
            otb::ogr::DataSource::Pointer reprojVector = otb::ogr::DataSource::New();
            outputGeomSet = GeometriesType::New(reprojVector);
            // Filter instantiation
            geometriesProjFilter = ProjectionFilterType::New();
            geometriesProjFilter->SetInput(inputGeomSet);
            if (imageProjectionRef.empty())
            {
                geometriesProjFilter->SetOutputKeywordList(inputImg->GetImageKeywordlist()); // nec qd capteur
            }
            geometriesProjFilter->SetOutputProjectionRef(imageProjectionRef);
            geometriesProjFilter->SetOutput(outputGeomSet);
            otbAppLogINFO("Reprojecting input vectors ...");
            geometriesProjFilter->Update();
            otbAppLogINFO("Reprojecting input vectors done!");
            return reprojVector;
        }

        // if no reprojection, return the original vectors
        return vectors;
    }

    bool NeedsReprojection(const otb::ogr::DataSource::Pointer &vectors, const FloatVectorImageType::Pointer &inputImg) {
        const std::string &imageProjectionRef = inputImg->GetProjectionRef();
        FloatVectorImageType::ImageKeywordlistType imageKwl = inputImg->GetImageKeywordlist();
        const std::string &vectorProjectionRef = vectors->GetLayer(GetParameterInt("layer")).GetProjectionRef();

        const OGRSpatialReference imgOGRSref = OGRSpatialReference( imageProjectionRef.c_str() );
        const OGRSpatialReference vectorOGRSref = OGRSpatialReference( vectorProjectionRef.c_str() );
        bool doReproj = true;
        // don't reproject for these cases
        if (  vectorProjectionRef.empty() || imgOGRSref.IsSame( &vectorOGRSref )
            || ( imageProjectionRef.empty() && imageKwl.GetSize() == 0) ) {
            doReproj = false;
        }

        return doReproj;
    }

    std::string BuildUniqueFileName(const std::string &targetDir, const std::string &refFileName) {
        bool bOutputCsv = true; // TODO : Here we can have an adapter for .ipc?
        boost::filesystem::path rootFolder(targetDir);
        boost::filesystem::path pRefFile(refFileName);
        std::string fileName = pRefFile.stem().string() + (bOutputCsv ? ".csv" : ".ipc");
        return (rootFolder / fileName).string();
    }

    void LoadMonitorableParcelIdsFilters() {
        // Reproject geometries
        if (HasValue("filterids")) {
            const std::string &filterIdsFile = this->GetParameterString("filterids");
            if (boost::filesystem::exists(filterIdsFile ))
            {
                otbAppLogINFO("Loading filter IDs from file " << filterIdsFile);
                // load the indexes
                std::ifstream idxFileStream(filterIdsFile);
                std::string line;
                int i = 0;
                while (std::getline(idxFileStream, line)) {
                    // ignore the first line which is the header line
                    if (i == 0) {
                        i++;
                        continue;
                    }
                    NormalizeFieldId(line);
                    m_FilterIdsMap[line] = std::atoi(line.c_str());
                }
                otbAppLogINFO("Loading filter IDs file done!");
            }
        }
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
        std::string m_fieldName;
        otb::ogr::DataSource::Pointer m_vectors;
        ConcatenateImagesFilterType::Pointer m_concatenateImagesFilter;
        //ReadersListType::Pointer m_Readers;
        ImageReaderType::Pointer m_ImageReader;
        MaskImageReaderType::Pointer m_maskReader;
        GenericMapContainer         m_GenericMapContainer;

        Markers1CsvWriterType::Pointer m_agricPracticesDataWriter;

        std::vector<InputFileInfoType> m_InputFilesInfos;
        IntensityToDbFilterType::Pointer        m_IntensityToDbFunctor;

        ImageResampler<FeatureImageType, FeatureImageType>  m_ImageResampler;

        typedef std::map<std::string, int> FilterIdsMapType;
        FilterIdsMapType m_FilterIdsMap;
        char m_csvWriterSeparator;
        std::map<Satellite, std::vector<BandsMappingType>> m_l2aBandMappings;

        std::vector<std::unique_ptr<MetadataHelper<float, uint8_t>>> m_metadataHelpers;

};

} // end of namespace Wrapper
} // end of namespace otb

OTB_APPLICATION_EXPORT(otb::Wrapper::Markers1Extractor)
