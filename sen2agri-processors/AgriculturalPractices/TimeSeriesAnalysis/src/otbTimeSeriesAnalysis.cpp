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
#include <boost/filesystem.hpp>

#include "otbOGRDataSourceWrapper.h"

#include <boost/regex.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/fusion/algorithm/iteration/for_each.hpp>
#include <boost/accumulators/statistics.hpp>
#include <boost/accumulators/statistics/variance.hpp>

#include "TimeSeriesAnalysisTypes.h"
#include "TimeSeriesAnalysisUtils.h"
#include "StatisticsInfosReaderFactory.h"
#include "../../Common/include/PracticeReaderFactory.h"

#include "TsaPlotsWriter.h"
#include "TsaCSVWriter.h"
#include "TsaContinuousFileWriter.h"
#include "TsaDebugPrinter.h"
#include "TsaPrevPrdReader.h"
#include "TsaHelperFunctions.h"

#include "TsaDataExtractor.h"
#include "TsaDataPreProcessor.h"

#include "TsaHarvestOnlyAnalysisHandler.h"
#include "TsaCatchCropAnalysisHandler.h"
#include "TsaFallowAnalysisHandler.h"
#include "TsaNfcAnalysisHandler.h"
#include "TsaTillageAnalysisHandler.h"

#define CATCH_CROP_VAL                  "CatchCrop"
#define CATCH_CROP_IS_MAIN_VAL          "CatchCropIsMain"
#define FALLOW_LAND_VAL                 "Fallow"
#define NITROGEN_FIXING_CROP_VAL        "NFC"

#define MIN_REQUIRED_COHE_VALUES        15      // previous was 26

// TODO : re-analyse the usage of dates (start of week or exact date)

template <typename T>
bool InputFileLineInfoEqComparator (const InputFileLineInfoType& struct1, const InputFileLineInfoType& struct2) {
  return (struct1.weekNo == struct2.weekNo);
}

namespace otb
{
namespace Wrapper
{
class TimeSeriesAnalysis : public Application
{
public:
    /** Standard class typedefs. */
    typedef TimeSeriesAnalysis        Self;
    typedef Application                   Superclass;
    typedef itk::SmartPointer<Self>       Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    /** Standard macro */
    itkNewMacro(Self);

    itkTypeMacro(TimeSeriesAnalysis, otb::Application);

    /** Filters typedef */

private:
    TimeSeriesAnalysis() : m_tsaHarvestOnlyHandler(GetLogger()),
        m_tsaDataExtractor(GetLogger()), m_tsaDataExtrPreProc(GetLogger())
    {
        m_countryName = "UNKNOWN";
        m_practiceName = NA_STR;
        m_year = "UNKNOWN";
        m_nMinS1PixCnt = 8;

        m_CatchCropVal = CATCH_CROP_VAL;
        m_FallowLandVal = FALLOW_LAND_VAL;
        m_NitrogenFixingCropVal = NITROGEN_FIXING_CROP_VAL;

        m_bVerbose = false;
        m_tsaDataExtractor.SetVerbose(m_bVerbose);
        m_tsaDataExtrPreProc.SetVerbose(m_bVerbose);

        time(&m_ttLimitAcqDate);
        m_ttLimitAcqDate -= (SEC_IN_WEEK * 2);

        m_bMonitorTillage = false;
    }

    void DoInit() override
    {
        SetName("TimeSeriesAnalysis");
        SetDescription("TODO.");

        // Documentation
        SetDocName("TODO");
        SetDocLongDescription("TODO");
        SetDocLimitations("None");
        SetDocAuthors("CIU");
        SetDocSeeAlso(" ");

        AddDocTag(Tags::Learning);

        AddParameter(ParameterType_String, "dircohe", "Coherence statistics files directory");
        SetParameterDescription("dircohe", "Coherence statistics files directory");

        AddParameter(ParameterType_String, "diramp", "Amplitude statistics files directory");
        SetParameterDescription("diramp", "Amplitude statistics files directory");

        AddParameter(ParameterType_String, "dirndvi", "NDVI statistics files directory");
        SetParameterDescription("dirndvi", "NDVI statistics files directory");

        AddParameter(ParameterType_String, "harvestshp", "Harvest information shapefile");
        SetParameterDescription("harvestshp", "Harvest information shapefile");

        AddParameter(ParameterType_String, "outdir", "Output folder directory");
        SetParameterDescription("outdir", "Output folder directory");

        AddParameter(ParameterType_String, "intype", "Type of the inputs");
        SetParameterDescription("intype", "If xml then an xml file is expected as input. "
                                          "If dir, the application expects a directory with txt files for each parcel");
        MandatoryOff("intype");

        AddParameter(ParameterType_Int, "allowgaps", "Allow week gaps in time series");
        SetParameterDescription("allowgaps", "Allow week gaps in  time series");
        SetDefaultParameterInt("allowgaps", 1);
        MandatoryOff("allowgaps");

        AddParameter(ParameterType_String, "country", "The country to be used in the output file name");
        SetParameterDescription("country", "The country to be used in the output file name");
        MandatoryOff("country");

        AddParameter(ParameterType_String, "practice", "The practice to be used in the output file name");
        SetParameterDescription("practice", "The practice to be used in the output file name");
        MandatoryOff("practice");

        AddParameter(ParameterType_String, "year", "The year to be used in the output file name");
        SetParameterDescription("practice", "The year to be used in the output file name");
        MandatoryOff("year");

        AddParameter(ParameterType_Int, "tillage", "Specifies if tillage should be monitored");
        SetParameterDescription("tillage", "Specifies if tillage should be monitored. Boolean : 0 - do not monitor, 1 - monitor tillage");
        SetDefaultParameterInt("tillage", 0);
        MandatoryOff("tillage");


// /////////////////////// CONFIGURABLE PARAMETERS PER SITE AND PRACTICE ////////////////////
        AddParameter(ParameterType_Float, "optthrvegcycle", "Optical threshold vegetation cycle");
        SetParameterDescription("optthrvegcycle", "Optical threshold vegetation cycle");

        // for MARKER 2 - NDVI loss
        // expected value of harvest/clearance
        AddParameter(ParameterType_Float, "ndvidw", "NDVI down");
        SetParameterDescription("ndvidw", "for MARKER 2 - NDVI loss - expected value of harvest/clearance");

        AddParameter(ParameterType_Float, "ndviup", "NDVI up");
        SetParameterDescription("ndviup", "buffer value (helps in case of sparse ndvi time-serie)");

        AddParameter(ParameterType_Float, "ndvistep", "NDVI step");
        SetParameterDescription("ndvistep", "opt.thr.value is round up to ndvi.step");

        AddParameter(ParameterType_Float, "optthrmin", "Optical threshold minimum");
        SetParameterDescription("optthrmin", "opt.thr.value is round up to ndvi.step");

        AddParameter(ParameterType_Float, "cohthrbase", "COHERENCE increase");
        SetParameterDescription("cohthrbase", "for MARKER 5 - COHERENCE increase");

        AddParameter(ParameterType_Float, "cohthrhigh", "Coherence threshold high");
        SetParameterDescription("cohthrhigh", "for MARKER 5 - COHERENCE increase");

        AddParameter(ParameterType_Float, "cohthrabs", "Coherence threshold absolute");
        SetParameterDescription("cohthrabs", "for MARKER 5 - COHERENCE increase");

        AddParameter(ParameterType_Float, "tlcohthrbase", "Tillage COHERENCE increase");
        SetParameterDescription("tlcohthrbase", "Used in Marker 5 as the Basic increase in coherence threshold");
        MandatoryOff("tlcohthrbase");
        SetDefaultParameterFloat("tlcohthrbase", 0.05);

        AddParameter(ParameterType_Float, "tlcohthrabs", "Tillage Coherence threshold absolute");
        SetParameterDescription("tlcohthrabs", "Used in Marker 5 as the Absolute coherence threshold");
        MandatoryOff("tlcohthrabs");
        SetDefaultParameterFloat("tlcohthrabs", 0.75);

        AddParameter(ParameterType_Float, "ampthrmin", "BACKSCATTER loss");
        SetParameterDescription("ampthrmin", "for MARKER 3 - BACKSCATTER loss");

        // INPUT THRESHOLDS - EFA PRACTICE evaluation
        AddParameter(ParameterType_String, "catchmain", "Catch main");
        SetParameterDescription("catchmain", "TODO");
        MandatoryOff("catchmain");

        AddParameter(ParameterType_String, "catchcropismain", "Catch crop is main");
        SetParameterDescription("catchcropismain", "TODO");
        MandatoryOff("catchcropismain");

        AddParameter(ParameterType_Int, "catchperiod", "Catch period");
        SetParameterDescription("catchperiod", "in days (e.g. 8 weeks == 56 days)");
        SetDefaultParameterInt("catchperiod", 56);
        MandatoryOff("catchperiod");

        AddParameter(ParameterType_Float, "catchproportion", "Catch proportion");
        SetParameterDescription("catchproportion", "buffer threshold");
        SetDefaultParameterFloat("catchproportion", 1./3);
        MandatoryOff("catchproportion");

        AddParameter(ParameterType_String, "catchperiodstart", "Catch period start");
        SetParameterDescription("catchperiodstart", "Catch period start");
        MandatoryOff("catchperiodstart");

        AddParameter(ParameterType_Int, "efandvithr", "Efa NDVI threshold");
        SetParameterDescription("efandvithr", "EFA practices NDVI threshold");
        SetDefaultParameterInt("efandvithr", 325);                // TODO: changing
        MandatoryOff("efandvithr");

        AddParameter(ParameterType_Int, "efandviup", "Efa NDVI up");
        SetParameterDescription("efandviup", "EFA practices NDVI up");
        SetDefaultParameterInt("efandviup", 400);                // TODO: changing
        MandatoryOff("efandviup");

        AddParameter(ParameterType_Int, "efandvidw", "Efa NDVI down");
        SetParameterDescription("efandvidw", "EFA practices NDVI down");
        SetDefaultParameterInt("efandvidw", 300);                // TODO: changing
        MandatoryOff("efandvidw");

        AddParameter(ParameterType_Float, "efacohchange", "Efa Coherence change");
        SetParameterDescription("efacohchange", "EFA practices coherence change");
        SetDefaultParameterInt("efacohchange", 0.2);
        MandatoryOff("efacohchange");

        AddParameter(ParameterType_Float, "efacohvalue", "Efa Coherence value");
        SetParameterDescription("efacohvalue", "EFA practices coherence value");
        SetDefaultParameterInt("efacohvalue", 0.7);
        MandatoryOff("efacohvalue");

        AddParameter(ParameterType_Float, "efandvimin", "Efa NDVI minimum");
        SetParameterDescription("efandvimin", "EFA practices NDVI minimum");
        SetDefaultParameterInt("efandvimin", NOT_AVAILABLE);
        MandatoryOff("efandvimin");

        AddParameter(ParameterType_Float, "efaampthr", "Efa Amplitude threshold");
        SetParameterDescription("efaampthr", "EFA practices amplitude threshold");
        SetDefaultParameterInt("efaampthr", NOT_AVAILABLE);
        MandatoryOff("efaampthr");

        AddParameter(ParameterType_Int, "stddevinampthr", "Use Standard deviation in amplitude threshold value computation");
        SetParameterDescription("stddevinampthr", "Use Standard deviation in amplitude threshold value computation");
        SetDefaultParameterInt("stddevinampthr", 0);
        MandatoryOff("stddevinampthr");

        AddParameter(ParameterType_Int, "optthrbufden", "Optical threshold buffer denomimator");
        SetParameterDescription("optthrbufden", "Optical threshold buffer denomimator");
        SetDefaultParameterInt("optthrbufden", 6);
        MandatoryOff("optthrbufden");

        AddParameter(ParameterType_Int, "ampthrbreakden", "Amplitude threshold break denomimator");
        SetParameterDescription("ampthrbreakden", "Amplitude threshold break denomimator");
        SetDefaultParameterInt("ampthrbreakden", 6);
        MandatoryOff("ampthrbreakden");

        AddParameter(ParameterType_Int, "ampthrvalden", "Amplitude threshold value denomimator");
        SetParameterDescription("ampthrvalden", "Amplitude threshold value denomimator");
        SetDefaultParameterInt("ampthrvalden", 2);
        MandatoryOff("ampthrvalden");

        AddParameter(ParameterType_String, "flmarkstartdate", "Fallow land markers start date");
        SetParameterDescription("flmarkstartdate", "Fallow land markers start date");
        MandatoryOff("flmarkstartdate");

        AddParameter(ParameterType_String, "flmarkstenddate", "Fallow land markers end date");
        SetParameterDescription("flmarkstenddate", "Fallow land markers end date");
        MandatoryOff("flmarkstenddate");

        AddParameter(ParameterType_Int, "s1pixthr", "Number of minimum S1 pixels to consider a parcel valid");
        SetParameterDescription("s1pixthr", "Number of minimum S1 pixels to consider a parcel valid");
        MandatoryOff("s1pixthr");
        SetDefaultParameterInt("s1pixthr", 1);


        AddParameter(ParameterType_String, "catchcropval", "Catch crop value");
        SetParameterDescription("catchcropval", "Catch crop value");
        MandatoryOff("catchcropval");

        AddParameter(ParameterType_String, "flval", "Fallow land value");
        SetParameterDescription("flval", "Fallow land value");
        MandatoryOff("flval");

        AddParameter(ParameterType_String, "ncval", "Nitrogen fixing crop value");
        SetParameterDescription("ncval", "Nitrogen fixing crop value");
        MandatoryOff("ncval");

        AddParameter(ParameterType_Int, "plotgraph", "Plot output graphs");
        SetParameterDescription("plotgraph", "In case graphs shall be generated set the value to TRUE otherwise set to FALSE");
        SetDefaultParameterInt("plotgraph", 0);
        MandatoryOff("plotgraph");

        AddParameter(ParameterType_Int, "rescontprd", "Results continuous products");
        SetParameterDescription("rescontprd", "In case continuos products (csv file) shall be generated set the value to TRUE otherwise set to FALSE");
        SetDefaultParameterInt("rescontprd", 0);
        MandatoryOff("rescontprd");

        AddParameter(ParameterType_Int, "debug", "Print debug messages");
        SetParameterDescription("debug", "Print debug messages");
        SetDefaultParameterInt("debug", 1);
        MandatoryOff("debug");

        AddParameter(ParameterType_String, "prevprd", "The previous product");
        SetParameterDescription("prevprd", "The previous product of this practice (if any) to be used for extracting "
                                           "the H_WEEK that needs to be preserved");
        MandatoryOff("prevprd");

        AddParameter(ParameterType_Int, "minacqs", "The minimum numner of coherence acquision dates in the time series");
        SetParameterDescription("minacqs", "The minimum numner of coherence acquision dates in the time series");
        SetDefaultParameterInt("minacqs", MIN_REQUIRED_COHE_VALUES);
        MandatoryOff("minacqs");

        AddParameter(ParameterType_String, "acqsdatelimit", "Limit acquisition date");
        SetParameterDescription("acqsdatelimit", "Limit acquisition date");
        MandatoryOff("acqsdatelimit");

        // Doc example parameter settings
        //SetDocExampleParameterValue("in", "support_image.tif");
    }

    void DoUpdateParameters() override
    {
    }

    void ExtractParameters() {
        m_outputDir = trim(GetParameterAsString("outdir"));
        m_tsaDataExtractor.SetAllowGaps(GetParameterInt("allowgaps") != 0);
        m_debugPrinter.SetDebugMode(GetParameterInt("debug") != 0);
        m_nMinS1PixCnt = GetParameterInt("s1pixthr");
        m_bMonitorTillage = GetParameterInt("tillage") != 0;

        if (HasValue("country")) {
            m_countryName = trim(GetParameterAsString("country"));
        }
        if (HasValue("practice")) {
            m_practiceName = trim(GetParameterAsString("practice"));
        }
        if (HasValue("year")) {
            m_year = trim(GetParameterAsString("year"));
        }
        if (HasValue("catchcropval")) {
            m_CatchCropVal = GetParameterString("catchcropval");
        }
        if (HasValue("flval")) {
            m_FallowLandVal = GetParameterString("flval");
        }
        if (HasValue("ncval")) {
            m_NitrogenFixingCropVal = GetParameterString("ncval");
        }

//  ///////////////////////////////////////////////////////////////////
        if (HasValue("plotgraph")) {
            m_plotsWriter.SetEnabled(GetParameterInt("plotgraph") != 0);
        }
        if (HasValue("rescontprd")) {
            m_contFileWriter.SetEnabled(GetParameterInt("rescontprd") != 0);
        }

        if (HasValue("prevprd")) {
            const std::string &prevFileName = TsaCSVWriter::BuildResultsCsvFileName(m_practiceName,
                                                                               m_countryName,
                                                                               std::atoi(m_year.c_str()));
            m_tsaHarvestOnlyHandler.SetPrevPracticeFileName(GetParameterString("prevprd"), prevFileName);
        }

        if (HasValue("acqsdatelimit")) {
            const std::string &limitDateStr = GetParameterString("acqsdatelimit");
            m_ttLimitAcqDate = GetTimeFromString(limitDateStr);
        }
        time_t yearLimitDate = GetTimeFromString(m_year + "-12-31");
        if (m_ttLimitAcqDate > yearLimitDate) {
            m_ttLimitAcqDate = yearLimitDate;
        }

        // Initialize handler parameters
        InitializeHarvestEvaluationHandler();

        // Initialize Efa handler, if needed
        InitializeEfaHandler();

        // Initialize tillage handler, if needed
        InitializeTillageHandler();
    }
    void DoExecute() override
    {
        // Extract the parameters
        ExtractParameters();

        std::string inputType = "csv";
        if (HasValue("intype")) {
            const std::string &inType = GetParameterAsString("intype");
            if (inType == "dir" || inType == "xml" || inType == "csv" || inType == "mcsv") {
                inputType = inType;
            } else {
                itkExceptionMacro("Invalid value provided for parameter intype " << inType
                                  << ". Only dir or xml or csv are supported (default: csv)");
            }
        }

        int curYear = std::atoi(m_year.c_str());
        m_tsaDataExtractor.Initialize(GetParameterAsString("diramp"), GetParameterAsString("dircohe"),
                                      GetParameterAsString("dirndvi"), GetParameterInt("minacqs"),
                                      curYear, inputType);

        const std::string &practicesInfoFile = GetParameterAsString("harvestshp");
        boost::filesystem::path practicesInfoPath(practicesInfoFile);
        std::string pfFormat = practicesInfoPath.extension().c_str();
        pfFormat.erase(pfFormat.begin(), std::find_if(pfFormat.begin(), pfFormat.end(), [](int ch) {
                return ch != '.';
            }));

        auto practiceReadersFactory = PracticeReaderFactory::New();
        m_pPracticeReader = practiceReadersFactory->GetPracticeReader(pfFormat);
        m_pPracticeReader->SetSource(practicesInfoFile);

        // write first the CSV header
        if (!m_csvWriter.WriteCSVHeader(m_outputDir, m_practiceName, m_countryName, curYear, m_bMonitorTillage)) {
            otbAppLogFATAL("Error opening output file for practice " << m_practiceName << " and year "
                           << curYear << " in the directory " << m_outputDir << ". Exiting...");
        }

        // create the plots file
        m_plotsWriter.CreatePlotsFile(m_outputDir, m_practiceName, m_countryName, curYear);

        // create the continous products file
        m_contFileWriter.CreateContinousProductFile(m_outputDir, m_practiceName, m_countryName, curYear);

        // start processing features
        using namespace std::placeholders;
        std::function<bool(const FeatureDescription&, void*)> f = std::bind(&TimeSeriesAnalysis::HandleFeature, this, _1, _2);
        m_pPracticeReader->ExtractFeatures(f);

        // close the plots file
        m_plotsWriter.ClosePlotsFile();

        otbAppLogINFO("Execution DONE!");
    }

    bool HandleFeature(const FeatureDescription& feature, void*) {
        // DisplayFeature(feature);

        const std::string &fieldId = feature.GetFieldId();
        const std::string &vegetationStart = feature.GetVegetationStart();
        const std::string &harvestStart = feature.GetHarvestStart();
        const std::string &harvestEnd = feature.GetHarvestEnd();
        const std::string &practiceStart = feature.GetPracticeStart();
        const std::string &practiceEnd = feature.GetPracticeEnd();

//        if (feature.GetFieldSeqId() != "883293") {
//            return false;
//        }
        FieldInfoType fieldInfos(fieldId);

        fieldInfos.fieldSeqId = feature.GetFieldSeqId();
        fieldInfos.ttVegStartTime = GetTimeFromString(vegetationStart);
        fieldInfos.ttVegStartWeekFloorTime = FloorDateToWeekStart(fieldInfos.ttVegStartTime);
        fieldInfos.ttHarvestStartTime = GetTimeFromString(harvestStart);
        fieldInfos.ttHarvestStartWeekFloorTime = FloorDateToWeekStart(fieldInfos.ttHarvestStartTime);
        fieldInfos.ttHarvestEndTime = GetTimeFromString(harvestEnd);
        fieldInfos.ttHarvestEndWeekFloorTime = FloorDateToWeekStart(fieldInfos.ttHarvestEndTime);
        fieldInfos.ttPracticeStartTime = GetTimeFromString(practiceStart);
        fieldInfos.ttPracticeStartWeekFloorTime = FloorDateToWeekStart(fieldInfos.ttPracticeStartTime);
        fieldInfos.ttPracticeEndTime = GetTimeFromString(practiceEnd);
        fieldInfos.ttPracticeEndWeekFloorTime = FloorDateToWeekStart(fieldInfos.ttPracticeEndTime);
        fieldInfos.practiceName = feature.GetPractice();
        fieldInfos.countryCode = feature.GetCountryCode();
        fieldInfos.mainCrop = feature.GetMainCrop();
        fieldInfos.practiceType = feature.GetPracticeType();
        fieldInfos.s1PixValue = feature.GetS1Pix();
        int s1PixVal = std::atoi(fieldInfos.s1PixValue.c_str());
        if (s1PixVal == 0) {
            // set also the fieldInfos.s1PixValue to "0"
            fieldInfos.s1PixValue = std::to_string(s1PixVal);
        }

        int year;
        if (!GetWeekFromDate(vegetationStart, year, fieldInfos.vegStartWeekNo))
        {
            otbAppLogWARNING("Cannot extract vegetation start week from the date " <<
                             vegetationStart << " of the feature " << fieldId);
            return false;
        }
        fieldInfos.year = year;
        if (!GetWeekFromDate(harvestStart, year, fieldInfos.harvestStartWeekNo))
        {
            otbAppLogWARNING("Cannot extract harvest start week from the date " <<
                             harvestStart << " of the feature " << fieldId);
            return false;
        }
        if (year != fieldInfos.year) {
            otbAppLogWARNING("Vegetation year and harvest start year are different for field " << fieldId);
        }
        if (year != fieldInfos.year) {
            otbAppLogWARNING("Vegetation year and harvest start year are different for field " << fieldId);
        }

        if (fieldInfos.vegStartWeekNo == 1) {
            // increment the vegetation start time to the next week
            fieldInfos.ttVegStartWeekFloorTime += SEC_IN_WEEK;
            fieldInfos.vegStartWeekNo++;
        }
        if (fieldInfos.harvestStartWeekNo == 1) {
            // increment the harvest start time to the next week
            fieldInfos.ttHarvestStartWeekFloorTime += SEC_IN_WEEK;
            fieldInfos.harvestStartWeekNo++;
        }

//      DEBUG
        m_debugPrinter.PrintFieldGeneralInfos(fieldInfos);
//      DEBUG

        bool bOK = true;
        int harvestStatusInitVal = NOT_AVAILABLE;
        if (s1PixVal < m_nMinS1PixCnt) {
            bOK = false;
            harvestStatusInitVal = NOT_AVAILABLE_1;
        }

        if (m_bVerbose) {
            otbAppLogINFO("Extracting amplitude file infos for field  " << fieldId);
        }
        if (bOK && !m_tsaDataExtractor.ExtractAmplitudeFilesInfos(fieldInfos)) {
            bOK = false;
        }

        if (m_bVerbose) {
            otbAppLogINFO("Extracting coherence file infos for field  " << fieldId);
        }
        if (bOK && !m_tsaDataExtractor.ExtractCoherenceFilesInfos(fieldInfos)) {
            bOK = false;
        }

        if (m_bVerbose) {
            otbAppLogINFO("Extracting NDVI file infos for field  " << fieldId);
        }
        if (bOK && !m_tsaDataExtractor.ExtractNdviFilesInfos(fieldInfos)) {
            bOK = false;
        }

        if (m_bVerbose) {
            otbAppLogINFO("Processing infos for field  " << fieldId);
        }
        if (bOK && !ProcessFieldInformation(fieldInfos)) {
            bOK = false;
        }
        if (!bOK) {
            fieldInfos.gapsInfos = harvestStatusInitVal;
            fieldInfos.hS1GapsInfos = harvestStatusInitVal;
            fieldInfos.h_W_S1GapsInfos = harvestStatusInitVal;
            fieldInfos.pS1GapsInfos = harvestStatusInitVal;

            // in case an error occurred, write in the end the parcel but with invalid infos
            HarvestEvaluationInfoType harvestEvalInfos(harvestStatusInitVal);
            EfaEvaluationInfoType efaEvalInfos(harvestStatusInitVal);
            TillageEvaluationInfoType tillageInfos(harvestStatusInitVal);
            harvestEvalInfos.Initialize(fieldInfos);
            efaEvalInfos.Initialize(fieldInfos);
            m_csvWriter.WriteHarvestInfoToCsv(fieldInfos, harvestEvalInfos, efaEvalInfos, tillageInfos);
        }

        return bOK;
    }

    // Compute L_DATE
    time_t GetMaxCohDate(const FieldInfoType &fieldInfos, const std::vector<MergedAllValInfosType> &allMergedValues) {
        time_t ttMaxCohDate = 0;
        time_t ttLastMergedValuesDate = allMergedValues[allMergedValues.size()-1].ttDate;
        time_t ttCurFloorDate;
        for (const InputFileLineInfoType &linfo : fieldInfos.coheVVLines) {
            ttCurFloorDate = linfo.GetFloorTime();
            if (ttCurFloorDate == ttLastMergedValuesDate && ttCurFloorDate > ttMaxCohDate) {
                ttMaxCohDate = ttCurFloorDate;
            }
        }
        return ttMaxCohDate;
    }

    bool ProcessFieldInformation(FieldInfoType &fieldInfos) {

        std::vector<MergedAllValInfosType> allMergedValues;
        if (!m_tsaDataExtrPreProc.GroupAndMergeAllData(fieldInfos, fieldInfos.ampVHLines,
                                                       fieldInfos.ampVVLines, fieldInfos.ndviLines,
                                                       fieldInfos.coheVVLines, fieldInfos.mergedAmpInfos,
                                                       fieldInfos.ampRatioGroups, fieldInfos.ndviGroups,
                                                       fieldInfos.coherenceGroups, allMergedValues)) {
            return false;
        }

        bool bShortenVegWeek = false;
        if (m_efaHandler) {
            bShortenVegWeek = m_efaHandler->IsShorteningHarvestInterval(fieldInfos);
        }
        if (m_tillageHandler) {
            bShortenVegWeek = m_tillageHandler->IsShorteningHarvestInterval(fieldInfos);
        }
        // ### TIME SERIES ANALYSIS FOR HARVEST ###
        HarvestEvaluationInfoType harvestInfos;
        m_tsaHarvestOnlyHandler.SetShortenVegWeeks(bShortenVegWeek);
        m_tsaHarvestOnlyHandler.PerformHarvestEvaluation(fieldInfos, allMergedValues, harvestInfos);

        EfaEvaluationInfoType efaHarvestEvalInfos;
        // ### TIME SERIES ANALYSIS FOR EFA PRACTICES ###
        if (m_efaHandler && fieldInfos.practiceName != NA_STR && fieldInfos.ttPracticeStartTime != 0) {
            efaHarvestEvalInfos.Initialize(fieldInfos);
            efaHarvestEvalInfos.SetValid(true);

            if (fieldInfos.practiceName.find(m_CatchCropVal) != std::string::npos ||
                    fieldInfos.practiceName == m_FallowLandVal ||
                    fieldInfos.practiceName == m_NitrogenFixingCropVal) {
                m_efaHandler->PerformAnalysis(fieldInfos, allMergedValues, harvestInfos, efaHarvestEvalInfos);
            } else {
                otbAppLogWARNING("Practice name " << fieldInfos.practiceName << " not supported!");
            }
            time_t ttMaxCohDate = GetMaxCohDate(fieldInfos, allMergedValues);
            if (efaHarvestEvalInfos.ttPracticeEndTime > ttMaxCohDate) {
                efaHarvestEvalInfos.efaIndex = NR_STR;
            }
        }

        TillageEvaluationInfoType tillageEvalInfos;
        if (m_tillageHandler) {
            m_tillageHandler->PerformAnalysis(fieldInfos, allMergedValues, harvestInfos, tillageEvalInfos);
        }

        // write infos to be generated as plots
        m_plotsWriter.WritePlotEntry(fieldInfos, harvestInfos, efaHarvestEvalInfos);

        // write the harvest information to the final file
        m_csvWriter.WriteHarvestInfoToCsv(fieldInfos, harvestInfos, efaHarvestEvalInfos, tillageEvalInfos);

        // Write the continuous field infos into the file
        m_contFileWriter.WriteContinousToCsv(fieldInfos, allMergedValues);

        return true;
    }

    void DisplayFeature(const FeatureDescription& feature)
    {
        std::cout << feature.GetFieldId() << " ; " <<
                     feature.GetCountryCode() << " ; " <<
                     feature.GetYear() << " ; " <<
                     feature.GetMainCrop() << " ; " <<
                     feature.GetVegetationStart() << " ; " <<
                     feature.GetHarvestStart() << " ; " <<
                     feature.GetHarvestEnd() << " ; " <<
                     feature.GetPractice() << " ; " <<
                     feature.GetPracticeType() << " ; " <<
                     feature.GetPracticeStart() << " ; " <<
                     feature.GetPracticeEnd() << std::endl;
    }

    std::string GetCatchMain() {
        if (HasValue("catchmain")) {
            return GetParameterString("catchmain");
        }
        return "";
    }
    std::string GetCatchCropIsMain() {
        std::string retVal = CATCH_CROP_IS_MAIN_VAL;
        if (HasValue("catchcropismain")) {
            retVal = GetParameterString("catchcropismain");
        }
        return retVal;
    }
    // in days (e.g. 8 weeks == 56 days)
    int GetCatchPeriod() {
        return GetParameterInt("catchperiod");
    }

    // buffer threshold
    double GetCatchProportion() {
        return GetParameterFloat("catchproportion");
    }
    std::string GetCatchPeriodStart() {
        if (HasValue("catchperiodstart")) {
            return GetParameterString("catchperiodstart");
        }
        return "";
    }
    double GetOpticalThrVegCycle() {
        double OpticalThrVegCycle = 550; //350;
        if (HasValue("optthrvegcycle")) {
            OpticalThrVegCycle = GetParameterFloat("optthrvegcycle");
        }
        return OpticalThrVegCycle;
    }
    // for MARKER 2 - NDVI loss
    // expected value of harvest/clearance
    double GetNdviDown() {
        double NdviDown = 300;// 350;
        if (HasValue("ndvidw")) {
            NdviDown = GetParameterFloat("ndvidw");
        }
        return NdviDown;
    }
    // buffer value (helps in case of sparse ndvi time-series)
    double GetNdviUp() {
        double NdviUp = 400;//550;
        if (HasValue("ndviup")) {
            NdviUp = GetParameterFloat("ndviup");
        }
        return NdviUp;
    }
    // opt.thr.value is round up to ndvi.step
    double GetNdviStep( ) {
        double NdviStep = 5;
        if (HasValue("ndvistep")) {
            NdviStep = GetParameterFloat("ndvistep");
        }
        return NdviStep;
    }
    double GetOpticalThresholdMinimum() {
        double OpticalThresholdMinimum = 100;
        if (HasValue("optthrmin")) {
            OpticalThresholdMinimum = GetParameterFloat("optthrmin");
        }
        return OpticalThresholdMinimum;
    }

    // for MARKER 5 - COHERENCE increase
    double GetCohThrBase() {
        double CohThrBase = 0.1; //0.05;
        if (HasValue("cohthrbase")) {
            CohThrBase = GetParameterFloat("cohthrbase");
        }
        return CohThrBase;
    }
    double GetCohThrHigh() {
        double CohThrHigh = 0.2; //0.15;
        if (HasValue("cohthrhigh")) {
            CohThrHigh = GetParameterFloat("cohthrhigh");
        }
        return CohThrHigh;
    }
    double GetCohThrAbs() {
        double CohThrAbs = 0.7;  //0.75;
        if (HasValue("cohthrabs")) {
            CohThrAbs = GetParameterFloat("cohthrabs");
        }
        return CohThrAbs;
    }

    double GetTLCohThrBase() {
        double CohThrBase = 0.05;
        if (HasValue("cohthrbase")) {
            CohThrBase = GetParameterFloat("tlcohthrbase");
        }
        return CohThrBase;
    }
    double GetTLCohThrAbs() {
        double CohThrAbs = 0.75;
        if (HasValue("tlcohthrabs")) {
            CohThrAbs = GetParameterFloat("tlcohthrabs");
        }
        return CohThrAbs;
    }

    // for MARKER 3 - BACKSCATTER loss
    double GetAmpThrMinimum() {
        double AmpThrMinimum = 0.1;
        if (HasValue("ampthrmin")) {
            AmpThrMinimum = GetParameterFloat("ampthrmin");
        }
        return AmpThrMinimum;
    }

    // INPUT THRESHOLDS - EFA PRACTICE evaluation
    int GetEfaNdviThr() {
        int EfaNdviThr = 400; // 325;
        if (HasValue("efandvithr")) {
            EfaNdviThr = GetParameterInt("efandvithr");
        }
        return EfaNdviThr;
    }
    int GetEfaNdviUp() {
        int EfaNdviUp = 600; // 400;
        if (HasValue("efandviup")) {
            EfaNdviUp = GetParameterInt("efandviup");
        }
        return EfaNdviUp;
    }
    int GetEfaNdviDown() {
        int EfaNdviDown = 600; // 300;
        if (HasValue("efandvidw")) {
            EfaNdviDown = GetParameterInt("efandvidw");
        }
        return EfaNdviDown;
    }
    double GetEfaCohChange() {
        double EfaCohChange = 0.2;
        if (HasValue("efacohchange")) {
            EfaCohChange = GetParameterFloat("efacohchange");
        }
        return EfaCohChange;
    }
    double GetEfaCohValue() {
        double EfaCohValue = 0.7;
        if (HasValue("efacohvalue")) {
            EfaCohValue = GetParameterFloat("efacohvalue");
        }
        return EfaCohValue;
    }
    double GetEfaNdviMin() {
        double EfaNdviMin = NOT_AVAILABLE;
        if (HasValue("efandvimin")) {
            EfaNdviMin = GetParameterFloat("efandvimin");
        }
        return EfaNdviMin;
    }
    double GetEfaAmpThr() {
        double EfaAmpThr = NOT_AVAILABLE;
        if (HasValue("efaampthr")) {
            EfaAmpThr = GetParameterFloat("efaampthr");
        }
        return EfaAmpThr;
    }
    bool GetUseStdDevInAmpThrValComp() {
        bool UseStdDevInAmpThrValComp = false;
        if (HasValue("stddevinampthr")) {
            UseStdDevInAmpThrValComp = GetParameterInt("stddevinampthr");
        }
        return UseStdDevInAmpThrValComp;
    }
    int GetOpticalThrBufDenominator() {
        int OpticalThrBufDenominator = 6;
        if (HasValue("optthrbufden")) {
            OpticalThrBufDenominator = GetParameterInt("optthrbufden");
        }
        return OpticalThrBufDenominator;
    }
    int GetAmpThrBreakDenominator() {
        int AmpThrBreakDenominator = 6;
        if (HasValue("ampthrbreakden")) {
            AmpThrBreakDenominator = GetParameterInt("ampthrbreakden");
        }
        return AmpThrBreakDenominator;
    }
    int GetAmpThrValDenominator() {
        int AmpThrValDenominator = 2;
        if (HasValue("ampthrvalden")) {
            AmpThrValDenominator = GetParameterInt("ampthrvalden");
        }
        return AmpThrValDenominator;
    }
    std::string GetFlMarkersStartDateStr() {
        if (HasValue("flmarkstartdate")) {
            return GetParameterString("flmarkstartdate");
        }
        return "";
    }
    std::string GetFlMarkersEndDateStr() {
        if (HasValue("flmarkstenddate")) {
            return GetParameterString("flmarkstenddate");
        }
        return "";
    }

    void InitializeEfaHandler()
    {
        TsaEfaAnalysisBase *pHandler = NULL;
        if (m_practiceName == CATCH_CROP_VAL) {
            pHandler = new TsaCatchCropAnalysisHandler(GetLogger());
            InitializeEfaHandler(pHandler);
            ((TsaCatchCropAnalysisHandler*)pHandler)->SetCatchMain(GetCatchMain());
            ((TsaCatchCropAnalysisHandler*)pHandler)->SetCatchCropIsMain(GetCatchCropIsMain());
            ((TsaCatchCropAnalysisHandler*)pHandler)->SetCatchPeriod(GetCatchPeriod());
            ((TsaCatchCropAnalysisHandler*)pHandler)->SetCatchProportion(GetCatchProportion());
            ((TsaCatchCropAnalysisHandler*)pHandler)->SetCatchPeriodStart(GetCatchPeriodStart());
            ((TsaCatchCropAnalysisHandler*)pHandler)->SetOpticalThrVegCycle(GetOpticalThrVegCycle());
        } else if (m_practiceName == FALLOW_LAND_VAL) {
            pHandler = new TsaFallowAnalysisHandler(GetLogger());
            InitializeEfaHandler(pHandler);
            ((TsaFallowAnalysisHandler*)pHandler)->SetMarkersStartDate(GetFlMarkersStartDateStr());
            ((TsaFallowAnalysisHandler*)pHandler)->SetMarkersEndDate(GetFlMarkersEndDateStr());
        } else if (m_practiceName == NITROGEN_FIXING_CROP_VAL) {
            pHandler = new TsaNfcAnalysisHandler(GetLogger());
            InitializeEfaHandler(pHandler);
        }

        if (pHandler != NULL) {
            m_efaHandler = std::unique_ptr<TsaEfaAnalysisBase>(pHandler);
        }
    }

    void InitializeHarvestEvaluationHandler() {
        m_tsaHarvestOnlyHandler.SetOpticalThrVegCycle(GetOpticalThrVegCycle());
        m_tsaHarvestOnlyHandler.SetNdviDown(GetNdviDown());
        m_tsaHarvestOnlyHandler.SetNdviUp(GetNdviUp());
        m_tsaHarvestOnlyHandler.SetNdviStep(GetNdviStep());
        m_tsaHarvestOnlyHandler.SetOpticalThresholdMinimum(GetOpticalThresholdMinimum());

        m_tsaHarvestOnlyHandler.SetCohThrBase(GetCohThrBase());
        m_tsaHarvestOnlyHandler.SetCohThrHigh(GetCohThrHigh());
        m_tsaHarvestOnlyHandler.SetCohThrAbs(GetCohThrAbs());

        m_tsaHarvestOnlyHandler.SetAmpThrMinimum(GetAmpThrMinimum());

        m_tsaHarvestOnlyHandler.SetUseStdDevInAmpThrValComp(GetUseStdDevInAmpThrValComp());
        m_tsaHarvestOnlyHandler.SetOpticalThrBufDenominator(GetOpticalThrBufDenominator());
        m_tsaHarvestOnlyHandler.SetAmpThrBreakDenominator(GetAmpThrBreakDenominator());
        m_tsaHarvestOnlyHandler.SetAmpThrValDenominator(GetAmpThrValDenominator());

        m_tsaHarvestOnlyHandler.SetLimitAcqDate(m_ttLimitAcqDate);
    }

    void InitializeEfaHandler(TsaEfaAnalysisBase *pHandler) {
        pHandler->SetEfaNdviThr(GetEfaNdviThr());
        pHandler->SetEfaNdviUp(GetEfaNdviUp());
        pHandler->SetEfaNdviDown(GetEfaNdviDown());

        pHandler->SetEfaCohChange(GetEfaCohChange());
        pHandler->SetEfaCohValue(GetEfaCohValue());

        pHandler->SetEfaNdviMin(GetEfaNdviMin());
        pHandler->SetEfaAmpThr(GetEfaAmpThr());

        pHandler->SetCohThrBase(GetCohThrBase());
        pHandler->SetCohThrHigh(GetCohThrHigh());
        pHandler->SetCohThrAbs(GetCohThrAbs());

        pHandler->SetNdviDown(GetNdviDown());
        pHandler->SetNdviUp(GetNdviUp());
        pHandler->SetNdviStep(GetNdviStep());
    }

    void InitializeTillageHandler() {
        if (m_bMonitorTillage) {
            TsaTillageAnalysisHandler *pHandler = new TsaTillageAnalysisHandler();
            pHandler->SetOpticalThrVegCycle(GetOpticalThrVegCycle());
            pHandler->SetCohThrBase(GetTLCohThrBase());
            pHandler->SetCohThrAbs(GetTLCohThrAbs());
            m_tillageHandler = std::unique_ptr<TsaTillageAnalysisHandler>(pHandler);
        }
    }
private:
    std::unique_ptr<PracticeReaderBase> m_pPracticeReader;

    std::string m_outputDir;
    std::string m_countryName;
    std::string m_practiceName;
    std::string m_year;

    std::string m_CatchCropVal;
    std::string m_FallowLandVal;
    std::string m_NitrogenFixingCropVal;

    // # optional: in case graphs shall be generated set the value to TRUE otherwise set to FALSE
    TsaPlotsWriter m_plotsWriter;
    TsaCSVWriter m_csvWriter;
    TsaContinuousFileWriter m_contFileWriter;
    TsaDebugPrinter m_debugPrinter;

    TsaPrevPrdReader m_prevPrdReader;

    bool m_bVerbose;

    int m_nMinS1PixCnt;

    time_t m_ttLimitAcqDate;

    TsaHarvestOnlyAnalysisHandler m_tsaHarvestOnlyHandler;
    std::unique_ptr<TsaEfaAnalysisBase> m_efaHandler;
    std::unique_ptr<TsaTillageAnalysisHandler> m_tillageHandler;

    TsaDataExtractor m_tsaDataExtractor;
    TsaDataExtrPreProcessor m_tsaDataExtrPreProc;

    HarvestEvaluationInfoType m_NAHarvestEvalInfos;
    EfaEvaluationInfoType m_NAEfaEvalInfos;
    TillageEvaluationInfoType m_NATillageEvalInfos;

    bool m_bMonitorTillage;


};

} // end of namespace Wrapper
} // end of namespace otb

OTB_APPLICATION_EXPORT(otb::Wrapper::TimeSeriesAnalysis)
