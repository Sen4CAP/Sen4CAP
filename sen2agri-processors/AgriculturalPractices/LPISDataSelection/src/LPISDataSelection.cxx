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
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

#include "string_utils.hpp"
#include "otbOGRDataSourceWrapper.h"

#include "CountryInfoFactory.h"
#include "../../Common/include/GSAAAttributesTablesReaderFactory.h"

#include "CommonFunctions.h"

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

namespace otb
{
namespace Wrapper
{
class LPISDataSelection : public Application
{
public:
    /** Standard class typedefs. */
    typedef LPISDataSelection        Self;
    typedef Application                   Superclass;
    typedef itk::SmartPointer<Self>       Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    /** Standard macro */
    itkNewMacro(Self);

    itkTypeMacro(LPISDataSelection, otb::Application);

    typedef std::map<std::string,int> MapIndex;//this map stores the index of data stored
                                               //in VectorData mapped to a string

    typedef struct FidInfos {
        std::string date;
        std::string date2;
        double meanVal;
        double stdDevVal;
    } FidInfosType;

    typedef struct Fid {
        std::string fid;
        std::string name;
        std::vector<FidInfosType> infos;
    } FidType;

    typedef struct {
          bool operator() (const FidInfosType &i, const FidInfosType &j) {
              return ((i.date.compare(j.date)) < 0);
              //return (i.date < j.date);
          }
    } FidInfosComparator;




private:
    LPISDataSelection() : m_bWriteIdsOnlyFile(false)
    {
    }

    void DoInit() override
    {
        SetName("LPISDataSelection");
        SetDescription("Extracts agricultural practices information from the input shapefile and additional files");

        // Documentation
        SetDocName("Agricultural practices information extractor");
        SetDocLongDescription("TODO");
        SetDocLimitations("None");
        SetDocAuthors("Cosmin UDROIU");
        SetDocSeeAlso(" ");

        AddParameter(ParameterType_String, "inshp", "Input shapefile");
        SetParameterDescription("inshp", "Support list of images to be merged to output");

        AddParameter(ParameterType_StringList, "addfiles", "Additional files");
        SetParameterDescription("addfiles", "List of files specific to each country containing additional information to be used");
        MandatoryOff("addfiles");

        AddParameter(ParameterType_String, "practice", "Practice");
        SetParameterDescription("practice", "The practice name");
        MandatoryOff("practice");

        AddParameter(ParameterType_String, "country", "Country");
        SetParameterDescription("country", "The country short name");

        AddParameter(ParameterType_String, "year", "Year");
        SetParameterDescription("year", "The year to be added into the practices file");
        MandatoryOff("year");

        AddParameter(ParameterType_String, "vegstart", "Vegetation start");
        SetParameterDescription("vegstart", "The vegetation start date");
        MandatoryOff("vegstart");

        AddParameter(ParameterType_String, "hstart", "Harvest start");
        SetParameterDescription("hstart", "The harvest start date");
        MandatoryOff("hstart");

        AddParameter(ParameterType_String, "hwinterstart", "Harvest winter start");
        SetParameterDescription("hwinterstart", "The harvest winter start date (when applicable)");
        MandatoryOff("hwinterstart");

        AddParameter(ParameterType_String, "hend", "Harvest end");
        SetParameterDescription("hend", "he harvest end date");
        MandatoryOff("hend");

        AddParameter(ParameterType_String, "hwinterend", "Harvest winter end");
        SetParameterDescription("hwinterend", "The harvest winter end date (when applicable)");
        MandatoryOff("hwinterend");

        AddParameter(ParameterType_String, "pstart", "Practice start");
        SetParameterDescription("pstart", "The practice start date");
        MandatoryOff("pstart");

        AddParameter(ParameterType_String, "pend", "Practice end");
        SetParameterDescription("pend", "The practice end date");
        MandatoryOff("pend");

        AddParameter(ParameterType_String, "wpstart", "Winter Practice start");
        SetParameterDescription("wpstart", "The winter practice start date (when applicable)");
        MandatoryOff("wpstart");

        AddParameter(ParameterType_String, "wpend", "Winter Practice end");
        SetParameterDescription("pend", "The winter practice end date (when applicable)");
        MandatoryOff("wpend");

        AddParameter(ParameterType_OutputFilename, "out", "Output practice CSV file.");
        SetParameterDescription("out", "Output file where the practices information are saved as CSV. "
                                       "If the field seqidsonly is set to 0 only SEQ_ID is exported");

        AddParameter(ParameterType_Int, "seqidsonly", "Export to the output file only the monitorable parcel ids");
        SetParameterDescription("seqidsonly", "Export to the output file only the monitorable parcel ids");
        MandatoryOff("seqidsonly");
        SetDefaultParameterInt("seqidsonly", 0);

        AddParameter(ParameterType_String, "filters", "Filter field ids");
        SetParameterDescription("filters","Filter field ids");
        MandatoryOff("filters");

        AddParameter(ParameterType_String, "ignoredids", "File containing the list of field ids that will be ignored from the extraction");
        SetParameterDescription("ignoredids","File containing the list of field ids that will be ignored from the extraction");
        MandatoryOff("ignoredids");

        AddParameter(ParameterType_String, "copydir", "A directory where a copy of the created output file will be copied");
        SetParameterDescription("copydir","A directory where a copy of the created output file will be copied");
        MandatoryOff("copydir");

        AddRAMParameter();

        // Doc example parameter settings
        SetDocExampleParameterValue("il", "file1.xml file2.xml");
        SetDocExampleParameterValue("out","time_series.xml");

        //SetOfficialDocLink();
    }

    void DoUpdateParameters() override
    {

    }


    void DoExecute() override
    {
        const std::string &inShpFile = this->GetParameterString("inshp");
        if(inShpFile.size() == 0) {
            otbAppLogFATAL(<<"No image was given as input!");
        }
        m_bWriteIdsOnlyFile = (GetParameterInt("seqidsonly") != 0);
        m_country = this->GetParameterString("country");
        auto factory = CountryInfoFactory::New();
        m_pCountryInfos = factory->GetCountryInfo(m_country);
        m_year = this->GetParameterString("year");
        m_pCountryInfos->SetYear(m_year);

        if (!m_bWriteIdsOnlyFile) {
            if (!HasValue("vegstart")) {
                otbAppLogFATAL(<<"vegstart is mandatory when extracting practices file!");
            }
            if (!HasValue("hstart")) {
                otbAppLogFATAL(<<"hstart is mandatory when extracting practices file!");
            }
            if (!HasValue("hend")) {
                otbAppLogFATAL(<<"hend is mandatory when extracting practices file!");
            }
            if (!HasValue("year")) {
                otbAppLogFATAL(<<"year is mandatory when extracting practices file!");
            }
            m_pCountryInfos->SetVegStart(trim(GetParameterAsString("vegstart")));

            std::string practice;
            std::string practiceStart;
            std::string practiceEnd;
            std::string wPracticeStart;
            std::string wPracticeEnd;
            if (HasValue("practice")) {
                practice = GetParameterAsString("practice");
            }
            if (HasValue("pstart")) {
                practiceStart = GetParameterAsString("pstart");
            }
            if (HasValue("pend")) {
                practiceEnd = GetParameterAsString("pend");
            }
            if (HasValue("wpstart")) {
                wPracticeStart = GetParameterAsString("wpstart");
            }
            if (HasValue("wpend")) {
                wPracticeEnd = GetParameterAsString("wpend");
            }
            m_pCountryInfos->SetPractice(trim(practice));
            m_pCountryInfos->SetPStart(trim(practiceStart));
            m_pCountryInfos->SetPEnd(trim(practiceEnd));
            m_pCountryInfos->SetWinterPStart(trim(wPracticeStart));
            m_pCountryInfos->SetWinterPEnd(trim(wPracticeEnd));
            m_pCountryInfos->SetHStart(trim(GetParameterAsString("hstart")));
            m_pCountryInfos->SetHEnd(trim(GetParameterAsString("hend")));

            if (HasValue("hwinterstart")) {
                m_pCountryInfos->SetHWinterStart(trim(GetParameterAsString("hwinterstart")));
            }
            if (HasValue("hwinterend")) {
                m_pCountryInfos->SetHWinterEnd(trim(GetParameterAsString("hwinterend")));
            }

        }
        const std::string &outFileName = GetParameterAsString("out");
        m_outPracticesFileStream.open(outFileName, std::ios_base::trunc | std::ios_base::out );
        if (!m_outPracticesFileStream.is_open()) {
            otbAppLogFATAL("Cannot open output file " << outFileName);
        }
        WritePracticesFileHeader();

        if (HasValue("addfiles")) {
            m_pCountryInfos->SetAdditionalFiles(GetParameterStringList("addfiles"));
        }

        m_FieldFilters = LoadIdsFile("filters");
        m_IgnoredIds = LoadIdsFile("ignoredids");

        otbAppLogINFO("#######################################");
        otbAppLogINFO("Using input: " << inShpFile);
        otbAppLogINFO("Method: " << (m_bWriteIdsOnlyFile ? "NewID ONLY" : " Full Practices Information"));
        otbAppLogINFO("Output: " << outFileName);
        otbAppLogINFO("#######################################");

        // Start the processing
        boost::filesystem::path practicesInfoPath(inShpFile);
        std::string pfFormat = practicesInfoPath.extension().c_str();
        pfFormat.erase(pfFormat.begin(), std::find_if(pfFormat.begin(), pfFormat.end(), [](int ch) {
                return ch != '.';
            }));

        auto practiceReadersFactory = GSAAAttributesTablesReaderFactory::New();
        m_pGSAAAttrsTablesReader = practiceReadersFactory->GetPracticeReader(pfFormat);
        m_pGSAAAttrsTablesReader->SetSource(inShpFile);

        // start processing features
        using namespace std::placeholders;
        std::function<void(const AttributeEntry&)> f = std::bind(&LPISDataSelection::ProcessFeature, this, _1);
        m_bFirstFeature = true;
        if (!m_pGSAAAttrsTablesReader->ExtractAttributes(f)) {
            otbAppLogFATAL(<<"Cannot extract attributes from provided input file " << inShpFile);
        }

        otbAppLogINFO("Extraction DONE!");

        CopyToTargetFolder(outFileName);
    }

    void ProcessFeature(const AttributeEntry& ogrFeat) {
        if (m_bFirstFeature) {
            // Initialize any indexes from the first feature
            m_pCountryInfos->InitializeIndexes(ogrFeat);
            m_bFirstFeature = false;
        }

        if (!FilterFeature(ogrFeat)) {
            return;
        }

        WritePracticesFileLine(ogrFeat);
    }

    void WritePracticesFileHeader() {
        if (!m_outPracticesFileStream.is_open()) {
            std::cout << "Trying  to write practices file header in a closed stream!" << std::endl;
            return;
        }
        if (m_bWriteIdsOnlyFile) {
            m_outPracticesFileStream << "SEQ_ID\n";
        } else {
            // # create result csv file for harvest and EFA practice evaluation
            m_outPracticesFileStream << "SEQ_ID;FIELD_ID;COUNTRY;YEAR;MAIN_CROP;VEG_START;H_START;H_END;PRACTICE;P_TYPE;P_START;P_END;"
                                        "GeomValid;Duplic;Overlap;Area_meter;ShapeInd;CTnum;CT;LC;S1Pix;S2Pix\n";
        }
    }

    void WritePracticesFileLine(const AttributeEntry& ogrFeat) {
        if (!m_outPracticesFileStream.is_open()) {
            std::cout << "Trying  to write feature in a closed stream!" << std::endl;
            return;
        }
        const std::string &uid = m_pCountryInfos->GetOriId(ogrFeat);
        int seqId = m_pCountryInfos->GetSeqId(ogrFeat);
        const std::string &mainCrop = GetValueOrNA(m_pCountryInfos->GetMainCrop(ogrFeat));
        if (mainCrop == "NA") {
            std::cout << "Main crop NA - Ignoring field with unique ID " << uid << std::endl;
            return;
        }

        if (m_bWriteIdsOnlyFile) {
            m_outPracticesFileStream << seqId << "\n";
        } else {
            m_outPracticesFileStream << seqId << ";" << uid.c_str() << ";" << m_country.c_str() << ";" << m_year.c_str() << ";";
            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetMainCrop(ogrFeat)).c_str() << ";";
            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetVegStart()).c_str() << ";";
            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetHStart(ogrFeat)).c_str() << ";";
            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetHEnd(ogrFeat)).c_str() << ";";
            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetPractice(ogrFeat)).c_str() << ";";
            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetPracticeType(ogrFeat)).c_str() << ";";
            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetPStart(ogrFeat)).c_str() << ";";
            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetPEnd(ogrFeat)).c_str() << ";";

            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetGeomValid(ogrFeat)).c_str() << ";";
            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetDuplic(ogrFeat)).c_str() << ";";
            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetOverlap(ogrFeat)).c_str() << ";";
            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetArea_meter(ogrFeat)).c_str() << ";";
            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetShapeInd(ogrFeat)).c_str() << ";";
            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetCTnum(ogrFeat)).c_str() << ";";
            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetCT(ogrFeat)).c_str() << ";";
            m_outPracticesFileStream << GetValueOrNA(m_pCountryInfos->GetLC(ogrFeat)).c_str() << ";";
            m_outPracticesFileStream << GetIntRepresentation(m_pCountryInfos->GetS1Pix(ogrFeat)).c_str() << ";";
            m_outPracticesFileStream << GetIntRepresentation(m_pCountryInfos->GetS2Pix(ogrFeat)).c_str() << "\n";
        }
    }

    std::string GetValueOrNA(const std::string &str) {
        if (str.size() > 0) {
            return str;
        }
        return "NA";
    }

    std::string GetIntRepresentation(const std::string &str) {
        int intVal = std::atoi(str.c_str());
        return std::to_string(intVal);
    }

    bool FilterFeature(const AttributeEntry &ogrFeat) {
        // if we have filters and we did not find the id
        std::string uid = m_pCountryInfos->GetOriId(ogrFeat);
        NormalizeFieldId(uid);
        if(m_FieldFilters.size() != 0 && m_FieldFilters.find(uid) == m_FieldFilters.end()) {
            return false;
        }
        if(m_IgnoredIds.size() != 0 && m_IgnoredIds.find(uid) != m_IgnoredIds.end()) {
            return false;
        }

        if (!m_pCountryInfos->IsMonitoringParcel(ogrFeat)) {
            return false;
        }

        if (m_bWriteIdsOnlyFile) {
            return true;
        }

        // If practice information are needed, then export the fields according to the practice
        bool ret = false;
        // If the current line has required practice
        if (m_pCountryInfos->GetPractice().size() > 0 &&
                // this should be filled but there were some errors for some countries in input data
                m_pCountryInfos->GetPractice() != "NA") {
            if (m_pCountryInfos->GetHasPractice(ogrFeat, m_pCountryInfos->GetPractice())) {
                ret = true;
            }

        } else if (!m_pCountryInfos->GetHasPractice(ogrFeat, CATCH_CROP_VAL) &&
                !m_pCountryInfos->GetHasPractice(ogrFeat, FALLOW_LAND_VAL) &&
                !m_pCountryInfos->GetHasPractice(ogrFeat, NITROGEN_FIXING_CROP_VAL)) {
            ret = true;
        }

        return ret;
    }

    std::map<std::string, int> LoadIdsFile(const std::string &paramName) {
        std::map<std::string, int> filters;
        if (HasValue(paramName)) {
            const std::string &filtersFile = trim(GetParameterAsString(paramName));
            std::ifstream fStream(filtersFile);
            if (!fStream.is_open()) {
                otbAppLogFATAL("Cannot load the specified filters file: " << filtersFile);
            }
            std::string line;
            while (std::getline(fStream, line)) {
                NormalizeFieldId(line);
                filters[line] = 1;
            }
            otbAppLogINFO("Found a number of " << filters.size() << " filters!")
        }
        return filters;
    }

    void CopyToTargetFolder(const std::string &outFilePath) {
        if (HasValue("copydir")) {
            const std::string &copyDir = this->GetParameterString("copydir");
            if(copyDir.size() == 0 || !boost::filesystem::is_directory(copyDir)) {
                return;
            }
            otbAppLogINFO("Copying file " << outFilePath << " to " << copyDir << "...");

            // first close the stream
            m_outPracticesFileStream.flush();
            m_outPracticesFileStream.close();

            boost::filesystem::path p(outFilePath);
            std::string fileName = p.filename().string();
            std::string ext = p.extension().string();

            boost::filesystem::path finalFilePath(copyDir);
            finalFilePath /= (fileName);
            std::string finalPathStr = finalFilePath.string();

            if (!boost::filesystem::exists(finalPathStr) || !areFilesIdentical(outFilePath, finalPathStr)) {
                boost::uuids::uuid uuid = boost::uuids::random_generator()();
                const std::string &newFileName = (boost::lexical_cast<std::string>(uuid) + ext);
                boost::filesystem::path targetPath(copyDir);
                targetPath /= newFileName;
                if (!copyFile(outFilePath, targetPath.string())) {
                    otbAppLogCRITICAL("Output file " << outFilePath << " cannot be copied to target dir " << copyDir);
                    return;
                }
                try {
                    // check again if the target file exists and has the exact same content
                    std::string targetPathStr = targetPath.string();
                    if (!boost::filesystem::exists(finalPathStr) || !areFilesIdentical(targetPathStr, finalPathStr)) {
                        boost::filesystem::rename(targetPathStr, finalPathStr);
                    }
                } catch (...) {
                    otbAppLogCRITICAL("Output file " << targetPath << " cannot be copied to final target file " << finalFilePath);
                    return;
                }
            }
        }
    }

    bool areFilesIdentical(const std::string &file1, const std::string &file2) {
          std::ifstream f1(file1, std::ifstream::binary|std::ifstream::ate);
          std::ifstream f2(file2, std::ifstream::binary|std::ifstream::ate);

          if (f1.fail() || f2.fail()) {
            return false; //file problem
          }

          if (f1.tellg() != f2.tellg()) {
            return false; //size mismatch
          }

          //seek back to beginning and use std::equal to compare contents
          f1.seekg(0, std::ifstream::beg);
          f2.seekg(0, std::ifstream::beg);
          return std::equal(std::istreambuf_iterator<char>(f1.rdbuf()),
                            std::istreambuf_iterator<char>(),
                            std::istreambuf_iterator<char>(f2.rdbuf()));
    }

    bool copyFile(const std::string& fileNameFrom, const std::string& fileNameTo) {
        std::ifstream in (fileNameFrom.c_str());
        std::ofstream out (fileNameTo.c_str());
        if (in.is_open() && out.is_open()) {
            out << in.rdbuf();
            out.close();
            in.close();
            return true;
        }
        return false;
    }

private:
    std::string m_year;
    std::map<std::string, int> m_FieldFilters;
    std::map<std::string, int> m_IgnoredIds;

    std::string m_country;
    std::string m_practice;
    std::unique_ptr<CountryInfoBase> m_pCountryInfos;
    std::ofstream m_outPracticesFileStream;

    bool m_bWriteIdsOnlyFile;
    bool m_bFirstFeature;

    std::unique_ptr<GSAAAttributesTablesReaderBase> m_pGSAAAttrsTablesReader;
};

} // end of namespace Wrapper
} // end of namespace otb

OTB_APPLICATION_EXPORT(otb::Wrapper::LPISDataSelection)
