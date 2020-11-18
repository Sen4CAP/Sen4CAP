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
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string.hpp>
#include <unordered_map>
#include "CommonFunctions.h"
#include <cstdint>

#include <chrono>
#include <ctime>

#define DATE_IDX_IN_COL     0
#define LOAD_BUFFER_SIZE    10000

namespace otb
{
namespace Wrapper
{

class Markers2Extractor : public Application
{
public:
    /** Standard class typedefs. */
    typedef Markers2Extractor        Self;
    typedef Application                   Superclass;
    typedef itk::SmartPointer<Self>       Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    /** Standard macro */
    itkNewMacro(Self);

    itkTypeMacro(Markers2Extractor, otb::Application);

    /** Filters typedef */
    typedef struct {
        int fieldId;
        std::vector<std::string> values;
    } FieldInfo;

    typedef struct {
        int vvSrcIndex;
        int vhSrcIndex;
        std::string colName;    // out column name
    } OutColInfo;

    typedef struct OutputLine {
        OutputLine() {}
        OutputLine(int nbCols) : values(nbCols) {}
        std::vector<std::string> values;

    } OutputLine;


private:
    Markers2Extractor() : m_CsvSeparator(',')
    {
    }

    void DoInit() override
    {
        SetName("Markers2Extractor");
        SetDescription("Extracts the Marker 2 CSV file.");

        // Documentation
        SetDocName("Markers 2 CSV extractor");
        SetDocLongDescription("");
        SetDocLimitations("None");
        SetDocAuthors("OTB-Team");
        SetDocSeeAlso(" ");

        AddDocTag(Tags::Learning);

        AddParameter(ParameterType_String, "in", "Input csv files containing P1 markers");
        SetParameterDescription("in", "Input csv files containing P1 markers");

        AddParameter(ParameterType_String, "out", "Output file");
        SetParameterDescription("out","Output file ");

        AddParameter(ParameterType_String, "sep", "CSV Separator");
        SetParameterDescription("sep","The CSV separator");
        MandatoryOff("sep");

        AddRAMParameter();

        // Doc example parameter settings
        SetDocExampleParameterValue("in", "input.csv");
        SetDocExampleParameterValue("out","/path/to/output/output.csv");
    }

    void DoUpdateParameters() override
    {
    }

    void DoExecute() override
    {
        if (HasValue("sep")) {
            const std::string &sep = GetParameterAsString("sep");
            if (sep.length() > 0) {
                m_CsvSeparator = sep[0];
            }
        }
        const std::string &inFilePath = this->GetParameterString("in");
        if ( !boost::filesystem::exists( inFilePath ) ) {
            otbAppLogFATAL("The provided input path does not exists: " << inFilePath);
        }

        auto start = std::chrono::system_clock::now();

        const std::string &outFilePath = this->GetParameterString("out");

        std::ofstream outFileStream;
        std::ofstream indexFileStream;
        CreateOutputStreams(outFilePath, outFileStream, indexFileStream);

        // Initialize file infos
        otbAppLogINFO("Starting processing input file: " << inFilePath);
        InitializeFileInfos(inFilePath);
        const std::vector<OutColInfo> &outputHeader = BuildOutputHeader();
        if (outputHeader.size() == 0) {
            otbAppLogFATAL("No output markers columns were generated for file: " << inFilePath);
        }

        // Write the header to the output file
        uintmax_t curFileIdx = WriteHeader(outFileStream, outputHeader);
        otbAppLogINFO("Header written for output file: " << outFilePath);
        bool bValidLine;
        int fieldId;
        // We load buffered
        while(true) {
//            auto beforeLoadBuffers = std::chrono::system_clock::now();

            if (!LoadBuffer()) {
                break;
            }
//            PrintElapsedTime(beforeLoadBuffers, "Get min at: ");
//            auto beforeGenLine = std::chrono::system_clock::now();

            // Extract output line for the current buffer line
            const OutputLine &newLine = GenerateOutputLine(outputHeader, fieldId, bValidLine);
//            PrintElapsedTime(beforeGenLine, "Gen out line at: ");

            if (bValidLine) {
//                auto beforeWriteLine = std::chrono::system_clock::now();
                WriteLineToOutputFile(outFileStream, indexFileStream, fieldId, newLine, curFileIdx);
//                PrintElapsedTime(beforeWriteLine, "Write line at: ");
            }
        }
        PrintElapsedTime(start, "All done at ");
    }


    // Read the first line of each file and extract the header and sort them
    std::vector<OutColInfo> BuildOutputHeader() {
        std::vector<OutColInfo> outColumns;
        int i = 0;
        for (const std::string &colName: m_columns) {
            // for all the VV columns, check that we have also the VH
            // For these, build the VV_VH column
            size_t pos = colName.find("_VV_");
            if (pos != std::string::npos) {
                if (colName.find("_AMP_") != std::string::npos &&
                    (colName.find("_stdev_") != std::string::npos ||
                    colName.find("_mean_") != std::string::npos)) {
                    std::string vhColName = colName;
                    vhColName.replace(pos, 4, "_VH_");
                    std::vector<std::string>::iterator it = std::find (m_columns.begin(), m_columns.end(), vhColName);
                    if (it != m_columns.end()) {
                        // found the corresponding VH
                        int vhPos = std::distance(m_columns.begin(), it);
                        OutColInfo outColInfo;
                        outColInfo.vvSrcIndex = i;
                        outColInfo.vhSrcIndex = vhPos;
                        outColInfo.colName = vhColName.replace(pos, 4, "_VVVH_");
                        outColumns.push_back(outColInfo);
                    }
                }
            }
            i++;
        }

        return outColumns;
    }

    void InitializeFileInfos( const std::string &filePath)
    {
        // create the buffers but will be empty
        uintmax_t newOffset;
        const std::vector<std::string> &lines = GetLinesInFile(filePath, 1, 0, newOffset);
        // check if we have also at least 2 lines, one for header and at least one for data
        if (lines.size() < 1) {
            otbAppLogFATAL("The provided input file is empty: " << filePath);
        }
        const std::vector<std::string> &colItems = ExtractColumnNames(lines[0]);
        if (colItems.size() == 0) {
             otbAppLogFATAL("The first line of the provided input file is empty: " << filePath);
        }

        // create the file info and initialize the buffers
        m_inFilePath = filePath;
        m_columns = colItems;
        m_curPosInFile = newOffset;
    }

    std::vector<std::string> ExtractColumnNames(const std::string &headerLine) {
        std::vector<std::string> ret;
        const std::vector<std::string> &hdrItems = split(headerLine, m_CsvSeparator);
        for(const std::string &colName: hdrItems) {
            // ignore NewID column, this should be always first
            if (colName == SEQ_UNIQUE_ID) {
                continue;
            }
            // Get the date from the header item
            time_t ttTime = GetDateFromColumnName(colName);
            if (ttTime == 0) {
                // ignore the column
                continue;
            }
            ret.push_back(colName);
        }
        return ret;
    }

    time_t GetDateFromColumnName(const std::string &colName) {
        const std::vector<std::string> &items = split(colName, '_');
        if (DATE_IDX_IN_COL < items.size()) {
            return to_time_t(boost::gregorian::from_undelimited_string(items[DATE_IDX_IN_COL]));
        }
        return 0;
    }

    // Iterate all buffers and get the minimum field id

    OutputLine GenerateOutputLine(const std::vector<OutColInfo> &header, int &fieldId, bool &bValid) {
        float vvVal, vhVal, retVal;
        OutputLine outLine(header.size());
        const FieldInfo &fi = GetCurrentField();
        fieldId = fi.fieldId;
        bValid = false;
        int i = 0;
        for (const OutColInfo &outColInfo: header) {
            const std::string &vvSrcVal = fi.values[outColInfo.vvSrcIndex];
            const std::string &vhSrcVal = fi.values[outColInfo.vhSrcIndex];
            if (vvSrcVal.size() > 0 && vhSrcVal.size() > 0) {
                vvVal = std::atof(vvSrcVal.c_str());
                vhVal = std::atof(vhSrcVal.c_str());
                retVal = vvVal - vhVal;
                outLine.values[i] = boost::lexical_cast<std::string>(retVal);
                bValid = true;
            }
            i++;
        }

        OnFieldProcessed();

        return outLine;
    }

    bool LoadBuffer() {
        // check first if a load is needed
        if (IsEmptyBuffer()) {
            uintmax_t newPosInFile = -1;
            const std::vector<std::string> &lines = GetLinesInFile(m_inFilePath,
                                                                   LOAD_BUFFER_SIZE, m_curPosInFile,
                                                                   newPosInFile);
            if(lines.size() == 0) {
                return false;
            }
            OnNewLinesLoaded(lines);
            m_curPosInFile = newPosInFile;
        }
        return true;
    }

    std::vector<std::string> GetLinesInFile(const std::string &filePath, int nLinesToExtract,
                                            uintmax_t offetInFile, uintmax_t &newOffsetInFile)
    {
        std::vector<std::string> ret;
        FILE* fp = fopen(filePath.c_str(), "r");
        if (fp == NULL) {
            return ret;
        }

        // search the pos in file
        fseek(fp, offetInFile, SEEK_SET);

        char* line = NULL;
        size_t len = 0;
        int readLines = 0;
        while ((readLines++ < nLinesToExtract) && (getline(&line, &len, fp) != -1)) {
            std::string lineStr(line);
            boost::trim(lineStr);
            if (lineStr.size() > 0) {
                ret.push_back(lineStr);
            }
        }
        newOffsetInFile = ftell(fp);
        fclose(fp);
        if (line) {
            free(line);
        }
        return ret;
    }

    void CreateOutputStreams(const std::string &outFilePath, std::ofstream &outFileStream, std::ofstream &indexFileStream) {
        std::string outIdxPath;
        boost::filesystem::path path(outFilePath);
        outIdxPath = (path.parent_path() / path.filename()).string() + ".idx";
        indexFileStream.open(outIdxPath, std::ios_base::trunc | std::ios_base::out);

        otbAppLogINFO("Writing results to file " << outFilePath);

        outFileStream.open(outFilePath, std::ios_base::trunc | std::ios_base::out);
    }

    int WriteHeader(std::ofstream &fileStream, const std::vector<OutColInfo> &hdr) {
        std::stringstream ss;
        ss << SEQ_UNIQUE_ID << m_CsvSeparator;
        for (size_t i = 0; i<hdr.size(); i++) {
            ss << hdr[i].colName;
            if (i < hdr.size()-1) {
                ss << m_CsvSeparator;
            }
        }
        ss << "\n";
        const std::string &ssStr = ss.str();
        fileStream << ssStr.c_str();
        return ssStr.size();
    }

    void  WriteLineToOutputFile(std::ofstream &outStream, std::ofstream &outIdxStream,
                                int fieldId, const OutputLine &line, uintmax_t &curFileIdx)
    {
        std::stringstream ss;

        ss << fieldId << m_CsvSeparator;
        for (size_t i = 0; i<line.values.size(); i++) {
            ss << line.values[i];
            if (i < line.values.size()-1) {
                ss << m_CsvSeparator;
            }
        }
        ss << "\n";
        const std::string &ssStr = ss.str();
        size_t byteToWrite = ssStr.size();
        if (outIdxStream.is_open()) {
            outIdxStream << fieldId << m_CsvSeparator << curFileIdx << m_CsvSeparator << byteToWrite <<"\n";
        }
        curFileIdx += byteToWrite;
        outStream << ssStr.c_str();
     }

    void PrintElapsedTime(const std::chrono::system_clock::time_point &start, const char *msg) {
        const std::chrono::system_clock::time_point &end = std::chrono::system_clock::now();

        std::chrono::duration<double> elapsed_seconds = end-start;
        std::time_t end_time = std::chrono::system_clock::to_time_t(end);
        char mbstr[30];
        std::strftime(mbstr, sizeof(mbstr), "%Y-%m-%dT%H:%M:%S", std::localtime(&end_time));


        std::cout << std::string(msg) << std::string(mbstr)
                  << ". Elapsed time: " << elapsed_seconds.count() << " s\n";
    }

    inline void OnNewLinesLoaded(const std::vector<std::string> &lines) {
        m_fieldInfos.clear();
        m_curLineInBuffer = 0;

        for (const std::string &line: lines) {
            // extract the NewID field, it is assumed to be on the first column
            std::vector<std::string> lineItems;
            boost::split(lineItems, line, [this](char c){return c == m_CsvSeparator;});
            if (lineItems.size() != m_columns.size() + 1) {  // add 1 for NewId which is not in the columns list
                // ignore this line (or maybe exit with error?)
                // TODO : give an error here
                otbAppLogWARNING("Line does not has header size: " << line);
                continue;
            }
            FieldInfo fieldInfo;
            fieldInfo.fieldId = atoi(lineItems.front().c_str());
            fieldInfo.values = lineItems;
            fieldInfo.values.erase(fieldInfo.values.begin());
            m_fieldInfos.push_back(fieldInfo);
        }
        // otbAppLogINFO("Loaded a number of " << m_fieldInfos.size() << " lines");
    }

    void OnFieldProcessed() {
        m_curLineInBuffer++;
        // otbAppLogINFO("Processed from buffer " << m_curLineInBuffer << ". Remaining lines " << m_fieldInfos.size() - m_curLineInBuffer);
    }
    FieldInfo GetCurrentField() const {
        return m_fieldInfos[m_curLineInBuffer];
    }
    bool IsEmptyBuffer() {
        return (m_curLineInBuffer == -1 || (size_t)m_curLineInBuffer == m_fieldInfos.size() || m_fieldInfos.size() == 0);
    }


    private:
        char m_CsvSeparator;
        std::string m_inFilePath;
        std::vector<std::string> m_columns;     // the list of columns, ignoring NewID
        uintmax_t m_curPosInFile;
        std::vector<FieldInfo> m_fieldInfos;
        int m_curLineInBuffer;

    //TODO
};

} // end of namespace Wrapper
} // end of namespace otb

OTB_APPLICATION_EXPORT(otb::Wrapper::Markers2Extractor)
