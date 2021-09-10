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

class Markers1CsvMerge : public Application
{
public:
    /** Standard class typedefs. */
    typedef Markers1CsvMerge        Self;
    typedef Application                   Superclass;
    typedef itk::SmartPointer<Self>       Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    /** Standard macro */
    itkNewMacro(Self);

    itkTypeMacro(Markers1CsvMerge, otb::Application);

    /** Filters typedef */
    typedef struct {
        int fieldId;
        std::vector<std::string> values;
    } FieldInfo;

    typedef struct FileInfo {
        FileInfo() : curPosInFile(0), curFieldIdx(-1), bIgnore(false), csvSep(',') {

        }
        std::string fileName;
        uintmax_t curPosInFile;
        std::vector<FieldInfo> fieldInfos;
        int curFieldIdx;
        std::vector<std::string> columns;    // the list of columns, ignoring NewID
        bool bIgnore;           // specify if this file should be ignored from now on (nothing to read anymore)
        char csvSep;

        inline void OnNewLinesLoaded(const std::vector<std::string> &lines, int newPosInFile) {
            fieldInfos.clear();
            if(lines.size() == 0) {
                bIgnore = true;
                return;
            }
            curPosInFile = newPosInFile;
            curFieldIdx = 0;

            for (const std::string &line: lines) {
                // extract the NewID field, it is assumed to be on the first column
                const std::vector<std::string> &lineItems = split(line, csvSep);
                if (lineItems.size() != columns.size() + 1) {  // add 1 for NewId which is not in the columns list
                    // ignore this line (or maybe exit with error?)
                    // TODO : give an error here
                    continue;
                }
                FieldInfo fieldInfo;
                fieldInfo.fieldId = atoi(lineItems.front().c_str());
                fieldInfo.values = lineItems;
                fieldInfo.values.erase(fieldInfo.values.begin());
                fieldInfos.push_back(fieldInfo);
            }
            if (fieldInfos.size() == 0) {
                // if all the loaded buffers are invalid, ignore the file
                bIgnore = true;
            }
        }

        inline void OnFieldProcessed() {
            curFieldIdx++;
        }
        inline FieldInfo GetCurrentField() const {
            return fieldInfos[curFieldIdx];
        }
        inline bool IsEmptyBuffer() {
            return (curFieldIdx == -1 || (size_t)curFieldIdx == fieldInfos.size());
        }


    } FileInfo;

    typedef struct
    {
        std::vector<std::string> header;
        std::unordered_map<std::string, int> mapIndexes;

        int GetColumnIndex(const std::string &colName) const {
            std::unordered_map<std::string,int>::const_iterator idx = mapIndexes.find (colName);
            if (idx != mapIndexes.end()) {
                return idx->second;
            }
            return -1;
        }
        inline void AddColumn(const std::string &column) {
            // we do not add this column in the columns
            if (column == SEQ_UNIQUE_ID) {
                return;
            }
            std::vector<std::string>::const_iterator it = std::find (header.begin(), header.end(), column);
            if (it == header.end()) {
                header.push_back(column);
            }
        }
        inline void Sort() {
            std::sort(header.begin(), header.end());
            for(size_t i = 0; i<header.size(); i++) {
                mapIndexes[header[i]] = i;
            }
        }
    } OutputHeader;

    typedef struct OutputLine {
        void Initialize(int nbCols) {
            values.resize(nbCols);
        }
        std::vector<std::string> values;

    } OutputLine;


private:
    Markers1CsvMerge() : m_CsvSeparator(',')
    {
    }

    void DoInit() override
    {
        SetName("Markers1CsvMerge");
        SetDescription("Computes statistics on a training polygon set.");

        // Documentation
        SetDocName("Merges Markers 1 CSV files");
        SetDocLongDescription("");
        SetDocLimitations("None");
        SetDocAuthors("OTB-Team");
        SetDocSeeAlso(" ");

        AddDocTag(Tags::Learning);

        AddParameter(ParameterType_StringList, "il", "Input Images or file containing the list");
        SetParameterDescription("il", "Support images that will be classified or a file containing these images");

        AddParameter(ParameterType_String, "out", "Output file");
        SetParameterDescription("out","Output file ");

        AddParameter(ParameterType_String, "sep", "CSV Separator");
        SetParameterDescription("sep","The CSV separator");
        MandatoryOff("sep");

        AddParameter(ParameterType_Int, "ignnodatecol", "Ignore columns not starting with a date");
        SetParameterDescription("ignnodatecol","gnore columns not starting with a date");
        MandatoryOff("ignnodatecol");
        SetDefaultParameterInt("ignnodatecol", 1);

        AddRAMParameter();

        // Doc example parameter settings
        SetDocExampleParameterValue("in", "support_image.tif");
        SetDocExampleParameterValue("out","/path/to/output/");

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
                m_CsvSeparator = sep[0];
            }
        }

        const std::vector<std::string> &inputFilePaths = GetInputFilePaths();
        if(inputFilePaths.size() == 0) {
            otbAppLogFATAL(<<"No files were given as input!");
        }
        auto start = std::chrono::system_clock::now();

        // Initialize file infos
        std::vector<FileInfo> infoFiles = InitializeFileInfos(inputFilePaths);
        const OutputHeader &header = BuildOutputHeader(infoFiles);
        if (header.header.size() == 0) {
            // do not create the file if header cannot be created
            otbAppLogFATAL(<<"No header was possible to be built from the provided input files!");
        }

        std::ofstream outFileStream;
        std::ofstream indexFileStream;
        const std::string &outFilePath = this->GetParameterString("out");

        CreateOutputStreams(outFilePath, outFileStream, indexFileStream);

        // Write the header to the output file
        uintmax_t curFileIdx = WriteHeader(outFileStream, header);

        std::unordered_map<int, int> mapProcessedFields;
        // We load buffered
        while(true) {
            //auto beforeLoadBuffers = std::chrono::system_clock::now();

            if (LoadBuffers(infoFiles) == 0) {
                break;
            }
            //PrintElapsedTime(beforeLoadBuffers, "Load buffers at: ");
            //auto beforeGetMin = std::chrono::system_clock::now();

            int minFieldId = GetBuffersMinFieldId(infoFiles);
            // all files have ignore flag, so exit
            if (minFieldId == -1) {
                break;
            }
            //PrintElapsedTime(beforeGetMin, "Get min at: ");
            //auto beforeGenLine = std::chrono::system_clock::now();

            // Iterate again and create the line for the current id
            bool bValidLine;
            const OutputLine &newLine = GenerateOutputLine(header, mapProcessedFields, minFieldId, infoFiles, bValidLine);
            mapProcessedFields[minFieldId] = minFieldId;

            //PrintElapsedTime(beforeGenLine, "Gen out line at: ");

            if (bValidLine) {
                // auto beforeWriteLine = std::chrono::system_clock::now();
                WriteLineToOutputFile(outFileStream, indexFileStream, minFieldId, newLine, curFileIdx);
                // PrintElapsedTime(beforeWriteLine, "Write line at: ");
            }
        }
        PrintElapsedTime(start, "All done at ");
    }


    // Read the first line of each file and extract the header and sort them
    OutputHeader BuildOutputHeader(const std::vector<FileInfo> &infoFiles) {
        OutputHeader outHdr;
        for (const FileInfo &fileInfo : infoFiles) {
            for (const std::string &colName: fileInfo.columns) {
                outHdr.AddColumn(colName);
            }
        }
        outHdr.Sort();

        return outHdr;
    }

    std::vector<FileInfo> InitializeFileInfos( const std::vector<std::string> &files)
    {
        // create the buffers but will be empty
        std::vector<FileInfo> fileInfos;
        for (const std::string &filePath: files) {
            // first iterate all files and extract the header line
            uintmax_t newOffset;
            const std::vector<std::string> &lines = GetLinesInFile(filePath, 2, 0, newOffset);
            // check if we have also at least 2 lines, one for header and at least one for data
            if (lines.size() < 2) {
                continue;
            }
            const std::vector<std::string> &colItems = ExtractColumnNames(lines[0]);
            if (colItems.size() == 0) {
                continue;
            }
            // ignore files where the first line after header do not have the same size with the header
            // We avoid in this way eventual columns in the output that will have no data
            const std::vector<std::string> &dataItems = split(lines[1], m_CsvSeparator);
            // In the columns extracted we do not consider the NewID column so we need to subtract 1
            if (colItems.size() != dataItems.size() - 1) {
                continue;
            }
            // skip the header for the future by computing offset after the header otherwise we loose the first line of data
            GetLinesInFile(filePath, 1, 0, newOffset);

            // create the file info and initialize the buffers
            FileInfo fileInfo;
            fileInfo.fileName = filePath;
            fileInfo.columns = colItems;
            fileInfo.curPosInFile = newOffset;
            fileInfo.bIgnore = false;
            fileInfo.csvSep = m_CsvSeparator;
            fileInfos.push_back(fileInfo);
        }
        return fileInfos;
    }

    std::vector<std::string> ExtractColumnNames(const std::string &headerLine) {
        std::vector<std::string> ret;
        const std::vector<std::string> &hdrItems = split(headerLine, m_CsvSeparator);
        for(const std::string &colName: hdrItems) {
            // ignore NewID column, this should be always first
            if (colName == SEQ_UNIQUE_ID) {
                continue;
            }
            if (GetParameterInt("ignnodatecol")) {
                // Get the date from the header item
                time_t ttTime = GetDateFromColumnName(colName);
                if (ttTime == 0) {
                    // ignore the column
                    continue;
                }
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

    // Load new lines in the file info buffers, if needed
    int LoadBuffers(std::vector<FileInfo> &infoFiles) {
        int validBuffers = 0;
        for(FileInfo &infoFile: infoFiles) {
            LoadBuffer(infoFile);
            if (!infoFile.bIgnore) {
                validBuffers++;
            }
        }
        return validBuffers;
    }

    // Iterate all buffers and get the minimum field id
    int GetBuffersMinFieldId(const std::vector<FileInfo> &infoFiles) {
        int minFieldId = -1;
        for(const FileInfo &infoFile: infoFiles) {
            if (infoFile.bIgnore) {
                continue;
            }
            const FieldInfo &fi = infoFile.GetCurrentField();
            if (minFieldId == -1) {
                minFieldId = fi.fieldId;
            } else {
                if (minFieldId > fi.fieldId) {
                    minFieldId = fi.fieldId;
                }
            }
        }
        return minFieldId;
    }

    OutputLine GenerateOutputLine(const OutputHeader &header, const std::unordered_map<int, int> &mapProcessedFields,
                            int minFieldId, std::vector<FileInfo> /*inout*/ &infoFiles,
                            bool &bValidLine) {
        bValidLine = false;
        OutputLine outLine;
        outLine.Initialize(header.header.size());
        for(FileInfo &infoFile: infoFiles) {
            if (infoFile.bIgnore) {
                continue;
            }

            const FieldInfo &fi = infoFile.GetCurrentField();
            if (fi.fieldId == minFieldId) {
                // update it again only if we did not added already.
                // TODO: Should we still consider the latest value?
                if (mapProcessedFields.find(minFieldId) == mapProcessedFields.end()) {
                    // search the columns in the header
                    int curPos = 0;
                    for (const std::string &colName: infoFile.columns) {
                        int colIndex = header.GetColumnIndex(colName);
                        if (colIndex == -1) {
                            // ERROR
                            std::cout<< "Error ... column " << colName
                                     << " not found in the computed header!!!! Exiting ..."
                                     << std::endl;
                            exit(1);
                        }
                        // populate the values
                        outLine.values[colIndex] = fi.values[curPos++];
                        bValidLine = true;
                    }
                }
                // remove at the end the field from the files fields
                infoFile.OnFieldProcessed();
            }
        }
        return outLine;
    }

    void LoadBuffer(FileInfo &fileInfo) {
        // check first if a load is needed
        if (!fileInfo.bIgnore && fileInfo.IsEmptyBuffer()) {
            uintmax_t newPosInFile = -1;
            const std::vector<std::string> &lines = GetLinesInFile(fileInfo.fileName,
                                                                   LOAD_BUFFER_SIZE, fileInfo.curPosInFile,
                                                                   newPosInFile);
            fileInfo.OnNewLinesLoaded(lines, newPosInFile);
        }
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

    int WriteHeader(std::ofstream &fileStream, const OutputHeader &hdr) {
        std::stringstream ss;
        ss << SEQ_UNIQUE_ID << m_CsvSeparator;
        for (size_t i = 0; i<hdr.header.size(); i++) {
            ss << hdr.header[i];
            if (i < hdr.header.size()-1) {
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

    std::vector<std::string> GetInputFilePaths() {
        std::vector<std::string> retFilePaths;
        const std::vector<std::string> &inFilePaths = this->GetParameterStringList("il");
        for (const std::string &inPath: inFilePaths) {
            if ( !boost::filesystem::exists( inPath ) ) {
                otbAppLogWARNING("The provided input path does not exists: " << inPath);
                continue;
            }
            if (boost::filesystem::is_directory(inPath)) {
                boost::filesystem::directory_iterator end_itr;

                boost::filesystem::path dirPath(inPath);
                // cycle through the directory
                for (boost::filesystem::directory_iterator itr(dirPath); itr != end_itr; ++itr) {
                    if (boost::filesystem::is_regular_file(itr->path())) {
                        // assign current file name to current_file and echo it out to the console.
                        boost::filesystem::path pathObj = itr->path();
                        if (pathObj.has_extension()) {
                            std::string fileExt = pathObj.extension().string();
                            // Fetch the extension from path object and return
                            if (boost::iequals(fileExt, ".csv")) {
                                std::string current_file = pathObj.string();
                                if (std::find(retFilePaths.begin(), retFilePaths.end(), current_file) == retFilePaths.end()) {
                                    retFilePaths.push_back(current_file);
                                }
                            }
                        }
                    }
                }
            } else {
                if (std::find(retFilePaths.begin(), retFilePaths.end(), inPath) == retFilePaths.end()) {
                    retFilePaths.push_back(inPath);
                }
            }
        }

        if(retFilePaths.size() == 0) {
            otbAppLogFATAL(<<"No image was given as input!");
        }

        return retFilePaths;
    }

    private:
        char m_CsvSeparator;
};

} // end of namespace Wrapper
} // end of namespace otb

OTB_APPLICATION_EXPORT(otb::Wrapper::Markers1CsvMerge)
