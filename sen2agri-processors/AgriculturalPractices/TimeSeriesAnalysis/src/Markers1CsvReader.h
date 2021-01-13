#ifndef Markers1CsvReader_h
#define Markers1CsvReader_h

#include "StatisticsInfosReaderBase.h"
#include <inttypes.h>

class Markers1CsvReader : public StatisticsInfosReaderBase
{

    typedef struct {
        uintmax_t startIdx;
        unsigned int len;
    } FieldIndexInfos;

    typedef std::map<std::string, std::vector<FieldIndexInfos>> IdxMapType;

    typedef struct StdDevMeanColColIdxs {
        bool isMeanCol;
        int meanColIdx;
        int stdDevColIdx;
    } StdDevMeanColColIdxs;

    typedef struct ColumnInfo {
        time_t ttDate;
        std::string strDateSeparators;
        int weekNo;
        time_t ttDateFloor;
        int yearNo;
        std::string paramName;
        std::string prdType;
        std::string polarisation;
        std::string orbit;

        std::string fullColName;
        bool bIgnoreColumn;

        // mapping for the stddev and mean indexes in the header.
        // This is useful for fast identifying the columns where the two are located
        StdDevMeanColColIdxs stdDevMeanColMap;

    } ColumnInfo;

public:
    Markers1CsvReader();

   virtual  ~Markers1CsvReader()
    {
    }
    virtual void Initialize(const std::string &source, const std::vector<std::string> &filters, int year);
    virtual std::string GetName() { return "mcsv"; }

    virtual bool GetEntriesForField(const std::string &fid, const std::vector<std::string> &filters,
                            std::map<std::string, std::vector<InputFileLineInfoType>> &retMap);


private:
    bool ExtractHeaderInfos(const std::string &hdrLine, int year);
    bool LoadIndexFile(const std::string &source);
    std::vector<std::string> GetInputFileLineElements(const std::string &line);
    bool ExtractLinesFromStream(std::istream &inStream, const std::string &fieldId,
                                const std::vector<std::string> &findFilters,
                                std::map<std::string, std::vector<InputFileLineInfoType>> &retMap);
    bool ExtractInfosFromLine(const std::string &fileLine, const std::vector<std::string> &findFilters,
                              std::map<std::string, std::vector<InputFileLineInfoType> > &lineInfos);

private:
    std::vector<ColumnInfo> m_header;

    std::string m_strSource;
    IdxMapType m_IdxMap;
    std::string m_inDateFormat;
    char m_separator;
};

#endif
