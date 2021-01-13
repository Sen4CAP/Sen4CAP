#ifndef TsaCSVWriter_h
#define TsaCSVWriter_h

#include "TimeSeriesAnalysisTypes.h"
#include <map>

class TsaCSVWriter
{
public:
    TsaCSVWriter();
    static std::string BuildResultsCsvFileName(const std::string &practiceName, const std::string &countryCode, int year);
    bool WriteCSVHeader(const std::string &outDir, const std::string &practiceName, const std::string &countryCode, int year, bool addTillage);
    void WriteHarvestInfoToCsv(const FieldInfoType &fieldInfo, const HarvestEvaluationInfoType &harvestEvalInfo,
                               const EfaEvaluationInfoType &efaHarvestEvalInfo, const TillageEvaluationInfoType &tillageEvalInfo);

private:
    std::string GetResultsCsvFilePath(const std::string &outDir, const std::string &practiceName, const std::string &countryCode, int year);
    std::string TranslateWeekNrDate(const std::string &strHDate, bool isTillage = false);

    std::string GetHWS1Gaps(const FieldInfoType &fieldInfo, const HarvestEvaluationInfoType &harvestEvalInfo);
    std::string GetHQuality(const FieldInfoType &fieldInfo, const HarvestEvaluationInfoType &harvestEvalInfo);
    std::string GetCQuality(const FieldInfoType &fieldInfo, const HarvestEvaluationInfoType &harvestEvalInfo);

private:
    std::ofstream m_OutFileStream;
    std::map<std::string, int> m_IndexPossibleVals;
    std::map<std::string, int> m_HWeekInvalidVals;
    bool m_bAddTillage;
};

#endif
