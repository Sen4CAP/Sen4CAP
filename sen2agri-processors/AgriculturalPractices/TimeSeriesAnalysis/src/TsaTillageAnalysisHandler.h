#ifndef TsaTillageAnalysisHandler_H
#define TsaTillageAnalysisHandler_H

#include "TimeSeriesAnalysisTypes.h"
#include "TimeSeriesAnalysisUtils.h"
#include "TsaHandlerBase.h"

class TsaTillageAnalysisHandler : public TsaHandlerBase
{
public:
    TsaTillageAnalysisHandler();

    void SetTillageMonitoringEndDate(const std::string &val) { m_tlMonitEndDate = GetTimeFromString(val); }
    void SetOpticalThrVegCycle(double val) { m_OpticalThrVegCycle= val; }
    void SetCohThrBase(double val) { m_CohThrBase = val; }
    void SetCohThrAbs(double val) { m_CohThrAbs = val; }

    bool PerformAnalysis(const FieldInfoType &fieldInfos, std::vector<MergedAllValInfosType> &retAllMergedValues,
                                 HarvestEvaluationInfoType &harvestEvalInfos, TillageEvaluationInfoType &tillageEvalInfo);

private:
    time_t m_tlMonitEndDate;
    double m_OpticalThrVegCycle;
    double m_CohThrBase;
    double m_CohThrAbs;
};

#endif
