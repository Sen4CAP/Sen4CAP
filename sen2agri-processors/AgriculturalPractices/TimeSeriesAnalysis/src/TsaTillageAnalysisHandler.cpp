#include "TsaTillageAnalysisHandler.h"

#include "TsaHelperFunctions.h"

TsaTillageAnalysisHandler::TsaTillageAnalysisHandler()
{
    m_tlMonitEndDate = 0;
}

bool TsaTillageAnalysisHandler::PerformAnalysis(const FieldInfoType &fieldInfos, std::vector<MergedAllValInfosType> &retAllMergedValues,
                             HarvestEvaluationInfoType &harvestEvalInfos, TillageEvaluationInfoType &tillageEvalInfo)
{
    // if we have harvest detected, then we can proceed with the tillage detection
    if (IsNA(harvestEvalInfos.harvestConfirmWeek)) {
        return false;
    }

    int tlMonitEndDate = m_tlMonitEndDate;
    if (tlMonitEndDate == 0) {
        tlMonitEndDate = retAllMergedValues[retAllMergedValues.size() - 1].ttDate;
    }

    // # to avoid gap in vegseason.start week
    int minTillageStartDateIdx = -1;
    for(size_t i = 0; i<retAllMergedValues.size(); i++) {
        if (retAllMergedValues[i].ttDate == harvestEvalInfos.ttHarvestConfirmWeekStart) {
            // get the first date greater than the harvest confirm week
            if (minTillageStartDateIdx == -1) {
                minTillageStartDateIdx = i;
                // reset also all the ndviPresence flags. We are resetting only for the ones in the interval of interest
                for (size_t j = i; j < retAllMergedValues.size() && retAllMergedValues[j].ttDate <= tlMonitEndDate; j++) {
                    retAllMergedValues[j].ndviPresence = false;
                }
                break;
            }
        }
    }

    double CohThrAbs = m_CohThrAbs;
    if (IsLess(fieldInfos.coheVVMaxValue, m_CohThrAbs)) {
        CohThrAbs = fieldInfos.coheVVMaxValue - m_CohThrBase;
    }

    bool coherenceBase, coherencePresence;
    // check each week for the NDVI presence and for the M5
    for(size_t i = minTillageStartDateIdx; i<retAllMergedValues.size(); i++) {
        if (retAllMergedValues[i].ttDate <= tlMonitEndDate) {
            if (IsNA(retAllMergedValues[i].ndviMeanVal)) {
                retAllMergedValues[i].ndviPresence = false;
            } else if (IsGreater(retAllMergedValues[i].ndviMeanVal, m_OpticalThrVegCycle)) {
                tillageEvalInfo.tillageConfirmWeek = NR;
                tillageEvalInfo.ttTillageConfirmWeekStart = NR;
                break;  // start of vegetation growth after the harvest of the main crop, tillage is not detected
            } else {
                retAllMergedValues[i].ndviPresence = false;
            }

            // check M5 conditions
            coherenceBase = IsGreaterOrEqual(retAllMergedValues[i].cohChange, m_CohThrBase);
            coherencePresence = IsGreaterOrEqual(retAllMergedValues[i].cohMax, CohThrAbs);
            retAllMergedValues[i].candidateCoherence = (coherenceBase || coherencePresence);
            if ((int)i > minTillageStartDateIdx) {
                // M1=False OR M1 is not set AND M5(previous week)=True AND M5(current week)=False
                if (retAllMergedValues[i-1].candidateCoherence && !retAllMergedValues[i].candidateCoherence) {
                    tillageEvalInfo.tillageConfirmWeek = GetWeekFromDate(retAllMergedValues[i].ttDate);
                    tillageEvalInfo.ttTillageConfirmWeekStart = retAllMergedValues[i].ttDate;
                    break;
                }
            }
        }
    }
    return true;
}
