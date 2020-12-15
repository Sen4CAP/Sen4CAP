#ifndef TimeSeriesAnalysisTypes_h
#define TimeSeriesAnalysisTypes_h

#include <string>
#include <vector>
#include <iostream>
#include <fstream>

#include "CommonFunctions.h"

#define DEFAULT_EFA_NDVI_MIN        300

typedef struct {
    std::string fieldId;

    time_t ttDate;              // needed for sorting the initial values
    std::string strDate;        // TODO: see if really needed, ttDate is used instead to avoid working on strings
    time_t ttDateFloor;
    int weekNo;                 // TODO: to see if is really needed

    double stdDev;
    double meanVal;
    double meanValChange;       // The mean value difference from the previous line

    time_t ttDate2;             // optional - TODO: to see if is really needed
    std::string strDate2;       // optional (TODO: to see if is really needed, ttDate2 is used instead to avoid working on strings)
    time_t ttDate2Floor;        // optional
    int weekNo2;                // optional - TODO: to see if is really needed

    void Reset() {
        fieldId.clear();
        ttDate = 0;
        strDate.clear();
        ttDateFloor = 0;
        weekNo = 0;
        stdDev = 0;
        meanVal = 0;
        meanValChange = 0;
        ttDate2 = 0;
        strDate2.clear();
        ttDate2Floor = 0;
        weekNo2 = 0;
    }

    double GetValue() const {
        return meanVal;
    }

    bool GetChangeValue(double &retVal) const {
        retVal = meanValChange;
        return true;
    }

    time_t GetFloorTime() const {
        return ttDateFloor;
    }

} InputFileLineInfoType;

typedef struct MergedDateAmplitude {
    InputFileLineInfoType vvInfo;
    InputFileLineInfoType vhInfo;
    double ampRatio;
    time_t ttDate;


    double GetValue() const {
        return ampRatio;
    }

    bool GetChangeValue(double &retVal) const {
        retVal = 0.0;
        return false;
    }

    time_t GetFloorTime() const {
        return vvInfo.ttDateFloor;
    }
} MergedDateAmplitudeType;

typedef struct {
    time_t ttDate;                  // current timestamp
    double meanVal;                 // mean value
    double ampChange;               // difference from with the previous 3 weeks mean ratio - amplitude only
    double maxVal;                  // maximum value in a 7 days interval                   - coherence only
    double maxChangeVal;            // maximum difference from the previous value           - coherence only
    double sum;                     // intermediate for computing mean val - can be done outside
    size_t cnt;                     // intermediate for computing mean val - can be done outside
} GroupedMeanValInfosType;

typedef struct MergedAllValInfosType {
    MergedAllValInfosType() {
        ttDate = 0;
        cohMax = NOT_AVAILABLE;
        cohChange = NOT_AVAILABLE;

        ampMean = NOT_AVAILABLE;
        ampMax = NOT_AVAILABLE;
        ampChange = NOT_AVAILABLE;

        ndviMeanVal = NOT_AVAILABLE;
        ndviPrev = NOT_AVAILABLE;
        ndviNext = NOT_AVAILABLE;
        vegWeeks = false;
        ndviPresence = NOT_AVAILABLE;
        ndviDrop = NOT_AVAILABLE;
        vegWeeks = NOT_AVAILABLE;
        candidateOptical = false;

        candidateCoherence = false;
        candidateAmplitude = false;
        amplitudePresence = false;
        harvest = false;
        candidateEfa = false;
        catchStart = false;

    }
    time_t ttDate;

    double cohMax;                  // coherence max value in week week
    double cohChange;               // coherence change from the previous value

    double ampMean;                 // amplitude week mean
    double ampMax;                  // amplitude max value in week
    double ampChange;               // ammplitude ration difference from with the previous 3 weeks mean

    double ndviMeanVal;
    double ndviPrev;
    double ndviNext;
    short vegWeeks;                // TODO: I think this could be converted to boolean with only TRUE/FALSE values TBC
    short ndviPresence;
    double ndviDrop;

    bool candidateOptical;

    bool candidateCoherence;

    bool candidateAmplitude;
    bool amplitudePresence;

    bool harvest;

    bool candidateEfa;
    bool catchStart;

} MergedAllValInfosType;

template <typename T>
struct TimedValInfosComparator
{
    inline bool operator() (const T& struct1, const T& struct2)
    {
        return (struct1.ttDate < struct2.ttDate);
    }
};




typedef struct FieldInfoType {

    FieldInfoType(const std::string &fid) {
        fieldId = fid;

        ttVegStartTime = 0;
        ttVegStartWeekFloorTime = 0;
        ttHarvestStartTime = 0;
        ttHarvestStartWeekFloorTime = 0;
        ttHarvestEndTime = 0;
        ttHarvestEndWeekFloorTime = 0;
        ttPracticeStartTime = 0;
        ttPracticeStartWeekFloorTime = 0;
        ttPracticeEndTime = 0;
        ttPracticeEndWeekFloorTime = 0;
        vegStartWeekNo = NOT_AVAILABLE;
        harvestStartWeekNo = NOT_AVAILABLE;
        coheVVMaxValue = NOT_AVAILABLE;
        gapsInfos = NOT_AVAILABLE;
        hS1GapsInfos = NOT_AVAILABLE;
        h_W_S1GapsInfos = NOT_AVAILABLE;
        pS1GapsInfos = NOT_AVAILABLE;
    }
    std::string fieldId;
    std::string fieldSeqId;
    std::string countryCode;
    std::string mainCrop;
    std::string practiceType;
    std::string s1PixValue;
    int gapsInfos;
    int hS1GapsInfos;
    int h_W_S1GapsInfos;
    int pS1GapsInfos;

    int year;                                   // TODO: Check that it is extracted from the YEAR field of the shape
    time_t ttVegStartTime;
    time_t ttVegStartWeekFloorTime;
    time_t ttHarvestStartTime;
    time_t ttHarvestStartWeekFloorTime;
    time_t ttHarvestEndTime;
    time_t ttHarvestEndWeekFloorTime;

    std::string practiceName;
    time_t ttPracticeStartTime;
    time_t ttPracticeStartWeekFloorTime;
    time_t ttPracticeEndTime;
    time_t ttPracticeEndWeekFloorTime;

    int vegStartWeekNo;
    int harvestStartWeekNo;

    std::vector<InputFileLineInfoType> ampVVLines;
    std::vector<InputFileLineInfoType> ampVHLines;
    std::vector<MergedDateAmplitudeType> mergedAmpInfos;

    std::vector<InputFileLineInfoType> coheVVLines;
    //std::vector<InputFileLineInfoType> coheVHLines;     // TODO: These are read but not actually used anywhere. To be removed (???)
    double coheVVMaxValue;                              // Needed for marker 5

    std::vector<InputFileLineInfoType> ndviLines;

    std::vector<GroupedMeanValInfosType> ampRatioGroups;
    std::vector<GroupedMeanValInfosType> ndviGroups;
    std::vector<GroupedMeanValInfosType> coherenceGroups;

} FieldInfoType;

typedef struct HarvestEvaluationInfoType {
    HarvestEvaluationInfoType(int NAInitVal = NOT_AVAILABLE) {
        int naInitVal = NAInitVal;
        fieldId = NA_STR;
        year = NOT_AVAILABLE;
        mainCrop = NOT_AVAILABLE;
        ttVegStartTime = 0;
        ttHarvestStartTime = 0;
        ttHarvestEndTime = 0;

        ndviPresence = naInitVal;               // M1
        candidateOptical = naInitVal;           // M2
        candidateAmplitude = naInitVal;         // M3
        amplitudePresence = naInitVal;          // M4
        candidateCoherence = naInitVal;         // M5
        harvestConfirmWeek = naInitVal;         // WEEK
        ttHarvestConfirmWeekStart = naInitVal;  //  WEEK start as date
        ttS1HarvestWeekStart = naInitVal;
        ttLWeekStartTime = naInitVal;
    }

    void Initialize(const FieldInfoType &fieldInfos) {
        fieldId = fieldInfos.fieldId;
        year = fieldInfos.year;
        ttVegStartTime = fieldInfos.ttVegStartTime;
        ttHarvestStartTime = fieldInfos.ttHarvestStartTime;
        ttHarvestEndTime = fieldInfos.ttHarvestEndTime;
    }

    std::string fieldId;
    int year;
    int mainCrop;
    time_t ttVegStartTime;
    time_t ttHarvestStartTime;
    time_t ttHarvestEndTime;

    short ndviPresence;             // M1
    short candidateOptical;          // M2
    short candidateAmplitude;        // M3
    short amplitudePresence;         // M4
    short candidateCoherence;        // M5
    short harvestConfirmWeek;       // WEEK
    time_t ttHarvestConfirmWeekStart; // WEEK start as date

    time_t ttLWeekStartTime;
    time_t ttS1HarvestWeekStart;    // S1 Harvest Week Start

} HarvestEvaluationInfoType;

typedef struct EfaEvaluationInfoType {
    EfaEvaluationInfoType(int naInitVal = NOT_AVAILABLE) {
        isValid = false;
        ttPracticeStartTime = 0;
        ttPracticeEndTime = 0;

        efaIndex = ValueToString(naInitVal); // C_INDEX
        ndviPresence = naInitVal;               // M6
        ndviGrowth = naInitVal;                 // M7
        ndviNoLoss = naInitVal;                 // M8
        ampNoLoss = naInitVal;                  // M9
        cohNoLoss = naInitVal;                  // M10
    }
    void Initialize(const FieldInfoType &fieldInfos) {
        ttPracticeStartTime = fieldInfos.ttPracticeStartTime;
        ttPracticeEndTime = fieldInfos.ttPracticeEndTime;
    }
    void SetValid(bool valid) {isValid = valid;}

    bool isValid;
    short ndviPresence;             // M6
    short ndviGrowth;               // M7
    double ndviNoLoss;              // M8
    double ampNoLoss;               // M9
    double cohNoLoss;               // M10
    std::string efaIndex;           // C_INDEX

    time_t ttPracticeStartTime;
    time_t ttPracticeEndTime;

} EfaEvaluationInfoType;

typedef struct TillageEvaluationInfoType {
    TillageEvaluationInfoType(int naInitVal = NOT_AVAILABLE) {
        isValid = false;
        tillageConfirmWeek = naInitVal;         // WEEK
        ttTillageConfirmWeekStart = naInitVal;          //  WEEK start as date
    }

    void SetValid(bool valid) {isValid = valid;}

    bool isValid;
    short tillageConfirmWeek;       // WEEK
    time_t ttTillageConfirmWeekStart; // WEEK start as date
} TillageEvaluationInfoType;

typedef struct {
    time_t ttDate;
    short ndviPresence;
    short ndviDrop;
    double ndviMean;
    short ndviGrowth;
    short ndviNoLoss;
    short ampNoLoss;
    short cohNoLoss;

} EfaMarkersInfoType;

#endif
