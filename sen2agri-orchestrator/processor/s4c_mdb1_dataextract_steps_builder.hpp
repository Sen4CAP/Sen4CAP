#ifndef S4CMARKERSDB1_EXTRACTOR_BASE_HPP
#define S4CMARKERSDB1_EXTRACTOR_BASE_HPP
#include "processorhandler.hpp"
#include "s4c_utils.hpp"

#define MDB1_PROC_SHORT_NAME "s4c_mdb1"
#define MDB1_CFG_PREFIX     "processor.s4c_mdb1."
#define LPIS_PATH_CFG_KEY   "processor.lpis.path"

typedef struct {
    QDateTime productDate;
    QDateTime insertedDate;

    // LPIS informations
    QString fullDeclsFilePath;
    QString ndviIdsGeomShapePath;
    QString ampCoheIdsGeomShapePath;

} LpisInfos;

typedef struct {
    QString marker;
    ProductType prdType;
    QString markerSubstrInFileName;
} MarkerType;

typedef struct {
    QString inFilePath;
    QDateTime prdTime;
} PrdFileInfo;

typedef struct {
    MarkerType markerInfo;
    PrdFileInfo prdFileInfo;
} PrdMarkerInfo;

class S4CMarkersDB1DataExtractStepsBuilder
{
public:
    S4CMarkersDB1DataExtractStepsBuilder();
    void Initialize(EventProcessingContext &ctx, const JobSubmittedEvent &evt, const QStringList &markersEnabled = {});
    void CreateTasks(const MarkerType &marker, QList<TaskToSubmit> &outAllTasksList, int &curTaskIdx) const;
    void CreateSteps(const MarkerType &marker, QList<TaskToSubmit> &allTasksList, NewStepList &steps, int &curTaskIdx, QStringList &dataExtrDirs) const;
    QList<MarkerType> GetEnabledMarkers() const;
    void GetDataExtractionInterval(QDateTime &minDate, QDateTime &maxDate) const;
    QString GetDataExtractionDir(const QString &markerName) const;

private:
    QString GetDataExtractionDir(int year, const QString &markerName) const;
    QDateTime ExtractDateFromRegex(const QString &fileName, const QString &regex, int minDateGrpIdx, int maxDateGrpIdx = -1);
    void ExtractProductFiles();
    void RemoveNoLpisProducts();
    QStringList GetDataExtractionArgs(const QString &uidField, const PrdMarkerInfo &inputFileInfo, const QString &outDir) const;
    ProductList GetLpisProduct(ExecutionContextBase *pCtx, int siteId);
    QMap<int, LpisInfos> ExtractLpisInfos();

    QString GetShortNameForProductType(const ProductType &prdType);
    int UpdateJobSubmittedParamsFromSchedReq();
    QList<PrdFileInfo> ExtractMissingDataExtractionProducts(const MarkerType &markerType, const QDateTime &startDate,
                                                     const QDateTime &endDate, QList<PrdFileInfo> &alreadyProcessedFiles);
    bool IsDataExtractionPerformed(const QString &dataExtrDirPath, const QString &prdPath);
    QList<PrdFileInfo> FilterAndUpdateAlreadyProcessingPrds(const QList<PrdFileInfo> &missingPrdsFiles, const QList<PrdFileInfo> &processedPrdsFiles, const MarkerType &markerType);
    void AddProductListToJSonArray(const QList<PrdFileInfo> &prdList, QJsonArray &retArr);
    bool IsScheduledJobRequest(const QJsonObject &parameters);

//    bool CheckExecutionPreconditions(ExecutionContextBase *pCtx, const std::map<QString, QString> &configParameters, int siteId,
//                                        const QString &siteShortName, QString &errMsg);
    QMap<int, QList<PrdFileInfo>> GroupProductFileInfosByYear(const QList<PrdFileInfo> &fileInfos);

private:
    EventProcessingContext *pCtx;
    JobSubmittedEvent event;
    QJsonObject parameters;
    std::map<QString, QString> configParameters;

    // parameters used for data extraction step
    bool isScheduledJob;

    int siteId;
    QString siteShortName;

    QDateTime seasonStartDate;
    QDateTime seasonEndDate;
    QDateTime prdMinDate;
    QDateTime prdMaxDate;

    QList<MarkerType> allMarkerFileTypes;
    QList<MarkerType> enabledMarkers;
    bool bHasL3BMarkers;

    QMap<int, LpisInfos> lpisInfos;
    QList<PrdMarkerInfo> fileInfos;

};


#endif // S4CMARKERSDB1_EXTRACTOR_BASE_HPP
