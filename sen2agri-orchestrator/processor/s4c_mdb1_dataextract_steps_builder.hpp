#ifndef S4CMARKERSDB1_EXTRACTOR_BASE_HPP
#define S4CMARKERSDB1_EXTRACTOR_BASE_HPP
#include "processorhandler.hpp"
#include "s4c_utils.hpp"

#define MDB1_PROC_SHORT_NAME "s4c_mdb1"
#define MDB1_CFG_PREFIX     "processor.s4c_mdb1."
#define PARCELS_PRD_KEYS_PREFIX   "general.parcels_product."

typedef struct {
    QString configName;
    QString paramName;
} MetricType;

typedef struct {
    QDateTime productDate;
    QDateTime insertedDate;

    // LPIS informations
    QString productName;
    QString productPath;
    QString opticalIdsGeomShapePath;
    QString sarGeomShapePath;

} LpisInfos;

typedef struct {
    QString marker;
    ProductType prdType;
    QString markerSubstrInFileName;
    // this is used for rasters having multiple bands like old MAJA FRE format
    // In this case, the discrimination info is related to the name of the S2
    // band that will be used from the raster (if correctly mapped, otherwise
    // it will not be possible to extract the markers)
    QString bandDiscriminationInfo;
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
    void Initialize(const QString &parentProc, EventProcessingContext &ctx, const QJsonObject &evtParams,
                    int siteId, int jobId, const QStringList &markersEnabled = {});
    void CreateTasks(const MarkerType &marker, QList<TaskToSubmit> &outAllTasksList, int &curTaskIdx) const;
    void CreateSteps(const MarkerType &marker, QList<TaskToSubmit> &allTasksList, NewStepList &steps,
                     int &curTaskIdx, QStringList &dataExtrDirs) const;
    QList<MarkerType> GetEnabledMarkers() const;
    QDateTime GetDataExtractionMinDate() const;
    QDateTime GetDataExtractionMaxDate() const;
    QString GetDataExtractionDir(const QString &markerName) const;

    static bool HasAnyMarkerEnabled(const ProductType &prdType, const std::map<QString, QString> &cfgParams);

    void SetIdFieldName(const QString &idFieldName) { m_idFieldName = idFieldName; }
    void SetOptParcelsPattern(const QString &pattern) { m_optParcelsPattern = pattern; }
    void SetSarParcelsPattern(const QString &pattern) { m_sarParcelsPattern = pattern; }

private:
    void InitEnabledMarkersDescriptions(const QStringList &markersEnabled);

    QString GetDataExtractionDir(int year, const QString &markerName) const;
    void ExtractProductFiles();
    QStringList GetDataExtractionArgs(const QString &uidField, const PrdMarkerInfo &inputFileInfo, const QString &outDir) const;
    QMap<int, LpisInfos> ExtractLpisInfos();

    bool IsDataExtractionPerformed(const QString &dataExtrDirPath, const QString &prdPath);
    QList<PrdFileInfo> FilterAndUpdateAlreadyProcessingPrds(const QList<PrdFileInfo> &missingPrdsFiles,
                                                            const QList<PrdFileInfo> &processedPrdsFiles,
                                                            const MarkerType &markerType);
    bool IsScheduledJobRequest(const QJsonObject &parameters);
    QMap<int, QList<PrdFileInfo>> GroupProductFileInfosByYear(const QList<PrdFileInfo> &fileInfos);
    void UpdateParcelsPrdDescriptionsFromDB();

private:
    EventProcessingContext *pCtx;
    QJsonObject parameters;
    std::map<QString, QString> configParameters;
    QString parentProcessorName;

    // parameters used for data extraction step
    bool isScheduledJob;

    int siteId;
    int jobId;
    QString siteShortName;

    QDateTime prdMinDate;
    QDateTime prdMaxDate;

    static QList<MetricType> supportedMetrics;
    static QList<MarkerType> allMarkerFileTypes;
    QList<MarkerType> enabledMarkers;
    QList<ProductType> enabledMarkersProductTypes;

    QMap<int, LpisInfos> lpisInfos;
    QList<PrdMarkerInfo> fileInfos;

    QString m_idFieldName;
    QString m_optParcelsPattern;
    QString m_sarParcelsPattern;
};


#endif // S4CMARKERSDB1_EXTRACTOR_BASE_HPP
