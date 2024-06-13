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

typedef struct ParcelsProductDescriptor
{
    QString m_idFieldName;
    QString m_optParcelsPattern;
    QString m_sarParcelsPattern;

    QString m_optParcelsTiffsPattern;
    QString m_sarParcelsTiffsPattern;
} ParcelsProductDescriptor;

typedef struct {
    QDateTime productDate;
    QDateTime insertedDate;

    // LPIS informations
    QString productName;
    QString productPath;
    QString opticalIdsGeomShapePath;
    QMap<QString, QString> sarGeomShapePaths;
    QMap<QString, QString> optTilesGeomsRasters;
    QMap<QString, QString> sarTilesGeomsRasters;

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
    // If resolution set to -1 or 0 means not used
    int nRes;
} MarkerType;

typedef struct PrdFileInfo {
    QString inFilePath;
    QDateTime prdTime;
    QString inFileMsk;
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
                    int siteId, int jobId, const QStringList &markersEnabled = {}, bool bUseLpisTileRasters = false,
                    const QMap<int, LpisInfos> &customParcelPrdsInfos = {},
                    const ParcelsProductDescriptor &customParcelPrdDescriptor = {"","","","",""},
                    const QString &dataExtrRootDir = "");
    void CreateTasks(const MarkerType &marker, QList<TaskToSubmit> &outAllTasksList, int &curTaskIdx,
                     const QList<int> &parentTaskIdxs = {}) const;
    void CreateSteps(const MarkerType &marker, QList<TaskToSubmit> &allTasksList, NewStepList &steps,
                     int &curTaskIdx, QStringList &dataExtrDirs) const;
    QList<MarkerType> GetEnabledMarkers() const;
    QDateTime GetDataExtractionMinDate() const;
    QDateTime GetDataExtractionMaxDate() const;
    QString GetDataExtractionDir(const QString &markerName) const;

    static bool HasAnyMarkerEnabled(const ProductType &prdType, const std::map<QString, QString> &cfgParams);

    void SetParcelsProductDescriptor(const ParcelsProductDescriptor &descr) { m_parcelsPrdDescr = descr; }

private:
    void InitEnabledMarkersDescriptions(const QStringList &markersEnabled);

    QString GetDataExtractionDir(int year, const QString &markerName) const;
    void ExtractProductFiles();
    QStringList GetDataExtractionFromShpArgs(const QString &uidField, const PrdMarkerInfo &inputFileInfo, const QString &outDir) const;
    QStringList GetDataExtractionFromRastersArgs(const PrdMarkerInfo &inputFileInfo, const QString &labelsImg, const QString &outDir) const;
    QMap<int, LpisInfos> ExtractLpisInfos();

    bool IsDataExtractionPerformed(const QString &dataExtrDirPath, const QString &prdPath);
    QList<PrdFileInfo> FilterAndUpdateAlreadyProcessingPrds(const QList<PrdFileInfo> &missingPrdsFiles,
                                                            const QList<PrdFileInfo> &processedPrdsFiles,
                                                            const MarkerType &markerType);
    bool IsScheduledJobRequest(const QJsonObject &parameters);
    QMap<int, QList<PrdFileInfo>> GroupProductFileInfosByYear(const QList<PrdFileInfo> &fileInfos);
    void UpdateParcelsPrdDescriptionsFromDB();
    QString GetS1ConfiguredProjection() const;
    QString GetLabelsImage(const QString &inRasterPath, int year) const;
    QString GetBestS1ParcelsShp(const LpisInfos &lpisInfo, const QString &filePath) const;

private:
    EventProcessingContext *pCtx;
    QJsonObject parameters;
    std::map<QString, QString> configParameters;
    std::map<QString, QString> parentConfigParameters;
    QString parentProcessorName;
    QString m_parentProcCfgPrefix;

    // parameters used for data extraction step
    bool isScheduledJob;

    int siteId;
    int jobId;
    QString siteShortName;
    QString dataExtractionRootDir;

    QDateTime prdMinDate;
    QDateTime prdMaxDate;

    static QList<MetricType> supportedMetrics;
    static QList<MarkerType> allMarkerFileTypes;
    QList<MarkerType> enabledMarkers;
    QList<ProductType> enabledMarkersProductTypes;

    QMap<int, LpisInfos> lpisInfos;
    QList<PrdMarkerInfo> fileInfos;
    QMap<QString, QStringList> markerDataExtrDirInfos;

    ParcelsProductDescriptor m_parcelsPrdDescr;
    bool m_bUseLpisTileRasters;
    QString m_s1PreprocessingProj;

};


#endif // S4CMARKERSDB1_EXTRACTOR_BASE_HPP
