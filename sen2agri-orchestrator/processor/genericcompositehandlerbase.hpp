#ifndef GENERICCOMPOSITEHANDLER_HPP
#define GENERICCOMPOSITEHANDLER_HPP

#include "processorhandler.hpp"
#include "s4c_utils.hpp"

typedef struct {
    QString marker;
    ProductType prdType;
    QStringList markerSubstrInFileName;
    // this is used for rasters having multiple bands like old MAJA FRE format
    // In this case, the discrimination info is related to the name of the S2
    // band that will be used from the raster (if correctly mapped, otherwise
    // it will not be possible to extract the markers)
    QString bandDiscriminationInfo;
    int noDataVal;
    int mskValidVal;
} MarkerDescriptorType;

typedef struct {
    QString inFilePath;
    QString mask;
    Product containingPrd;
} ProductFileInfo;

typedef struct {
    MarkerDescriptorType markerInfo;
    ProductFileInfo prdFileInfo;
} ProductMarkerInfo;

class GenericCompositeJobPayload {
 public:
    GenericCompositeJobPayload(EventProcessingContext *pContext, const JobSubmittedEvent &evt, const QString &cfgPrefix)
                                : pCtx(pContext), event(evt) {
        parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
        configParameters = pCtx->GetJobConfigurationParameters(evt.jobId, cfgPrefix);
        siteShortName = pContext->GetSiteShortName(evt.siteId);
        int jobVal;
        isScheduledJob = ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "scheduled_job", jobVal) && (jobVal == 1);
        if (isScheduledJob) {
            QString strStartDate, strEndDate;
            ProcessorHandlerHelper::GetParameterValueAsString(parameters, "start_date", strStartDate);
            ProcessorHandlerHelper::GetParameterValueAsString(parameters, "end_date", strEndDate);
        }
        compositeMethod = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "method", cfgPrefix).toLower();
        if (compositeMethod.size() == 0) {
            compositeMethod = "mean";
        }
    }
    void UpdateMinMaxDates(const QDateTime &start, const QDateTime &end) {
        if (!isScheduledJob) {
            minDate = start;
            maxDate = end;
        }
    }

    EventProcessingContext *pCtx;
    JobSubmittedEvent event;
    QJsonObject parameters;
    std::map<QString, QString> configParameters;
    QString siteShortName;
    bool isScheduledJob;
    QDateTime minDate;
    QDateTime maxDate;
    QString compositeMethod;
};

// Just a marker interface
class FilterAndGroupingOptions {
public :
    QString name;
    QString filter;
};

class GenericCompositeHandler : public ProcessorHandler
{
public:
    GenericCompositeHandler(const QString &cfgPrefix, const QStringList &markerNames);

protected:
    virtual QList<FilterAndGroupingOptions> GetFilterAndGroupingOptions(const GenericCompositeJobPayload &) { return QList<FilterAndGroupingOptions>(); }
    virtual QMap<QString, QList<ProductMarkerInfo>> Filter(const FilterAndGroupingOptions &,
                                                           const QMap<QString, QList<ProductMarkerInfo>> &tileFilesInfo)
    {
        return tileFilesInfo;
    }

private:
    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &evt) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;
    void HandleProductAvailableImpl(EventProcessingContext &ctx,
                                    const ProductAvailableEvent &event) override;

    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;
    virtual QStringList GetAlwaysEnabledMarkerNames() { return {}; }
    virtual ProductType GetOutputProductType() = 0;

private:
    void CreateTasks(const GenericCompositeJobPayload &jobPayload,
                     const QMap<QString, QList<ProductMarkerInfo> > &tileFileInfos,
                     QList<TaskToSubmit> &allTasksList);
    void CreateSteps(const GenericCompositeJobPayload &jobPayload, QList<TaskToSubmit> &allTasksList,
                     const QMap<QString, QList<ProductMarkerInfo> > &tileFileInfos, const QString &additionalFilter= "");

    NewStep CreateStep(TaskToSubmit &dataExtractionTask,
                    const QList<ProductMarkerInfo> &fileInfos, const QString &outFile, const QString &method);
    QList<MarkerDescriptorType> GetMarkers(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                 const QString &procKeyPrefix, const QStringList &filteringMarkers = {},
                                 const QStringList &markersForcedEnabled = {});
    QStringList GetProductFormatterArgs(const GenericCompositeJobPayload &jobPayload, TaskToSubmit &productFormatterTask,
                                        const QMap<QString, QString> &tileResults, const QString &prdNameSuffix);
    QList<ProductType> GetMarkerProductTypes(const QList<MarkerDescriptorType> &markers);
    QMap<QString, QList<ProductMarkerInfo> > ExtractFileInfos(EventProcessingContext &ctx, const JobSubmittedEvent &evt, const QJsonObject &parameters,
                                                              const QList<MarkerDescriptorType> &markers, QDateTime &prdMinDate, QDateTime &prdMaxDate);
    QMap<QString, QList<ProductMarkerInfo>> FilterByMarkerName(const QMap<QString, QList<ProductMarkerInfo>> &tileFileInfos, const QString &marker);
    QString GetProductFormatterLevel();
    QString GetByTile(const QStringList &files, const QString &filter);
    void WriteExecutionInfosFile(const QString &executionInfosPath, const GenericCompositeJobPayload &jobPayload, const QList<ProductDetails> &productDetails);

    static QList<MarkerDescriptorType> allMarkerFileTypes;

    QString m_cfgPrefix;
    QStringList m_markerNames;
};


#endif // GENERICCOMPOSITEHANDLER_HPP
