#ifndef S4CMARKERSDB1HANDLER_HPP
#define S4CMARKERSDB1HANDLER_HPP

#include "processorhandler.hpp"
#include "s4c_utils.hpp"
#include "s4c_mdb1_dataextract_steps_builder.hpp"

typedef struct MDB1JobPayload {
    MDB1JobPayload(EventProcessingContext *pContext, const JobSubmittedEvent &evt,
                   const QDateTime &minDate, const QDateTime &maxDate)
        : event(evt), minDate(minDate), maxDate(maxDate) {
        pCtx = pContext;
        parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
        configParameters = pCtx->GetJobConfigurationParameters(evt.jobId, MDB1_CFG_PREFIX);
        siteShortName = pContext->GetSiteShortName(evt.siteId);
        int jobVal;
        isScheduledJob = ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "scheduled_job", jobVal) && (jobVal == 1);
        ampvvvhEnabled = ProcessorHandlerHelper::GetBoolConfigValue(parameters, configParameters, "amp_vvvh_enabled", MDB1_CFG_PREFIX) &&
                         ProcessorHandlerHelper::GetBoolConfigValue(parameters, configParameters, "amp_enabled", MDB1_CFG_PREFIX);
        mdb3M1M5Enabled = ProcessorHandlerHelper::GetBoolConfigValue(parameters, configParameters, "mdb3_enabled", MDB1_CFG_PREFIX);
    }
    EventProcessingContext *pCtx;
    JobSubmittedEvent event;
    QJsonObject parameters;
    std::map<QString, QString> configParameters;
    QString siteShortName;
    bool isScheduledJob;
    QDateTime minDate;
    QDateTime maxDate;
    bool ampvvvhEnabled;
    bool mdb3M1M5Enabled;       // L4C M1-M5 markers extraction
} MDB1JobPayload;

class S4CMarkersDB1Handler : public ProcessorHandler
{
public:
    S4CMarkersDB1Handler();
private:
    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &evt) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;
    void HandleProductAvailableImpl(EventProcessingContext &ctx,
                                    const ProductAvailableEvent &event) override;

    void CreateTasks(QList<TaskToSubmit> &outAllTasksList, const MDB1JobPayload &siteCfg,
                     const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder);
    void CreateSteps(QList<TaskToSubmit> &allTasksList, const MDB1JobPayload &siteCfg,
                     const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder,
                     NewStepList &steps);

    void WriteExecutionInfosFile(const QString &executionInfosPath,
                                 const QStringList &listProducts);
    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;

private:
    QString CreateStepsForFilesMerge(const QStringList &dataExtrDirs,
                                  NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx);
    QString CreateStepsForAmpVVVHExtraction(const QString &mergedFile,
                                  NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx);
    QString PrepareIpcExport(const MDB1JobPayload &jobCfg, TaskToSubmit &exportTask, const QString &prdType);
    QString CreateStepsForExportIpc(const MDB1JobPayload &jobCfg, const QString &inputFile,
                                    NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx, const QString &prdType);
    ProductList GetLpisProduct(ExecutionContextBase *pCtx, int siteId);

    QString GetShortNameForProductType(const ProductType &prdType);
    QString GetDataExtractionDir(const MDB1JobPayload &jobCfg, int year, const QString &markerName);
    QString CreateMdb3Steps(const MDB1JobPayload &jobCfg, const Season &season, const QString &mergedFile,
                                NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx);
    QStringList GetFilesMergeArgs(const QStringList &listInputPaths, const QString &outFileName);
};


#endif // S4CMARKERSDB1HANDLER_HPP
