#ifndef S4CMARKERSDB1HANDLER_HPP
#define S4CMARKERSDB1HANDLER_HPP

#include "processorhandler.hpp"
#include "s4c_utils.hpp"
#include "s4c_mdb1_dataextract_steps_builder.hpp"

typedef struct MDB1JobPayload {
    MDB1JobPayload(EventProcessingContext *pContext, const JobSubmittedEvent &evt) : event(evt) {
        pCtx = pContext;
        parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
        configParameters = pCtx->GetJobConfigurationParameters(evt.jobId, MDB1_CFG_PREFIX);
        siteShortName = pContext->GetSiteShortName(evt.siteId);
        int jobVal;
        isScheduledJob = ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "scheduled_job", jobVal) && (jobVal == 1);
    }
    EventProcessingContext *pCtx;
    JobSubmittedEvent event;
    QJsonObject parameters;
    std::map<QString, QString> configParameters;
    QString siteShortName;
    bool isScheduledJob;
    QDateTime minDate;
    QDateTime maxDate;
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

    void CreateTasks(QList<TaskToSubmit> &outAllTasksList, const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder);
    void CreateSteps(QList<TaskToSubmit> &allTasksList,
                     const MDB1JobPayload &siteCfg, const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder, NewStepList &steps);
    void WriteExecutionInfosFile(const QString &executionInfosPath,
                                 const QStringList &listProducts);
    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;

private:
    QString CreateStepsForFilesMerge(const MarkerType &markerType, const QStringList &dataExtrDirs,
                                  NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx);
    QString CreateStepsForMdb2Export(const MarkerType &markerType, const QString &mergedFile,
                                  NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx);
    QString CreateStepsForExportIpc(const MDB1JobPayload &jobCfg, const MarkerType &marker, const QString &inputFile,
                                    NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx, const QString &prdType);
    ProductList GetLpisProduct(ExecutionContextBase *pCtx, int siteId);

    QString GetShortNameForProductType(const ProductType &prdType);
    QString GetDataExtractionDir(const MDB1JobPayload &jobCfg, int year, const QString &markerName);

    bool CheckExecutionPreconditions(ExecutionContextBase *pCtx, const std::map<QString, QString> &configParameters, int siteId,
                                        const QString &siteShortName, QString &errMsg);
    QString BuildMergeResultFileName(const MarkerType &markerType);
    QString BuildMdb2FileName(const MarkerType &markerType);
    QStringList GetFilesMergeArgs(const QStringList &listInputPaths, const QString &outFileName);
};


#endif // S4CMARKERSDB1HANDLER_HPP
