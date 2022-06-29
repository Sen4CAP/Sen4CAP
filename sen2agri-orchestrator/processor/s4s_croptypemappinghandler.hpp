#pragma once

#include "processorhandler.hpp"
#include "optional.hpp"

#define S4S_CTM_CFG_PREFIX "processor.s4s_crop_mapping."

class S4SCropTypeMappingHandler : public ProcessorHandler
{

    typedef struct CropTypeJobConfig {
        CropTypeJobConfig(EventProcessingContext *pContext, const JobSubmittedEvent &evt)
            : event(evt), isScheduled(false) {
            pCtx = pContext;
            siteShortName = pContext->GetSiteShortName(evt.siteId);
            configParameters = pCtx->GetJobConfigurationParameters(evt.jobId, S4S_CTM_CFG_PREFIX);
            parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
        }

        EventProcessingContext *pCtx;
        JobSubmittedEvent event;

        QString siteShortName;
        QDateTime startDate;
        QDateTime endDate;

        QMap<QString, QString> mapCfgValues;
        std::map<QString, QString> configParameters;
        QJsonObject parameters;
        bool isScheduled;

    } CropTypeJobConfig;

public:
    S4SCropTypeMappingHandler();

private:
    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &event) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;

    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;
    QList<std::reference_wrapper<TaskToSubmit>> CreateTasks(QList<TaskToSubmit> &outAllTasksList);
    NewStepList CreateSteps(EventProcessingContext &ctx, const JobSubmittedEvent &event, QList<TaskToSubmit> &allTasksList,
                            const CropTypeJobConfig &cfg);
    QStringList GetCropTypeTaskArgs(const CropTypeJobConfig &cfg, const QString &prdTargetDir,  const QString &workingPath);
    QStringList GetProductFormatterArgs(TaskToSubmit &productFormatterTask, EventProcessingContext &ctx,
                                        const JobSubmittedEvent &event, const QString &tmpPrdDir,
                                        const QDateTime &minDate, const QDateTime &maxDate);

    bool GetStartEndDatesFromProducts(EventProcessingContext &ctx, const JobSubmittedEvent &event,
                                      QDateTime &startDate, QDateTime &endDate, QList<ProductDetails> &productDetails);
    void UpdateJobConfigParameters(CropTypeJobConfig &cfgToUpdate);
    bool IsScheduledJobRequest(const QJsonObject &parameters);
};
