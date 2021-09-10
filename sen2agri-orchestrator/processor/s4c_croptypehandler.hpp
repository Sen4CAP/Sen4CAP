#pragma once

#include "processorhandler.hpp"
#include "optional.hpp"

#define L4A_CT_CFG_PREFIX "processor.s4c_l4a."

class S4CCropTypeHandler : public ProcessorHandler
{

    typedef struct CropTypeJobConfig {
        CropTypeJobConfig(EventProcessingContext *pContext, const JobSubmittedEvent &evt, const QStringList &cfgKeys)
            : event(evt), isScheduled(false) {
            pCtx = pContext;
            siteShortName = pContext->GetSiteShortName(evt.siteId);
            configParameters = pCtx->GetJobConfigurationParameters(evt.jobId, L4A_CT_CFG_PREFIX);
            parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
            for (QString cfgKey : cfgKeys) {
                mapCfgValues.insert(
                    cfgKey, ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters,
                                                                         cfgKey, L4A_CT_CFG_PREFIX));
            }
        }
        void SetFilteringProducts(const QStringList &filterPrds) {
            filterProductNames = filterPrds;
        }

        EventProcessingContext *pCtx;
        JobSubmittedEvent event;

        QString siteShortName;
        QDateTime startDate;
        QDateTime endDate;
        QStringList tileIds;
        QStringList filterProductNames;

        QMap<QString, QString> mapCfgValues;
        std::map<QString, QString> configParameters;
        QJsonObject parameters;
        bool isScheduled;

    } CropTypeJobConfig;

public:
    S4CCropTypeHandler();

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
    QStringList GetExtractParcelsTaskArgs(const CropTypeJobConfig &cfg,const QString &parcelsPath,
                                          const QString &lutPath, const QString &tilesPath,
                                          const QString &opticalPath, const QString &radarPath,
                                          const QString &lpisPath);
    QStringList GetCropTypeTaskArgs(const CropTypeJobConfig &cfg, const QString &prdTargetDir,  const QString &workingPath,
                                    const QString &outMarkerFilesInfos, const QString &parcelsPath, const QString &lutPath,
                                    const QString &tilesPath, const QString &opticalPath, const QString &radarPath, const QString &lpisPath);
    QStringList GetProductFormatterArgs(TaskToSubmit &productFormatterTask, EventProcessingContext &ctx,
                                        const JobSubmittedEvent &event, const QString &tmpPrdDir,
                                        const QDateTime &minDate, const QDateTime &maxDate);

    bool GetStartEndDatesFromProducts(EventProcessingContext &ctx, const JobSubmittedEvent &event,
                                      QDateTime &startDate, QDateTime &endDate, QList<ProductDetails> &productDetails);
    void UpdateJobConfigParameters(CropTypeJobConfig &cfgToUpdate);
    QStringList GetTileIdsFromProducts(EventProcessingContext &ctx,
                                        const JobSubmittedEvent &event, const QList<ProductDetails> &productDetails);
    bool IsScheduledJobRequest(const QJsonObject &parameters);
    void HandleMarkerProductsAvailable(EventProcessingContext &ctx, const TaskFinishedEvent &event);

    QStringList m_cfgKeys;
};
