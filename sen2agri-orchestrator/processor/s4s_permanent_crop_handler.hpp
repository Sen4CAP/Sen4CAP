#pragma once

#include "processorhandler.hpp"
#include "optional.hpp"

#define S4S_PERM_CROPS_CFG_PREFIX "processor.s4s_perm_crop."

class S4SPermanentCropHandler : public ProcessorHandler
{
    typedef struct S4SPermanentCropJobConfig {
        S4SPermanentCropJobConfig(EventProcessingContext *pContext, const JobSubmittedEvent &evt)
            : event(evt), isScheduled(false) {
            pCtx = pContext;
            siteShortName = pContext->GetSiteShortName(evt.siteId);
            configParameters = pCtx->GetJobConfigurationParameters(evt.jobId, S4S_PERM_CROPS_CFG_PREFIX);
            parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
        }
        void SetFilteringProducts(const QStringList &filterPrds) {
            filterProductNames = filterPrds;
        }
        void SetSamplesInfosProducts(const QString &sampleFile) {
            samplesShapePath = sampleFile;
        }

        EventProcessingContext *pCtx;
        JobSubmittedEvent event;

        QString siteShortName;
        QDateTime startDate;
        QDateTime endDate;
        QStringList tileIds;
        QStringList filterProductNames;

        std::map<QString, QString> configParameters;
        QJsonObject parameters;
        bool isScheduled;
        int year;
        QString samplesShapePath;

    } S4SPermanentCropJobConfig;

private:
    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &event) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;

    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;
    QList<std::reference_wrapper<TaskToSubmit>> CreateTasks(QList<TaskToSubmit> &outAllTasksList);
    NewStepList CreateSteps(QList<TaskToSubmit> &allTasksList,
                            const S4SPermanentCropJobConfig &cfg);

    QStringList GetExtractInputsTaskArgs(const S4SPermanentCropJobConfig &cfg, const QString &outFile);
    QStringList GetBuildVrtTaskArgs(const QString &inputsListFile, const QString &fullStackVrtPath, const QString &workingDir);
    QStringList GetBuildFullStackTifTaskArgs(const QString &inputFilesListPath, const QString &fullStackTifPath, const QString &workingDir);
    QStringList GetPolygonClassStatisticsTaskArgs(const QString &image, const QString &samples, const QString &fieldName, const QString &sampleStats);
    QStringList GetSampleSelectionTaskArgs(const QString &image, const QString &samples, const QString &fieldName, const QString &sampleStats, const QString &outRates, const QString &selectedUpdateSamples);
    QStringList GetSampleExtractionTaskArgs(const QString &image, const QString &fullStackTifPath, const QString &fieldName, const QString &finalUpdateSamples);
    QStringList GetSamplesRasterizationTaskArgs(const QString &reflStackTif, const QString &fullStackVrtPath, const QString &fieldName, int valToReplace, int replacingValue, const QString &outputFile);
    QStringList GetBroceliandeTaskArgs(const S4SPermanentCropJobConfig &cfg, const TaskToSubmit &task, const QString &fullStackVrtPath, const QString &samples, const QString &output);
    QStringList GetCropInfosExtractionTaskArgs(const QStringList &imgs, const QString &exp, const QString &out);
    QStringList GetCropSieveTaskArgs(const QString &annualCrop, const QString &annualSieve);

    bool GetStartEndDatesFromProducts(EventProcessingContext &ctx, const JobSubmittedEvent &event,
                                      QDateTime &startDate, QDateTime &endDate, QList<ProductDetails> &productDetails);
    void UpdateJobConfigParameters(S4SPermanentCropJobConfig &cfgToUpdate);
    QString ExtractSamplesInfos(const S4SPermanentCropJobConfig &cfg);
    QStringList GetTileIdsFromProducts(EventProcessingContext &ctx,
                                        const JobSubmittedEvent &event, const QList<ProductDetails> &productDetails);
    bool IsScheduledJobRequest(const QJsonObject &parameters);
    QStringList GetProductFormatterArgs(TaskToSubmit &productFormatterTask, const S4SPermanentCropJobConfig &cfg,
                                        const QStringList &listFiles);
};

