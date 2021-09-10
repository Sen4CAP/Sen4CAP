#pragma once

#include "processorhandler.hpp"
#include "optional.hpp"
#include "s4c_mdb1_dataextract_steps_builder.hpp"

#define S4S_YIELD_FEATS_CFG_PREFIX "processor.s4s_yield_feat."

class S4SYieldHandler : public ProcessorHandler
{
    typedef struct S4SYieldJobConfig {
        S4SYieldJobConfig(EventProcessingContext *pContext, const JobSubmittedEvent &evt)
            : event(evt), isScheduled(false) {
            pCtx = pContext;
            siteShortName = pContext->GetSiteShortName(evt.siteId);
            configParameters = pCtx->GetJobConfigurationParameters(evt.jobId, S4S_YIELD_FEATS_CFG_PREFIX);
            parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
        }
        void SetFilteringProducts(const QStringList &filterPrds) {
            filterProductNames = filterPrds;
        }
        void SetParcelsFile(const QString &parcelsFile) {
            parcelsFilePath = parcelsFile;
        }

        void SetWeatherProducts(const ProductList &weatherPrds) {
            weatherPrdPaths.reserve(weatherPrds.size());
            for (auto const &prd : weatherPrds) weatherPrdPaths << prd.fullPath;
        }

        EventProcessingContext *pCtx;
        JobSubmittedEvent event;

        QString siteShortName;
        QDateTime startDate;
        QDateTime endDate;
        QStringList tileIds;
        QStringList filterProductNames;
        QStringList weatherPrdPaths;

        std::map<QString, QString> configParameters;
        QJsonObject parameters;
        bool isScheduled;
        int year;
        QString parcelsFilePath;

    } S4SYieldJobConfig;

private:
    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &event) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;

    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;
    QList<std::reference_wrapper<TaskToSubmit>> CreateTasks(QList<TaskToSubmit> &outAllTasksList, const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder);
    NewStepList CreateSteps(QList<TaskToSubmit> &allTasksList,
                            const S4SYieldJobConfig &cfg, const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder);
    int CreateMergeTasks(QList<TaskToSubmit> &outAllTasksList, const QString &taskName, int minPrdDataExtrIndex, int maxPrdDataExtrIndex, int &curTaskIdx);
    QString CreateStepsForFilesMerge(const S4SYieldJobConfig &jobCfg, const QStringList &dataExtrDirs,
                                     NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx);

    QStringList GetSGLaiTaskArgs(int year, const QString &mdb1File, const QString &sgOutFile,
                                 const QString &outCropGrowthIndicesFile, const QString &outLaiMetricsFile);
    QStringList GetWeatherFeaturesTaskArgs(const QStringList &weatherFiles, const QString &parcelsShp, const QString &outDir,
                                                            const QString &outGridToParcels, const QString &outParcelToGrid);
    QStringList GetWeatherFeaturesMergeTaskArgs(const QString &inDir, const QString &outWeatherFeatures);

    QStringList GetSafyLutTaskArgs(const QStringList &weatherFiles, const QString &safyParamFile,
                                   const QString &safyParamsRangesDir, const QString &outLutDir);
    QStringList GetMergeLaiGridTaskArgs(const QString &inputLaiFile, const QString &parcelsToGridFile, const QString &outMergedFile);
    QStringList GetSafyOptimTaskArgs(const QStringList &weatherFiles,  int year, const QString &inputLaiFile, const QString &gridToParcelsFile,
                                     const QString &safyParamsFile, const QString &safyParamsRangesFile,
                                     const QString &lutDir, const QString &workingDir, const QString &outSafyOptimFile);
    QStringList GetAllFeaturesMergeTaskArgs(const QString &weatherFeatFile, const QString &sgCropGrowthIndicesFile,
                                            const QString &safyFeatsFile, const QString &outMergedFeatures);
    QStringList GetYieldFeaturesTaskArgs(const QString &inMergedFeatures, const QString &outYieldFeatures);

    bool GetStartEndDatesFromProducts(EventProcessingContext &ctx, const JobSubmittedEvent &event,
                                      QDateTime &startDate, QDateTime &endDate, QList<ProductDetails> &productDetails);
    void UpdateJobConfigParameters(S4SYieldJobConfig &cfgToUpdate);
    QString GetParcelsFile(const S4SYieldJobConfig &cfg);
    bool IsScheduledJobRequest(const QJsonObject &parameters);
    QString GetProcessorDirValue(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                 const QString &key, const QString &siteShortName, const QString &year, const QString &defVal = "");
    QStringList GetProductFormatterArgs(TaskToSubmit &productFormatterTask, const S4SYieldJobConfig &cfg, const QStringList &listFiles);
};

