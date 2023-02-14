#pragma once

#include "processorhandler.hpp"
#include "optional.hpp"
#include "s4c_mdb1_dataextract_steps_builder.hpp"
#include "products/generichighlevelproducthelper.h"

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
            enableYieldModel = ProcessorHandlerHelper::GetBoolConfigValue(parameters, configParameters,
                                                                            "enable_yield_model", S4S_YIELD_FEATS_CFG_PREFIX, false);
            const QString &yieldFeatPrdName = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters,
                                                                            "yield_features_product", S4S_YIELD_FEATS_CFG_PREFIX);
            const QMap<QString, QString> &prds = pCtx->GetProductsFullPaths(evt.siteId, {yieldFeatPrdName});
            if (prds.size() > 0) {
                yieldFeatPrd = prds[yieldFeatPrdName];
                yieldFeatPrd = QDir(QDir(yieldFeatPrd).filePath("VECTOR_DATA")).filePath("yield_features.csv");
                orchestrator::products::GenericHighLevelProductHelper prdHelper(yieldFeatPrdName);
                if(prdHelper.IsValid()) {
                    startDate = prdHelper.GetStartDate();
                    endDate = prdHelper.GetEndDate();
                }

            }
            extractFeatures = (!enableYieldModel || yieldFeatPrd.length() == 0);

        }
        void SetFilteringProducts(const QStringList &filterPrds) {
            filterProductNames = filterPrds;
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
        bool enableYieldModel;
        QString yieldFeatPrd;
        bool extractFeatures;

        std::map<QString, QString> configParameters;
        QJsonObject parameters;
        bool isScheduled;
        int year;

    } S4SYieldJobConfig;

private:
    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &event) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;

    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;
    QList<std::reference_wrapper<TaskToSubmit>> CreateTasks(const S4SYieldJobConfig &cfg, QList<TaskToSubmit> &outAllTasksList,
                                                            const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder);
    NewStepList CreateSteps(QList<TaskToSubmit> &allTasksList,
                            const S4SYieldJobConfig &cfg, const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder);
    int CreateMergeTasks(QList<TaskToSubmit> &outAllTasksList, const QString &taskName, int minPrdDataExtrIndex, int maxPrdDataExtrIndex, int &curTaskIdx);
    QString CreateStepsForFilesMerge(const S4SYieldJobConfig &jobCfg, const QStringList &dataExtrDirs,
                                     NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx);

    QStringList GetSGLaiTaskArgs(int year, const QString &mdb1File, const QString &sgOutFile,
                                 const QString &outCropGrowthIndicesFile, const QString &outLaiMetricsFile);
    QStringList GetParcelsExtractionTaskArgs(int siteId, int year, const QString &outFile);
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
    QStringList GetYieldReferenceExtractionTaskArgs(int siteId, const QString &outRefYieldFile, const QDateTime &startDate, const QDateTime &endDate);
    QStringList GetYieldModelTaskArgs(const S4SYieldJobConfig &cfg, const QString &yieldReference, const QString &inYieldFeatures,
                                      const QString &outYieldEstimates, const QString &outYieldSUEstimates);

    bool GetStartEndDatesFromProducts(EventProcessingContext &ctx, const JobSubmittedEvent &event,
                                      QDateTime &startDate, QDateTime &endDate, QList<ProductDetails> &productDetails);
    void UpdateJobConfigParameters(S4SYieldJobConfig &cfgToUpdate);
    bool IsScheduledJobRequest(const QJsonObject &parameters);
    QString GetProcessorDirValue(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                 const QString &key, const QString &siteShortName, const QString &year, const QString &defVal = "");
    QStringList GetProductFormatterArgs(TaskToSubmit &productFormatterTask, const S4SYieldJobConfig &cfg, const QStringList &listFiles);
};

