#pragma once

#include "processorhandler.hpp"
#include "optional.hpp"
#include "s4c_mdb1_dataextract_steps_builder.hpp"
#include "products/generichighlevelproducthelper.h"

#define S4S_YIELD_SU_CFG_PREFIX "processor.s4s_yield_su."

#define ESU_EXTRACTION_TASK_NAME QStringLiteral("yield-esu-extraction")
#define S4S_YIELD_SU_DEF_DATA_EXTR_ROOT   "/mnt/archive/marker_database_files/yield_su/mdb1/{site}/{year}/data_extraction/"
#define S4S_YIELD_SU_DEF_HIST_DATA_PATH   "/mnt/archive/s4s_yield/{site}/yield_su/HistoricalData/SU_yield_historical_data.csv"

class S4SYieldSUHandler : public ProcessorHandler
{
    typedef struct S4SYieldJobConfig {
        S4SYieldJobConfig(EventProcessingContext *pContext, const JobSubmittedEvent &evt, const QString &procShortName)
            : event(evt), isScheduled(false) {
            pCtx = pContext;
            siteShortName = pContext->GetSiteShortName(evt.siteId);
            configParameters = pCtx->GetJobConfigurationParameters(evt.jobId, S4S_YIELD_SU_CFG_PREFIX);
            parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();

            startDate = ProcessorHandlerHelper::GetDateTimeFromString(
                        ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "start_date", S4S_YIELD_SU_CFG_PREFIX));
            endDate = ProcessorHandlerHelper::GetDateTimeFromString(
                        ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "end_date", S4S_YIELD_SU_CFG_PREFIX));

            year = endDate.date().year();           // TODO: see if this is valid

            // extract the historical yield file
            historicalYieldFile = GetProcessorDirValue(parameters, configParameters, "historical_data_path", siteShortName,
                                                       procShortName, S4S_YIELD_SU_DEF_HIST_DATA_PATH);
            QFileInfo qfi(historicalYieldFile);
            if (!qfi.exists() || !qfi.isFile()) {
                pCtx->MarkJobFailed(event.jobId);
                throw std::runtime_error(QStringLiteral("Yield SU: Historical Yield file for site %1 was not uploaded yet!")
                                         .arg(siteShortName).toStdString());
            }

            dataExtractionRootDir = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "data_extr_dir", S4S_YIELD_SU_CFG_PREFIX);
            if (dataExtractionRootDir.size() == 0) {
                dataExtractionRootDir = S4S_YIELD_SU_DEF_DATA_EXTR_ROOT;
            }
            suPath = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "su_path", S4S_YIELD_SU_CFG_PREFIX);
            suPath = suPath.replace("{site}", siteShortName);
            suPath = GetSUShapefile(suPath);
            suUniqueId = "ID_2";    // TODO: This should be configurable

            enableYieldModel = ProcessorHandlerHelper::GetBoolConfigValue(parameters, configParameters,
                                                                            "enable_yield_model", S4S_YIELD_SU_CFG_PREFIX, false);
            if (enableYieldModel) {
                const QString &yieldFeatPrdName = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters,
                                                                                "yield_features_product", S4S_YIELD_SU_CFG_PREFIX);
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
            }

            extractFeatures = (!enableYieldModel || yieldFeatPrd.length() == 0);

            if (extractFeatures) {
                cropTypePrdPath = GetCropTypeProductPath();

                const ProductList &weatherPrdsList = pCtx->GetProducts(event.siteId, (int)ProductType::ERA5WeatherProductTypeId,
                                                                                   startDate, endDate);
                if (weatherPrdsList.size() == 0) {
                    pCtx->MarkJobFailed(event.jobId);
                    throw std::runtime_error(QStringLiteral("Yield: No weather products were found in database for site %1 and interval %2 - %3.")
                                             .arg(siteShortName)
                                             .arg(startDate.toString())
                                             .arg(endDate.toString()).toStdString());
                }
                SetWeatherProducts(weatherPrdsList);
            }

            lpisInfos = CreateLpisInfos(procShortName);
        }

        void SetWeatherProducts(const ProductList &weatherPrds) {
            weatherPrdPaths.reserve(weatherPrds.size());
            for (auto const &prd : weatherPrds) weatherPrdPaths << prd.fullPath;
        }

        QString GetSUShapefile(const QString &suDir) {
            QDir directory(suDir);
            const QStringList &dirFiles = directory.entryList(QStringList() << "*.shp" ,QDir::Files);
            foreach(const QString &fileName, dirFiles) {
                return directory.filePath(fileName);
            }
            pCtx->MarkJobFailed(event.jobId);
            throw std::runtime_error(
                QStringLiteral("Yield SU: Unable to find a shapefile in directory %1").arg(suDir).toStdString());
        }

        QMap<int, LpisInfos> CreateLpisInfos(const QString &procShortName)
        {
            const QString &jobPath = pCtx->GetJobOutputPath(event.jobId, procShortName);
            const QString &rastersPath = QDir(jobPath).filePath("0000" + ESU_EXTRACTION_TASK_NAME + "-lpis-rasters");
            if (!QDir::root().mkpath(rastersPath)) {
                pCtx->MarkJobFailed(event.jobId);
                throw std::runtime_error(
                    QStringLiteral("Unable to create job output path %1").arg(rastersPath).toStdString());
            }

            siteTiles = pCtx->GetSiteTiles(event.siteId, (int)Satellite::Sentinel2);
            for (const Tile &tile : siteTiles) {
                esuTileRasterPaths[tile.tileId] = QDir(rastersPath).filePath("ESU_Random_" + tile.tileId + ".tif");
            }

            int startYear = startDate.date().year();
            int endYear = endDate.date().year();
            for (int i = 0; i <= (endYear - startYear); i++) {
                LpisInfos infos;
                int curYear = startYear+i;
                infos.productDate = startDate.addYears(i);
                infos.insertedDate = infos.productDate;
                infos.productName = QStringLiteral("LPIS_") + QString::number(curYear);
                infos.optTilesGeomsRasters = esuTileRasterPaths;
                infos.sarTilesGeomsRasters = esuTileRasterPaths;
                lpisInfos[curYear] = infos;
            }

            return lpisInfos;
        }

        QString GetCropTypeProductPath();
        QString GetProcessorDirValue(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                     const QString &key, const QString &siteShortName, const QString &procShortName, const QString &defVal );

        EventProcessingContext *pCtx;
        JobSubmittedEvent event;

        QString siteShortName;
        QDateTime startDate;
        QDateTime endDate;
        QStringList filterProductNames;
        QStringList weatherPrdPaths;
        bool enableYieldModel;
        QString yieldFeatPrd;
        bool extractFeatures;

        std::map<QString, QString> configParameters;
        QJsonObject parameters;
        bool isScheduled;
        int year;
        QMap<int, LpisInfos> lpisInfos;
        QString cropTypePrdPath;
        QString dataExtractionRootDir;
        QString suPath;
        QString suUniqueId;
        QMap<QString, QString> esuTileRasterPaths;
        TileList siteTiles;
        QString historicalYieldFile;

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

    QStringList GetEsuExtractionTaskArgs(const S4SYieldJobConfig &cfg, const QString &workingDir, const QString &outESUCsvFile);
    QStringList GetESUAggregationTaskArgs(const QString &esuCsvFile, const QString &laiMergedPath, const QString &workingDir,
                                          const QString &outAggregatedLAI);
    QStringList GetSGLaiTaskArgs(int year, const QString &mdb1File, const QString &sgOutFile,
                                 const QString &outCropGrowthIndicesFile, const QString &outLaiMetricsFile);
    QStringList GetTrendFeaturesTaskArgs(const QString &input, int year, const QString &output);
    QStringList GetParcelsExtractionTaskArgs(int siteId, int year, const QString &outFile);
    QStringList GetWeatherFeaturesTaskArgs(const QStringList &weatherFiles, const QString &parcelsShp, const QString &shpIdFieldName, const QString &outDir);
    QStringList GetWeatherFeaturesMergeTaskArgs(const QString &inDir, const QString &outWeatherFeatures);

    QStringList GetAllFeaturesMergeTaskArgs(const QString &sgListFile, const QString &trendFeatFile, const QString &weatherFeatFile, const QString &outMergedFeatures, const QString &sgYieldLaiFeaturesPath);
    QStringList GetYieldFeaturesTaskArgs(const QString &inMergedFeatures, const QString &outYieldFeatures);
    QStringList GetYieldReferenceExtractionTaskArgs(int siteId, const QString &outRefYieldFile, const QDateTime &startDate, const QDateTime &endDate);
    QStringList GetCropTypesExtractionTaskArgs(int siteId, int year, const QString &outCropTypesFile);
    QStringList GetYieldModelTaskArgs(const S4SYieldJobConfig &cfg, const QString &yieldReference, const QString &cropCodesFile, const QString &inYieldFeatures,
                                      const QString &outYieldEstimates, const QString &outYieldSUEstimates);

    QString GetProcessorDirValue(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                 const QString &key, const QString &siteShortName, const QString &defVal = "");
    QStringList GetProductFormatterArgs(TaskToSubmit &productFormatterTask, const S4SYieldJobConfig &cfg, const QStringList &listFiles);
};

