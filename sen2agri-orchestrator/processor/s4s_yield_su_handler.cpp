#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "logger.hpp"
#include "processorhandlerhelper.h"
#include "s4s_yield_su_handler.hpp"
#include "s4c_utils.hpp"
#include "stepexecutiondecorator.h"

#include "products/generichighlevelproducthelper.h"
using namespace orchestrator::products;

// TODO: These defines shoule be extracted from config
static QStringList YIELD_INPUT_MARKER_NAMES = {"LAI"};

QList<std::reference_wrapper<TaskToSubmit>>
S4SYieldSUHandler::CreateTasks(const S4SYieldJobConfig &cfg, QList<TaskToSubmit> &outAllTasksList,
                             const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder)
{
    int curTaskIdx = 0;
    int yieldFeatExtrIdx = -1;
    int prdFormatterParentIdx = -1;
    if (cfg.extractFeatures) {
        outAllTasksList.append(TaskToSubmit{ "s4s-yield-esu-extraction", {} });
        int extractESUIdx = curTaskIdx++;
        auto dataExtrParentTaskIdxs = {extractESUIdx};


        const QList<MarkerType> &enabledMarkers = dataExtrStepsBuilder.GetEnabledMarkers();
        QList<int> mergeTasksIndexes;
        QList<std::reference_wrapper<const TaskToSubmit>> mergeTasks;
        for (const auto &marker: enabledMarkers) {
            // Create data extraction tasks if needed
            int minDataExtrIndex = curTaskIdx;
            dataExtrStepsBuilder.CreateTasks(marker, outAllTasksList, curTaskIdx, dataExtrParentTaskIdxs);
            int maxDataExtrIndex = curTaskIdx-1;

            if (YIELD_INPUT_MARKER_NAMES.contains(marker.marker)) {
                // create the merging tasks if needed
                int mergeTaskIdx = CreateMergeTasks(outAllTasksList, marker.marker.toLower() + "-data-extraction-merge",
                                                        minDataExtrIndex, maxDataExtrIndex, curTaskIdx);
                mergeTasksIndexes.push_back(mergeTaskIdx);
                mergeTasks.append(outAllTasksList[mergeTaskIdx]);
            }
        }

        outAllTasksList.append(TaskToSubmit{ "s4s-yield-esu-aggregate", mergeTasks });
        int aggESUIdx = curTaskIdx++;

        outAllTasksList.append(TaskToSubmit{"s4s-savitzky-golay-wrp", {outAllTasksList[aggESUIdx]}  });
        int sgIdx = curTaskIdx++;

        outAllTasksList.append(TaskToSubmit{ "s4s-yield-trend-features-extraction", {} });
        int trendFeatExtrIdx = curTaskIdx++;
        outAllTasksList.append(TaskToSubmit{ "s4s-extract-weather-features", {}  });
        int weatherFeatIdx = curTaskIdx++;
        outAllTasksList.append(TaskToSubmit{ "s4s-merge-weather-features", {outAllTasksList[weatherFeatIdx]}  });
        int mergeWeatherFeatIdx = curTaskIdx++;
        outAllTasksList.append(TaskToSubmit{ "s4s-merge-all-features-wrp", {outAllTasksList[sgIdx],
                                                                            outAllTasksList[mergeWeatherFeatIdx],
                                                                            outAllTasksList[trendFeatExtrIdx]}  });
        int mergeAllFeatIdx = curTaskIdx++;
        outAllTasksList.append(TaskToSubmit{ "s4s-yield-features-extraction-wrp", {outAllTasksList[mergeAllFeatIdx]} });

        yieldFeatExtrIdx = curTaskIdx++;
        prdFormatterParentIdx = yieldFeatExtrIdx;
    }

    if (cfg.enableYieldModel) {
        outAllTasksList.append(TaskToSubmit{ "s4s-yield-model", {} });
        int yieldModelIdx = curTaskIdx++;
        prdFormatterParentIdx = yieldModelIdx;
    }

    outAllTasksList.append(TaskToSubmit{ "product-formatter", {outAllTasksList[prdFormatterParentIdx]} });

    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    for (TaskToSubmit &task : outAllTasksList) {
        allTasksListRef.append(task);
    }
    return allTasksListRef;
}

NewStepList S4SYieldSUHandler::CreateSteps(QList<TaskToSubmit> &allTasksList,const S4SYieldJobConfig &cfg,
                                         const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder)
{
    int curTaskIdx = 0;
    NewStepList allSteps;
    QStringList prdFormatterFiles;
    QString yieldFeaturesOutputPath;
    if (cfg.extractFeatures) {
        // create the step for ESU extraction
        TaskToSubmit &esuExtrTask = allTasksList[curTaskIdx++];

        const QString &esuExtrPath = esuExtrTask.GetFilePath("");
        const QString &esuCSVPath = esuExtrTask.GetFilePath("ESU.csv");
        const QStringList &esuExtrArgs = GetEsuExtractionTaskArgs(cfg, esuExtrPath, esuCSVPath);
        allSteps.append(CreateTaskStep(esuExtrTask, "ESUExtraction", esuExtrArgs ));

        const QList<MarkerType> &enabledMarkers = dataExtrStepsBuilder.GetEnabledMarkers();
        // if only data extraction is needed, then we create the filter ids step into the general configured directory
        QString mdb1File;
        for (const auto &marker: enabledMarkers) {
            QStringList dataExtrDirs;
            // Create the data extraction steps if needed
            dataExtrStepsBuilder.CreateSteps(marker, allTasksList, allSteps, curTaskIdx, dataExtrDirs);

            if (YIELD_INPUT_MARKER_NAMES.contains(marker.marker)) {
                // If scheduled jobs, force adding the data extraction directories for all markers as data extraction source
                if (cfg.isScheduled) {
                    // add a data extraction dir corresponding to the scheduled date which is saved as jobCfg.maxPrdDate
                    const QString &dataExtrDirName = dataExtrStepsBuilder.GetDataExtractionDir(marker.marker);
                    if (!dataExtrDirs.contains(dataExtrDirName)) {
                        QDir().mkpath(dataExtrDirName);
                        dataExtrDirs.append(dataExtrDirName);
                    }
                }
                const QString &retMergedFile = CreateStepsForFilesMerge(cfg, dataExtrDirs, allSteps,
                                                                         allTasksList, curTaskIdx);
                mdb1File = retMergedFile;
            }
        }
        if (mdb1File.size() == 0) {
            cfg.pCtx->MarkJobFailed(cfg.event.jobId);
            throw std::runtime_error(
                QStringLiteral(
                    "Yield SU: Impossible to create the merged markers file. LAI Marker not enabled in database for MDB1?")
                    .toStdString());
        }

        TaskToSubmit &esuAggregateTask = allTasksList[curTaskIdx++];
        TaskToSubmit &sgTask = allTasksList[curTaskIdx++];
        TaskToSubmit &trendFeatExtrTask = allTasksList[curTaskIdx++];
        TaskToSubmit &weatherFeatTask = allTasksList[curTaskIdx++];
        TaskToSubmit &weatherFeatMergeTask = allTasksList[curTaskIdx++];
        TaskToSubmit &mergeAllFeatTask = allTasksList[curTaskIdx++];
        TaskToSubmit &yieldFeatTask = allTasksList[curTaskIdx++];

        // Resulting files from tasks
        const QString &esuAggWorkPath = esuAggregateTask.GetFilePath("");
        const QString &esuAggResultPath = esuAggregateTask.GetFilePath("LAIGrouped.csv");
        const QString &sgLaiPath = sgTask.GetFilePath("sg_lai_outputs.csv");
        const QString &trendFeaturesPath = trendFeatExtrTask.GetFilePath("trend_features.csv");
        const QString &sgCropGrowthIndicesPath = sgTask.GetFilePath("sg_crop_growth_indices.csv");
        const QString &sgYieldLaiFeaturesPath = sgTask.GetFilePath("yield_lai_features.csv");
        const QString &weatherWorkingDirPath = weatherFeatTask.GetFilePath("");
        // Workaround: Althogh created by weather featurs task, we add these here in order to avoid putting them in the same directory
        const QString &outWeatherFeaturesPath = weatherFeatMergeTask.GetFilePath("weather_raw_features.csv");
        const QString &allFeatOutputPath = mergeAllFeatTask.GetFilePath("merged_weather_sg_features.csv");
        const QString &yieldFeaturesOutputPath = yieldFeatTask.GetFilePath("yield_features.csv");

        // Inputs extraction and reflectances stack tif creation
        const QStringList &esuAggArgs = GetESUAggregationTaskArgs(esuCSVPath, mdb1File, esuAggWorkPath, esuAggResultPath);
        allSteps.append(CreateTaskStep(esuAggregateTask, "ESUAggregation", esuAggArgs ));

        // Inputs extraction and reflectances stack tif creation
        const QStringList &sgArgs = GetSGLaiTaskArgs(cfg.year, esuAggResultPath, sgLaiPath, sgCropGrowthIndicesPath, sgYieldLaiFeaturesPath);
        allSteps.append(CreateTaskStep(sgTask, "SavitzkyGolay", sgArgs ));

        const QStringList &trendArgs = GetTrendFeaturesTaskArgs(cfg.historicalYieldFile, cfg.year, trendFeaturesPath);
        allSteps.append(CreateTaskStep(trendFeatExtrTask, "TrendFeatures", trendArgs ));

        const QStringList &weatherFeaturesExtractionArgs = GetWeatherFeaturesTaskArgs(cfg.weatherPrdPaths, cfg.suPath, cfg.suUniqueId,
                                                                                      weatherWorkingDirPath);
        allSteps.append(CreateTaskStep(weatherFeatTask, "WeatherFeatures", weatherFeaturesExtractionArgs));

        const QStringList &weatherFeaturesMergeArgs = GetWeatherFeaturesMergeTaskArgs(weatherWorkingDirPath, outWeatherFeaturesPath);
        allSteps.append(CreateTaskStep(weatherFeatMergeTask, "WeatherFeaturesMerge", weatherFeaturesMergeArgs));

        const QStringList &allFeatureMergeArgs = GetAllFeaturesMergeTaskArgs(sgCropGrowthIndicesPath, trendFeaturesPath, outWeatherFeaturesPath, allFeatOutputPath);
        allSteps.append(CreateTaskStep(mergeAllFeatTask, "AllFeaturesMerge", allFeatureMergeArgs));

        const QStringList &yieldFeatExtractionArgs = GetYieldFeaturesTaskArgs(allFeatOutputPath, yieldFeaturesOutputPath);
        allSteps.append(CreateTaskStep(yieldFeatTask, "YieldFeatures", yieldFeatExtractionArgs));
        prdFormatterFiles.append(yieldFeaturesOutputPath);
    } else {
        yieldFeaturesOutputPath = cfg.yieldFeatPrd;
    }

    int yieldRefTskId = -1;
    int yieldModelTskId = -1;
    if (cfg.enableYieldModel) {
        yieldRefTskId = curTaskIdx++;
        yieldModelTskId = curTaskIdx++;
    }
    TaskToSubmit &productFormatterTask = allTasksList[curTaskIdx++];

    if (cfg.enableYieldModel) {
        TaskToSubmit &yieldReferenceExtrTask = allTasksList[yieldRefTskId];
        const QString &yieldReference = yieldReferenceExtrTask.GetFilePath("yield_reference.csv");
        const QStringList &yieldReferenceExtractionArgs = GetYieldReferenceExtractionTaskArgs(cfg.event.siteId, yieldReference, cfg.startDate, cfg.endDate);
        allSteps.append(CreateTaskStep(yieldReferenceExtrTask, "YieldReferenceExtraction", yieldReferenceExtractionArgs));

        TaskToSubmit &yieldModelTask = allTasksList[yieldModelTskId];
        const QString &yieldEstimateOutputPath = yieldModelTask.GetFilePath("yield_estimate.csv");
        const QString &yieldStatisticalUnitEstimateOutputPath = yieldModelTask.GetFilePath("yield_statistical_units_estimate.csv");
        const QStringList &yieldModelExtractionArgs = GetYieldModelTaskArgs(cfg, yieldReference, yieldFeaturesOutputPath, yieldEstimateOutputPath, yieldStatisticalUnitEstimateOutputPath);
        allSteps.append(CreateTaskStep(yieldModelTask, "YieldModel", yieldModelExtractionArgs));
        prdFormatterFiles += {yieldEstimateOutputPath, yieldStatisticalUnitEstimateOutputPath};
    }

    const QStringList &productFormatterArgs = GetProductFormatterArgs(productFormatterTask, cfg, prdFormatterFiles);
    allSteps.append(CreateTaskStep(productFormatterTask, "ProductFormatter", productFormatterArgs));

    return allSteps;
}

int S4SYieldSUHandler::CreateMergeTasks(QList<TaskToSubmit> &outAllTasksList, const QString &taskName,
                                        int minPrdDataExtrIndex, int maxPrdDataExtrIndex, int &curTaskIdx) {
    outAllTasksList.append(TaskToSubmit{ taskName, {} });
    int mergeTaskIdx = curTaskIdx++;
    // update the parents for this task
    if (minPrdDataExtrIndex != -1) {
        for (int i = minPrdDataExtrIndex; i <= maxPrdDataExtrIndex; i++) {
            outAllTasksList[mergeTaskIdx].parentTasks.append(outAllTasksList[i]);
        }
    }
    return mergeTaskIdx;
}

QString S4SYieldSUHandler::CreateStepsForFilesMerge(const S4SYieldJobConfig &jobCfg,
                              const QStringList &dataExtrDirs, NewStepList &steps,
                              QList<TaskToSubmit> &allTasksList, int &curTaskIdx) {
    TaskToSubmit &mergeTask = allTasksList[curTaskIdx++];
    QString yearStr = QString::number(jobCfg.year);
    QString mergeResultFileName = yearStr.append("_LAI_Extracted_Data.csv");
    const QString &mergedFile = mergeTask.GetFilePath(mergeResultFileName);
    QStringList mergeArgs = { "Markers1CsvMerge", "-out", mergedFile, "-il" };
    mergeArgs += dataExtrDirs;
    steps.append(CreateTaskStep(mergeTask, "Markers1CsvMerge", mergeArgs));

    return mergedFile;
}

QStringList S4SYieldSUHandler::GetEsuExtractionTaskArgs(const S4SYieldJobConfig &cfg, const QString &workingDir, const QString &outESUCsvFile)
{
    QStringList args = {    "--crop-type-path", cfg.cropTypePrdPath,
                "--su-path", cfg.suPath,
                "--su-unique-id", cfg.suUniqueId,
                "--working-dir", workingDir,
                "--output", outESUCsvFile
    };
    args += "--out-tile-rasters";
    for (const Tile &tile: cfg.siteTiles) {
        args += cfg.esuTileRasterPaths[tile.tileId];
    }
    args += "--tiles";
    for(const Tile &tile: cfg.siteTiles) {
        args += tile.tileId;
    }

    return args;
}

QStringList S4SYieldSUHandler::GetESUAggregationTaskArgs(const QString &esuCsvFile, const QString &laiMergedPath,
                                              const QString &workingDir, const QString &outAggregatedLAI)
{
    return {    "--esu-path", esuCsvFile,
                "--lai-merged-path", laiMergedPath,
                "--working-dir", workingDir,
                "--output", outAggregatedLAI
    };
}

QStringList S4SYieldSUHandler::GetSGLaiTaskArgs(int year, const QString &mdb1File, const QString &sgOutFile,
                                              const QString &outCropGrowthIndicesFile, const QString &outLaiMetricsFile)
{
    return {    "--input", mdb1File,
                "--year", QString::number(year),
                "--sg-output", sgOutFile,
                "--indices-output", outCropGrowthIndicesFile,
                "--metrics-output", outLaiMetricsFile
    };
}

QStringList S4SYieldSUHandler::GetTrendFeaturesTaskArgs(const QString &input, int year, const QString &output)
{
    return {    "--input", input,
                "--year", QString::number(year),
                "--output", output
    };
}

QStringList S4SYieldSUHandler::GetParcelsExtractionTaskArgs(int siteId, int year, const QString &outFile)
{
    return { "-s", QString::number(siteId), "-y", QString::number(year), "-o", outFile};
}

QStringList S4SYieldSUHandler::GetWeatherFeaturesTaskArgs(const QStringList &weatherFiles, const QString &parcelsShp,
                                                          const QString &shpIdFieldName, const QString &outDir)
{
    QStringList args = { "-v", parcelsShp, "-o", outDir, "-f", shpIdFieldName};
    args += "-i";
    args.append(weatherFiles);

    return args;
}

QStringList S4SYieldSUHandler::GetWeatherFeaturesMergeTaskArgs(const QString &inDir, const QString &outWeatherFeatures)
{
    return { "Markers1CsvMerge", "-out", outWeatherFeatures, "-il", inDir };
}

QStringList S4SYieldSUHandler::GetAllFeaturesMergeTaskArgs(const QString &sgListFile, const QString &trendFeatFile,
                                                           const QString &weatherFeatFile, const QString &outMergedFeatures)
{
    return  { "-i", sgListFile,
              "-t", trendFeatFile,
              "-w", weatherFeatFile,
              "-o", outMergedFeatures,
              "-g", "0"};
}

QStringList S4SYieldSUHandler::GetYieldFeaturesTaskArgs(const QString &inMergedFeatures, const QString &outYieldFeatures)
{

    return { "-i", inMergedFeatures, "-o", outYieldFeatures};
}

QStringList S4SYieldSUHandler::GetYieldReferenceExtractionTaskArgs(int siteId, const QString &outRefYieldFile, const QDateTime &startDate, const QDateTime &endDate)
{
    return { "-s", QString::number(siteId), "-o", outRefYieldFile,
             "-b", startDate.toString("yyyy-MM-dd"),
             "-e",  endDate.toString("yyyy-MM-dd")};
}

QStringList S4SYieldSUHandler::GetYieldModelTaskArgs(const S4SYieldJobConfig &cfg, const QString & yieldReference, const QString &inYieldFeatures,
                                                   const QString &outYieldEstimates, const QString &outYieldSUEstimates)
{
    const QString &algo = ProcessorHandlerHelper::GetStringConfigValue(cfg.parameters, cfg.configParameters,
                                                                               "algorithm", S4S_YIELD_SU_CFG_PREFIX);
    const QString &selectionType = ProcessorHandlerHelper::GetStringConfigValue(cfg.parameters, cfg.configParameters,
                                                                               "selection-type", S4S_YIELD_SU_CFG_PREFIX);
    QString maxAutomaticFeaturesNo;
    if (selectionType == "automatic") {
        maxAutomaticFeaturesNo = ProcessorHandlerHelper::GetStringConfigValue(cfg.parameters, cfg.configParameters,
                                                                                   "max-automatic-features-no", S4S_YIELD_SU_CFG_PREFIX);
    }
    QStringList manualFeatures;
    if (selectionType == "manual") {
        const QString &strManualFeatures = ProcessorHandlerHelper::GetStringConfigValue(cfg.parameters, cfg.configParameters,
                                                                                   "manual-selection-features", S4S_YIELD_SU_CFG_PREFIX);
        manualFeatures = strManualFeatures.split(',', QString::SkipEmptyParts);
    }

//        "-a", "--algo", required=False, default="rf", help="The algorithm to be used. lm - LinerarRegression, svm - SupportVectortMachine. Default rf = RandomForest", choices=['rf', 'lm', 'svm']
//        "-s", "--selection", required=False, default="none", help="The selection mode. Possible values: automatic or manual or none", choices=['none', 'manual', 'automatic']
//        "-m", "--manual-selection-features", required=False, help="The selection features list for the manual mode", nargs='+', type=str
//        "-n", "--max-automatic-features-no", required=False, help="The maximum number of selection features for the automatic mode", type=int, default = 44
//        "-i", "--input-features", required=True, help="The input features file"
//        "-r", "--yield-reference", required=True, help="The input yield reference file"
//        "-u", "--statistical-unit-fields", required=True, help="The input statistical unit fields mapping file"
//        "-o", "--output", required=True, help="The output estimation file"
//        "-e", "--output-statistical-units-estimate", required=True, help="The output for statistical units estimation file"


    QStringList args =
    {
        "-i", inYieldFeatures,
        "-o", outYieldEstimates,
        "-e", outYieldSUEstimates,
        "-r", yieldReference,
        // "-u", statisticalUnitFields // TODO: We should obtain this somehow
    };
    if (algo.size() > 0) {
        args += "-a";
        args.append(algo);
    }
    if (selectionType.size() > 0) {
        args += "-s";
        args.append(selectionType);
    }
    if (manualFeatures.size() > 0) {
        args += "-m";
        args.append(manualFeatures);
    }
    if (maxAutomaticFeaturesNo.size() > 0 && maxAutomaticFeaturesNo.toInt() > 0)
    {
        args += "-n";
        args += maxAutomaticFeaturesNo;
    }

    return args;
}



void S4SYieldSUHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                                const JobSubmittedEvent &event)
{
    S4SYieldJobConfig cfg(&ctx, event, processorDescr.shortName);
    S4CMarkersDB1DataExtractStepsBuilder dataExtrStepsBuilder;
    if (cfg.extractFeatures) {
        // we do not provide the patterns as we provide directly the custom lpisInfos
        ParcelsProductDescriptor descr = {cfg.suUniqueId, "", "", "", ""};
        dataExtrStepsBuilder.Initialize(processorDescr.shortName, ctx, cfg.parameters, event.siteId, event.jobId,
                                        {"LAI"}, true, cfg.lpisInfos, descr, cfg.dataExtractionRootDir);
    }

    QList<TaskToSubmit> allTasksList;
    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef = CreateTasks(cfg, allTasksList, dataExtrStepsBuilder);
    SubmitTasks(ctx, cfg.event.jobId, allTasksListRef);
    NewStepList allSteps = CreateSteps(allTasksList, cfg, dataExtrStepsBuilder);
    ctx.SubmitSteps(allSteps);
}

void S4SYieldSUHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                                const TaskFinishedEvent &event)
{
    if (event.module == "product-formatter") {
        const QString &prodName = GetOutputProductName(ctx, event);
        const QString &productFolder =
            GetFinalProductFolder(ctx, event.jobId, event.siteId) + "/" + prodName;
        if (prodName != "") {
            const QString &quicklook = GetProductFormatterQuicklook(ctx, event);
            const QString &footPrint = GetProductFormatterFootprint(ctx, event);
            // Insert the product into the database
            GenericHighLevelProductHelper prdHelper(productFolder);
            int prdId = ctx.InsertProduct({ ProductType::S4SYieldFeatProductTypeId, event.processorId,
                                            event.siteId, event.jobId, productFolder, prdHelper.GetAcqDate(),
                                            prodName, quicklook, footPrint,
                                            std::experimental::nullopt, TileIdList(), ProductIdsList() });
            const QString &prodFolderOutPath =
                ctx.GetOutputPath(event.jobId, event.taskId, event.module,
                                  processorDescr.shortName) +
                "/" + "prd_infos.txt";

            QFile file(prodFolderOutPath);
            if (file.open(QIODevice::ReadWrite)) {
                QTextStream stream(&file);
                stream << prdId << ';' << productFolder << '\n';
            }
            ctx.MarkJobFinished(event.jobId);
            // Now remove the job folder containing temporary files
            // TODO: reinsert this but check why it still remove the folder even if the key is set to 1
            // RemoveJobFolder(ctx, event.jobId, processorDescr.shortName);
        } else {
            ctx.MarkJobFailed(event.jobId);
            Logger::error(
                QStringLiteral("Cannot insert into database the product with name %1 and folder %2")
                    .arg(prodName)
                    .arg(productFolder));
        }
    }
}

ProcessorJobDefinitionParams S4SYieldSUHandler::GetProcessingDefinitionImpl(
    SchedulingContext &ctx,
    int siteId,
    int scheduledDate,
    const ConfigurationParameterValueMap &requestOverrideCfgValues)
{
    ProcessorJobDefinitionParams params;

    QDateTime seasonStartDate;
    QDateTime seasonEndDate;
    // extract the scheduled date
    QDateTime qScheduledDate = QDateTime::fromTime_t(scheduledDate);
    bool success = GetSeasonStartEndDates(ctx, siteId, seasonStartDate, seasonEndDate,
                                          qScheduledDate, requestOverrideCfgValues);
    // if cannot get the season dates
    if (!success) {
        Logger::debug(QStringLiteral("Scheduler Yield Features: Error getting season start dates for "
                                     "site %1 for scheduled date %2!")
                          .arg(siteId)
                          .arg(qScheduledDate.toString()));
        return params;
    }

    QDateTime limitDate = seasonEndDate.addMonths(2);
    if (qScheduledDate > limitDate) {
        Logger::debug(QStringLiteral("Scheduler Yield Features: Error scheduled date %1 greater than the "
                                     "limit date %2 for site %3!")
                          .arg(qScheduledDate.toString())
                          .arg(limitDate.toString())
                          .arg(siteId));
        return params;
    }

    ConfigurationParameterValueMap cfgValues =
        ctx.GetConfigurationParameters(S4S_YIELD_SU_CFG_PREFIX, siteId, requestOverrideCfgValues);
    // we might have an offset in days from starting the downloading products to start the S4C L4A
    // production
    int startSeasonOffset = cfgValues["processor.s4s_perm_crop.start_season_offset"].value.toInt();
    seasonStartDate = seasonStartDate.addDays(startSeasonOffset);

    QDateTime startDate = seasonStartDate;
    QDateTime endDate = qScheduledDate;
    // do not pass anymore the product list but the dates
    params.jsonParameters.append("{ \"scheduled_job\": \"1\", \"start_date\": \"" + startDate.toString("yyyyMMdd") + "\", " +
                                 "\"end_date\": \"" + endDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_start_date\": \"" + seasonStartDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_end_date\": \"" + seasonEndDate.toString("yyyyMMdd") + "\"}");

    // Normally, we need at least 1 product available, the crop mask and the shapefile in order to
    // be able to create a S4C Permanent Crops product but if we do not return here, the schedule block waiting
    // for products (that might never happen)
    bool waitForAvailProcInputs =
        (cfgValues["processor.s4s_perm_crop.sched_wait_proc_inputs"].value.toInt() != 0);
    if ((waitForAvailProcInputs == false) || ((params.productList.size() > 0))) {
        params.isValid = true;
        Logger::debug(
            QStringLiteral("Executing scheduled job. Scheduler extracted for S4C L4A a number "
                           "of %1 products for site ID %2 with start date %3 and end date %4!")
                .arg(params.productList.size())
                .arg(siteId)
                .arg(startDate.toString())
                .arg(endDate.toString()));
    } else {
        Logger::debug(QStringLiteral("Scheduled job for S4S Permanent Crops and site ID %1 with start date %2 "
                                     "and end date %3 will not be executed "
                                     "(productsNo = %4)!")
                          .arg(siteId)
                          .arg(startDate.toString())
                          .arg(endDate.toString())
                          .arg(params.productList.size()));
    }

    return params;
}

QStringList S4SYieldSUHandler::GetProductFormatterArgs(TaskToSubmit &productFormatterTask,
                                                     const S4SYieldJobConfig &cfg, const QStringList &listFiles) {
    QString strTimePeriod = cfg.startDate.toString("yyyyMMddTHHmmss").append("_").append(cfg.endDate.toString("yyyyMMddTHHmmss"));
    QStringList additionalArgs = {"-processor.generic.files"};
    additionalArgs += listFiles;
    return GetDefaultProductFormatterArgs(*(cfg.pCtx), productFormatterTask, cfg.event.jobId, cfg.event.siteId, "S4S_YIELDFEAT", strTimePeriod,
                                         "generic", additionalArgs, true);
}

bool ComparePrdsDates(const Product &prd1, const Product &prd2)
{
    return (prd1.created < prd2.created);
}

QString S4SYieldSUHandler::S4SYieldJobConfig::GetCropTypeProductPath()
{
    ProductList cropTypePrdsList = pCtx->GetProducts(event.siteId, (int)ProductType::S4SCropTypeMappingProductTypeId,
                                                                       startDate, endDate);
    if (cropTypePrdsList.size() == 0) {
        pCtx->MarkJobFailed(event.jobId);
        throw std::runtime_error(QStringLiteral("Yield SU: No crop type products were found in database for site %1 and interval %2 - %3.")
                                 .arg(siteShortName)
                                 .arg(startDate.toString())
                                 .arg(endDate.toString()).toStdString());
    }
    std::sort(cropTypePrdsList.begin(), cropTypePrdsList.end(), ComparePrdsDates);
    return cropTypePrdsList.at(0).fullPath;
}


QString S4SYieldSUHandler::S4SYieldJobConfig::GetProcessorDirValue(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                                    const QString &key, const QString &siteShortName, const QString &procShortName,
                                                    const QString &defVal ) {
    QString value = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, key, S4S_YIELD_SU_CFG_PREFIX);

    if (value.size() == 0) {
        value = defVal;
    }
    value = value.replace("{site}", siteShortName);
    value = value.replace("{processor}", procShortName);

    return value;

}

