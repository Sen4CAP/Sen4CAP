#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "logger.hpp"
#include "processorhandlerhelper.h"
#include "s4s_yieldhandler.hpp"
#include "s4c_utils.hpp"
#include "stepexecutiondecorator.h"

#include "products/generichighlevelproducthelper.h"
using namespace orchestrator::products;

// TODO: These defines shoule be extracted from config
static QStringList YIELD_INPUT_MARKER_NAMES = {"LAI"};

QList<std::reference_wrapper<TaskToSubmit>>
S4SYieldHandler::CreateTasks(QList<TaskToSubmit> &outAllTasksList, const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder)
{
    int curTaskIdx = 0;
    const QList<MarkerType> &enabledMarkers = dataExtrStepsBuilder.GetEnabledMarkers();
    QList<int> mergeTasksIndexes;
    QList<std::reference_wrapper<const TaskToSubmit>> mergeTasks;
    for (const auto &marker: enabledMarkers) {
        // Create data extraction tasks if needed
        int minDataExtrIndex = curTaskIdx;
        dataExtrStepsBuilder.CreateTasks(marker, outAllTasksList, curTaskIdx);
        int maxDataExtrIndex = curTaskIdx-1;

        if (YIELD_INPUT_MARKER_NAMES.contains(marker.marker)) {
            // create the merging tasks if needed
            int mergeTaskIdx = CreateMergeTasks(outAllTasksList, marker.marker.toLower() + "-data-extraction-merge",
                                                    minDataExtrIndex, maxDataExtrIndex, curTaskIdx);
            mergeTasksIndexes.push_back(mergeTaskIdx);
            mergeTasks.append(outAllTasksList[mergeTaskIdx]);
        }
    }

    outAllTasksList.append(TaskToSubmit{"s4s-savitzky-golay", mergeTasks});
    int sgIdx = curTaskIdx++;

    outAllTasksList.append(TaskToSubmit{ "s4s-extract-weather-features", mergeTasks });
    int weatherFeatIdx = curTaskIdx++;
    outAllTasksList.append(TaskToSubmit{ "s4s-merge-weather-features", {outAllTasksList[weatherFeatIdx]}  });
    int mergeWeatherFeatIdx = curTaskIdx++;

    outAllTasksList.append(TaskToSubmit{ "s4s-safy-lut", mergeTasks });
    int safyLutTaskIdx = curTaskIdx++;
    outAllTasksList.append(TaskToSubmit{ "s4s-merge-lai-with-grid", {outAllTasksList[weatherFeatIdx]}  });
    int mergeLaiGridIdx = curTaskIdx++;
    outAllTasksList.append(TaskToSubmit{ "s4s-safy-optim", {outAllTasksList[safyLutTaskIdx], outAllTasksList[mergeLaiGridIdx]} });
    int safyOptimIdx = curTaskIdx++;

    outAllTasksList.append(TaskToSubmit{ "s4s-merge-all-features", {outAllTasksList[sgIdx],
                                                                        outAllTasksList[mergeWeatherFeatIdx],
                                                                        outAllTasksList[safyOptimIdx] }  });
    int mergeAllFeatIdx = curTaskIdx++;
    outAllTasksList.append(TaskToSubmit{ "s4s-yield-features-extraction", {outAllTasksList[mergeAllFeatIdx]} });

    int yieldFearExtrIdx = curTaskIdx++;
    outAllTasksList.append(TaskToSubmit{ "product-formatter", {outAllTasksList[yieldFearExtrIdx]} });

    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    for (TaskToSubmit &task : outAllTasksList) {
        allTasksListRef.append(task);
    }
    return allTasksListRef;
}

NewStepList S4SYieldHandler::CreateSteps(QList<TaskToSubmit> &allTasksList,const S4SYieldJobConfig &cfg,
                                         const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder)
{
    int curTaskIdx = 0;
    NewStepList allSteps;
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
            break;
        }
    }
    if (mdb1File.size() == 0) {
        cfg.pCtx->MarkJobFailed(cfg.event.jobId);
        throw std::runtime_error(
            QStringLiteral(
                "Impossible to create the merged markers file. LAI Marker not enabled in database for MDB1?")
                .toStdString());
    }

    TaskToSubmit &sgTask = allTasksList[curTaskIdx++];
    TaskToSubmit &weatherFeatTask = allTasksList[curTaskIdx++];
    TaskToSubmit &weatherFeatMergeTask = allTasksList[curTaskIdx++];
    TaskToSubmit &safyLutTask = allTasksList[curTaskIdx++];
    TaskToSubmit &mergeLaiGridTask = allTasksList[curTaskIdx++];
    TaskToSubmit &safyOptimTask = allTasksList[curTaskIdx++];
    TaskToSubmit &mergeAllFeatTask = allTasksList[curTaskIdx++];
    TaskToSubmit &yieldFeatTask = allTasksList[curTaskIdx++];
    TaskToSubmit &productFormatterTask = allTasksList[curTaskIdx++];

    // Resulting files from tasks
    const QString &sgLaiPath = sgTask.GetFilePath("sg_lai_outputs.csv");
    const QString &sgCropGrowthIndicesPath = sgTask.GetFilePath("sg_crop_growth_indices.csv");
    const QString &sgYieldLaiFeaturesPath = sgTask.GetFilePath("yield_lai_features.csv");

    const QString &weatherWorkingDirPath = weatherFeatTask.GetFilePath("");

    // Workaround: Althogh created by weather featurs task, we add these here in order to avoid putting them in the same directory
    const QString &outGridToParcelIdsPath = weatherFeatMergeTask.GetFilePath("grid_to_parcels.csv");
    const QString &outParcelIdsToGridPath = weatherFeatMergeTask.GetFilePath("parcels_to_grid.csv");
    const QString &outWeatherFeaturesPath = weatherFeatMergeTask.GetFilePath("weather_raw_features.csv");

    const QString &safyLutRangesFilesDirPath = safyLutTask.GetFilePath("");
    const QString &safyLutOutputDirPath = safyLutTask.GetFilePath("");

    const QString &outMergedLaiGrid = mergeLaiGridTask.GetFilePath("lai_with_grid.csv");

    const QString &safyOptimWorkingDirPath = safyOptimTask.GetFilePath("");
    const QString &safyOptimOutputPath = safyOptimTask.GetFilePath("safy_optim_features.csv");

    const QString &allFeatOutputPath = mergeAllFeatTask.GetFilePath("merged_weather_sg_features.csv");
    const QString &yieldFeaturesOutputPath = yieldFeatTask.GetFilePath("yield_features.csv");

    // we expect the value to be something like /mnt/archive/s4s_yield/{site}/{year}/SAFY_Config/safy_params.json
    const QString &safyParamFile = GetProcessorDirValue(cfg.parameters, cfg.configParameters, "safy_params_path",
                                                        cfg.siteShortName, QString::number(cfg.year));

    // Inputs extraction and reflectances stack tif creation
    const QStringList &sgArgs = GetSGLaiTaskArgs(cfg.year, mdb1File, sgLaiPath, sgCropGrowthIndicesPath, sgYieldLaiFeaturesPath);
    allSteps.append(CreateTaskStep(sgTask, "SavitzkyGolay", sgArgs ));

    const QStringList &weatherFeaturesExtractionArgs = GetWeatherFeaturesTaskArgs(cfg.weatherPrdPaths, cfg.parcelsFilePath,
                                                                                  weatherWorkingDirPath,
                                                                                  outGridToParcelIdsPath, outParcelIdsToGridPath);
    allSteps.append(CreateTaskStep(weatherFeatTask, "WeatherFeatures", weatherFeaturesExtractionArgs));

    const QStringList &weatherFeaturesMergeArgs = GetWeatherFeaturesMergeTaskArgs(weatherWorkingDirPath, outWeatherFeaturesPath);
    allSteps.append(CreateTaskStep(weatherFeatMergeTask, "WeatherFeaturesMerge", weatherFeaturesMergeArgs));

    const QStringList &safyLutArgs = GetSafyLutTaskArgs(cfg.weatherPrdPaths, safyParamFile, safyLutRangesFilesDirPath, safyLutOutputDirPath);
    allSteps.append(CreateTaskStep(safyLutTask, "SafyLut", safyLutArgs));

    const QStringList &mergeLaiGridArgs = GetMergeLaiGridTaskArgs(mdb1File, outParcelIdsToGridPath, outMergedLaiGrid);
    allSteps.append(CreateTaskStep(mergeLaiGridTask, "MergeLaiWitGrid", mergeLaiGridArgs));

    const QStringList &safyOptimArgs = GetSafyOptimTaskArgs(cfg.weatherPrdPaths, cfg.year, outMergedLaiGrid,
                                                            outGridToParcelIdsPath, safyParamFile, safyLutRangesFilesDirPath,
                                                            safyLutOutputDirPath, safyOptimWorkingDirPath, safyOptimOutputPath);
    allSteps.append(CreateTaskStep(safyOptimTask, "SafyOptim", safyOptimArgs));

    const QStringList &allFeatureMergeArgs = GetAllFeaturesMergeTaskArgs(outWeatherFeaturesPath, sgCropGrowthIndicesPath, safyOptimOutputPath,
                                                                          allFeatOutputPath);
    allSteps.append(CreateTaskStep(mergeAllFeatTask, "AllFeaturesMerge", allFeatureMergeArgs));

    const QStringList &yieldFeatExtractionArgs = GetYieldFeaturesTaskArgs(allFeatOutputPath, yieldFeaturesOutputPath);
    allSteps.append(CreateTaskStep(yieldFeatTask, "YieldFeatures", yieldFeatExtractionArgs));

    const QStringList &productFormatterArgs = GetProductFormatterArgs(productFormatterTask, cfg, {yieldFeaturesOutputPath});
    allSteps.append(CreateTaskStep(productFormatterTask, "ProductFormatter", productFormatterArgs));

    return allSteps;
}

int S4SYieldHandler::CreateMergeTasks(QList<TaskToSubmit> &outAllTasksList, const QString &taskName,
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

QString S4SYieldHandler::CreateStepsForFilesMerge(const S4SYieldJobConfig &jobCfg,
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

QStringList S4SYieldHandler::GetSGLaiTaskArgs(int year, const QString &mdb1File, const QString &sgOutFile,
                                              const QString &outCropGrowthIndicesFile, const QString &outLaiMetricsFile)
{
    return {    "--input", mdb1File,
                "--year", QString::number(year),
                "--sg-output", sgOutFile,
                "--indices-output", outCropGrowthIndicesFile,
                "--metrics-output", outLaiMetricsFile
    };
}

QStringList S4SYieldHandler::GetWeatherFeaturesTaskArgs(const QStringList &weatherFiles, const QString &parcelsShp,
                                                        const QString &outDir, const QString &outGridToParcels,
                                                        const QString &outParcelToGrid)
{
    QStringList args = { "-v", parcelsShp, "-o", outDir,
                        "-p", outParcelToGrid, "-g", outGridToParcels};
    args += "-i";
    args.append(weatherFiles);

    return args;
}

QStringList S4SYieldHandler::GetWeatherFeaturesMergeTaskArgs(const QString &inDir, const QString &outWeatherFeatures)
{
    return { "Markers1CsvMerge", "-out", outWeatherFeatures, "-il", inDir };
}

QStringList S4SYieldHandler::GetSafyLutTaskArgs(const QStringList &weatherFiles, const QString &safyParamFile,
                                                const QString &safyParamsRangesDir, const QString &outLutDir)
{
    QStringList args = { "-p", safyParamFile,
                "-r", safyParamsRangesDir,
                "-o", outLutDir};

    args += "-i";
    args.append(weatherFiles);

    return args;
}

QStringList S4SYieldHandler::GetMergeLaiGridTaskArgs(const QString &inputLaiFile, const QString &parcelsToGridFile, const QString &outMergedFile)
{
    return { "Markers1CsvMerge", "-il", inputLaiFile, parcelsToGridFile, "-out", outMergedFile, "-ignnodatecol", "0"};
}

QStringList S4SYieldHandler::GetSafyOptimTaskArgs(const QStringList &weatherFiles, int year, const QString &mergedLaiGrid,
                                                  const QString &gridToParcelsFile, const QString &safyParamsFile,
                                                  const QString &safyParamsRangesFile, const QString &lutDir,
                                                  const QString &workingDir, const QString &outSafyOptimFile)
{
    QStringList args = {    "-y", QString::number(year),
                "-a", mergedLaiGrid,
                "-g", gridToParcelsFile,
                "-p", safyParamsFile,
                "-r", safyParamsRangesFile,
                "-l", lutDir,
                "-w", workingDir,
                "-o", outSafyOptimFile};
    args += "-i";
    args.append(weatherFiles);

    return args;
}

QStringList S4SYieldHandler::GetAllFeaturesMergeTaskArgs(const QString &weatherFeatFile, const QString &sgCropGrowthIndicesFile,
                                                      const QString &safyFeatsFile, const QString &outMergedFeatures)
{
    return { "Markers1CsvMerge", "-il", weatherFeatFile, sgCropGrowthIndicesFile, safyFeatsFile, "-out", outMergedFeatures, "-ignnodatecol", "0"};
}

QStringList S4SYieldHandler::GetYieldFeaturesTaskArgs(const QString &inMergedFeatures, const QString &outYieldFeatures)
{

    return {   "-i", inMergedFeatures, "-o", outYieldFeatures};
}

bool S4SYieldHandler::GetStartEndDatesFromProducts(EventProcessingContext &ctx,
                                                      const JobSubmittedEvent &event,
                                                      QDateTime &startDate,
                                                      QDateTime &endDate,
                                                      QList<ProductDetails> &productDetails)
{
    const auto &parameters = QJsonDocument::fromJson(event.parametersJson.toUtf8()).object();
    const ProductList &prds = GetInputProducts(ctx, parameters, event.siteId, ProductType::L3BProductTypeId);
    productDetails = ProcessorHandlerHelper::GetProductDetails(prds, ctx);

    return ProcessorHandlerHelper::GetIntevalFromProducts(prds, startDate, endDate);
}

void S4SYieldHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                                const JobSubmittedEvent &event)
{
    S4SYieldJobConfig cfg(&ctx, event);

    S4CMarkersDB1DataExtractStepsBuilder dataExtrStepsBuilder;
    dataExtrStepsBuilder.Initialize(processorDescr.shortName, ctx, cfg.parameters, event.siteId, event.jobId, {"LAI"});

    UpdateJobConfigParameters(cfg);

    QList<TaskToSubmit> allTasksList;
    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef = CreateTasks(allTasksList, dataExtrStepsBuilder);
    SubmitTasks(ctx, cfg.event.jobId, allTasksListRef);
    NewStepList allSteps = CreateSteps(allTasksList, cfg, dataExtrStepsBuilder);
    ctx.SubmitSteps(allSteps);
}

void S4SYieldHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
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
                stream << prdId << ";" << productFolder << endl;
            }
            ctx.MarkJobFinished(event.jobId);
            // Now remove the job folder containing temporary files
            RemoveJobFolder(ctx, event.jobId, processorDescr.shortName);
        } else {
            ctx.MarkJobFailed(event.jobId);
            Logger::error(
                QStringLiteral("Cannot insert into database the product with name %1 and folder %2")
                    .arg(prodName)
                    .arg(productFolder));
        }
    }
}

ProcessorJobDefinitionParams S4SYieldHandler::GetProcessingDefinitionImpl(
    SchedulingContext &ctx,
    int siteId,
    int scheduledDate,
    const ConfigurationParameterValueMap &requestOverrideCfgValues)
{
    ProcessorJobDefinitionParams params;
    params.isValid = false;
    params.retryLater = false;

    QDateTime seasonStartDate;
    QDateTime seasonEndDate;
    // extract the scheduled date
    QDateTime qScheduledDate = QDateTime::fromTime_t(scheduledDate);
    bool success = GetSeasonStartEndDates(ctx, siteId, seasonStartDate, seasonEndDate,
                                          qScheduledDate, requestOverrideCfgValues);
    // if cannot get the season dates
    if (!success) {
        Logger::debug(QStringLiteral("Scheduler Permanent Crops: Error getting season start dates for "
                                     "site %1 for scheduled date %2!")
                          .arg(siteId)
                          .arg(qScheduledDate.toString()));
        return params;
    }

    QDateTime limitDate = seasonEndDate.addMonths(2);
    if (qScheduledDate > limitDate) {
        Logger::debug(QStringLiteral("Scheduler Permanent Crops: Error scheduled date %1 greater than the "
                                     "limit date %2 for site %3!")
                          .arg(qScheduledDate.toString())
                          .arg(limitDate.toString())
                          .arg(siteId));
        return params;
    }

    ConfigurationParameterValueMap cfgValues =
        ctx.GetConfigurationParameters(S4S_YIELD_FEATS_CFG_PREFIX, siteId, requestOverrideCfgValues);
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


void S4SYieldHandler::UpdateJobConfigParameters(S4SYieldJobConfig &cfgToUpdate)
{
    if(IsScheduledJobRequest(cfgToUpdate.parameters)) {
        QString strStartDate, strEndDate;
        if (ProcessorHandlerHelper::GetParameterValueAsString(cfgToUpdate.parameters, "start_date", strStartDate) &&
            ProcessorHandlerHelper::GetParameterValueAsString(cfgToUpdate.parameters, "end_date", strEndDate) &&
            cfgToUpdate.parameters.contains("input_products") && cfgToUpdate.parameters["input_products"].toArray().size() == 0) {
            cfgToUpdate.isScheduled = true;
            cfgToUpdate.startDate = ProcessorHandlerHelper::GetDateTimeFromString(strStartDate);
            cfgToUpdate.endDate = ProcessorHandlerHelper::GetDateTimeFromString(strEndDate);
        }
    } else {
        const QStringList &filterProductNames = GetInputProductNames(cfgToUpdate.parameters);
        cfgToUpdate.SetFilteringProducts(filterProductNames);

        QList<ProductDetails> productDetails;
        bool ret = GetStartEndDatesFromProducts(*(cfgToUpdate.pCtx), cfgToUpdate.event, cfgToUpdate.startDate, cfgToUpdate.endDate, productDetails);
        if (!ret || productDetails.size() == 0) {
            // try to get the start and end date if they are given
            cfgToUpdate.pCtx->MarkJobFailed(cfgToUpdate.event.jobId);
            throw std::runtime_error(
                QStringLiteral(
                    "No products provided at input or no products available in the specified interval")
                    .toStdString());
        }
    }
    cfgToUpdate.year = cfgToUpdate.endDate.date().year();           // TODO: see if this is valid
    const QString &parcelsFile = GetParcelsFile(cfgToUpdate);
    cfgToUpdate.SetParcelsFile(parcelsFile);
    const ProductList &weatherPrdsList = cfgToUpdate.pCtx->GetProducts(cfgToUpdate.event.siteId, (int)ProductType::ERA5WeatherProductTypeId,
                                                                       cfgToUpdate.startDate, cfgToUpdate.endDate);
    if (weatherPrdsList.size() == 0) {
        cfgToUpdate.pCtx->MarkJobFailed(cfgToUpdate.event.jobId);
        throw std::runtime_error(QStringLiteral("No weather products were found in database for site %1 and interval %2 - %3.")
                                 .arg(cfgToUpdate.siteShortName)
                                 .arg(cfgToUpdate.startDate.toString())
                                 .arg(cfgToUpdate.endDate.toString()).toStdString());
    }
    cfgToUpdate.SetWeatherProducts(weatherPrdsList);
}

//TODO: We should return here only one LPIS product according to the year
QString S4SYieldHandler::GetParcelsFile(const S4SYieldJobConfig &cfg) {
    // We take it the last LPIS product for this site.
    const ProductList &samplesPrds = S4CUtils::GetLpisProduct(cfg.pCtx, cfg.event.siteId);
    if (samplesPrds.size() == 0) {
        cfg.pCtx->MarkJobFailed(cfg.event.jobId);
        throw std::runtime_error(QStringLiteral("No LPIS product found in database for the yield for site %1.").
                                 arg(cfg.siteShortName).toStdString());
    }

    QDateTime lastPrdInsertedDate;
    QString retParcelsFile;
    for(const Product &samplesPrd: samplesPrds) {
        int insituPrdYear = samplesPrd.created.date().year();
        if (insituPrdYear != cfg.year) {
            continue;
        }
        // ignore LPIS products from a year where we already added an LPIS product newer
        if (lastPrdInsertedDate.isValid() && samplesPrd.inserted < lastPrdInsertedDate) {
            continue;
        }

        QDir directory(samplesPrd.fullPath);
        QRegularExpression reGpkg("in_?situ_.*_\\d{4}.gpkg");
        const QStringList &dirFiles = directory.entryList(QStringList() << "*.gpkg",QDir::Files);
        foreach(const QString &fileName, dirFiles) {
            if (reGpkg.match(fileName).hasMatch())  {
                retParcelsFile = directory.filePath(fileName);
                break;
            }
        }
    }

    return retParcelsFile;
}

bool S4SYieldHandler::IsScheduledJobRequest(const QJsonObject &parameters) {
    int jobVal;
    return ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "scheduled_job", jobVal) && (jobVal == 1);
}

QString S4SYieldHandler::GetProcessorDirValue(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                                    const QString &key, const QString &siteShortName, const QString &year,
                                                    const QString &defVal ) {
    QString dataExtrDirName = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, key, S4S_YIELD_FEATS_CFG_PREFIX);

    if (dataExtrDirName.size() == 0) {
        dataExtrDirName = defVal;
    }
    dataExtrDirName = dataExtrDirName.replace("{site}", siteShortName);
    dataExtrDirName = dataExtrDirName.replace("{year}", year);
    dataExtrDirName = dataExtrDirName.replace("{processor}", processorDescr.shortName);

    return dataExtrDirName;

}

QStringList S4SYieldHandler::GetProductFormatterArgs(TaskToSubmit &productFormatterTask,
                                                     const S4SYieldJobConfig &cfg, const QStringList &listFiles) {
    // ProductFormatter /home/cudroiu/sen2agri-processors-build
    //    -vectprd 1 -destroot /mnt/archive_new/test/Sen4CAP_L4B_Tests/NLD_Validation_TSA/OutPrdFormatter
    //    -fileclass OPER -level S4C_L4B -baseline 01.00 -siteid 4 -timeperiod 20180101_20181231 -processor generic
    //    -processor.generic.files <files_list>

    const auto &targetFolder = GetFinalProductFolder(*(cfg.pCtx), cfg.event.jobId, cfg.event.siteId);
    const auto &outPropsPath = productFormatterTask.GetFilePath(PRODUCT_FORMATTER_OUT_PROPS_FILE);
    const auto &executionInfosPath = productFormatterTask.GetFilePath("executionInfos.txt");
    QString strTimePeriod = cfg.startDate.toString("yyyyMMddTHHmmss").append("_").append(cfg.endDate.toString("yyyyMMddTHHmmss"));
    QStringList productFormatterArgs = { "ProductFormatter",
                                         "-destroot", targetFolder,
                                         "-fileclass", "OPER",
                                         "-level", "S4S_YIELDFEAT",
                                         "-vectprd", "1",
                                         "-baseline", "01.00",
                                         "-siteid", QString::number(cfg.event.siteId),
                                         "-timeperiod", strTimePeriod,
                                         "-processor", "generic",
                                         "-outprops", outPropsPath,
                                         "-gipp", executionInfosPath
                                       };
    productFormatterArgs += "-processor.generic.files";
    productFormatterArgs += listFiles;

    return productFormatterArgs;
}

