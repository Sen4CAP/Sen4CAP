#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "cropmaskhandler.hpp"
#include "processorhandlerhelper.h"
#include "logger.hpp"

#include "products/generichighlevelproducthelper.h"
using namespace orchestrator::products;

void CropMaskHandler::SetProcessorDescription(const ProcessorDescription &procDescr) {
    this->processorDescr = procDescr;
}


void CropMaskHandler::GetJobConfig(EventProcessingContext &ctx,const JobSubmittedEvent &event,CropMaskJobConfig &cfg) {
    auto configParameters = ctx.GetJobConfigurationParameters(event.jobId, "processor.l4a.");
    auto resourceParameters = ctx.GetJobConfigurationParameters(event.jobId, "resources.working-mem");
    const auto &parameters = QJsonDocument::fromJson(event.parametersJson.toUtf8()).object();

    const ProductList &prds = GetInputProducts(ctx, parameters, event.siteId, ProductType::L2AProductTypeId,
                                               &cfg.startDate, &cfg.endDate);
    cfg.productDetails = ProcessorHandlerHelper::GetProductDetails(prds, ctx);
    if(cfg.productDetails.size() == 0) {
        ctx.MarkJobFailed(event.jobId);
        throw std::runtime_error(
            QStringLiteral("No products provided at input or no products available in the specified interval").
                    toStdString());
    }
    cfg.jobId = event.jobId;
    cfg.siteId = event.siteId;
    cfg.resolution = 0;
    if(!ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "resolution", cfg.resolution) ||
            cfg.resolution == 0) {
        cfg.resolution = 10;
    }

    QString siteName = ctx.GetSiteShortName(event.siteId);
    cfg.referenceDataSource = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "reference_data_source", S2A_CM_PREFIX);
    if (cfg.referenceDataSource == "earth_signature") {
        cfg.referencePolygons = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "earth_signature_insitu", S2A_CM_PREFIX);
        cfg.referencePolygons = cfg.referencePolygons.replace("{site}", siteName);
    } else {

        cfg.referencePolygons = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "reference_polygons", S2A_CM_PREFIX);
        cfg.strataShp = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "strata_shape", S2A_CM_PREFIX);
        // if the strata is not set by the user, try to take it from the database
        if(cfg.strataShp.size() == 0) {
            QString siteName = ctx.GetSiteShortName(event.siteId);
            // Get the reference dir
            QString refDir = configParameters["processor.l4a.reference_data_dir"];
            refDir = refDir.replace("{site}", siteName);
            QString strataFile;
            if(ProcessorHandlerHelper::GetStrataFile(refDir, strataFile) &&
                    QFile::exists(strataFile)) {
                cfg.strataShp = strataFile;
            }
        }

        cfg.referenceRaster = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "reference-map", S2A_CM_PREFIX);
    }

    cfg.lutPath = configParameters["processor.l4a.lut_path"];
    cfg.skipSegmentation = configParameters["processor.l4a.skip-segmentation"] == "true";
    cfg.appsMem = resourceParameters["resources.working-mem"];

    cfg.randomSeed = configParameters["processor.l4a.random_seed"];
    if(cfg.randomSeed.isEmpty())  cfg.randomSeed = "0";

    cfg.sampleRatio = configParameters["processor.l4a.sample-ratio"];
    if(cfg.sampleRatio.length() == 0) cfg.sampleRatio = "0.75";

    cfg.temporalResamplingMode = configParameters["processor.l4a.temporal_resampling_mode"];
    if(cfg.temporalResamplingMode != "resample") cfg.temporalResamplingMode = "gapfill";

    cfg.window = configParameters["processor.l4a.window"];
    if(cfg.window.length() == 0) cfg.window = "6";

    cfg.nbcomp = configParameters["processor.l4a.nbcomp"];
    if(cfg.nbcomp.length() == 0) cfg.nbcomp = "6";

    cfg.spatialr = configParameters["processor.l4a.segmentation-spatial-radius"];
    if(cfg.spatialr.length() == 0) cfg.spatialr = "10";

    cfg.ranger = configParameters["processor.l4a.range-radius"];
    if(cfg.ranger.length() == 0) cfg.ranger = "0.65";

    cfg.minsize = configParameters["processor.l4a.segmentation-minsize"];
    if(cfg.minsize.length() == 0) cfg.minsize = "10";

    cfg.minarea = configParameters["processor.l4a.min-area"];
    if(cfg.minarea.length() == 0) cfg.minarea = "20";

    cfg.classifier = configParameters["processor.l4a.classifier"];
    if(cfg.classifier.length() == 0) cfg.classifier = "rf";

    cfg.fieldName = configParameters["processor.l4a.classifier.field"];
    if(cfg.fieldName.length() == 0) cfg.fieldName = "CROP";

    cfg.classifierRfNbTrees = configParameters["processor.l4a.classifier.rf.nbtrees"];
    if(cfg.classifierRfNbTrees.length() == 0) cfg.classifierRfNbTrees = "100";

    cfg.classifierRfMinSamples = configParameters["processor.l4a.classifier.rf.min"];
    if(cfg.classifierRfMinSamples.length() == 0) cfg.classifierRfMinSamples = "25";

    cfg.classifierRfMaxDepth = configParameters["processor.l4a.classifier.rf.max"];
    if(cfg.classifierRfMaxDepth.length() == 0) cfg.classifierRfMaxDepth = "25";

    cfg.classifierSvmKernel = configParameters["processor.l4a.classifier.svm.k"];
    cfg.classifierSvmOptimize = configParameters["processor.l4a.classifier.svm.opt"];

    cfg.nbtrsample = configParameters["processor.l4a.training-samples-number"];
    if(cfg.nbtrsample.length() == 0) cfg.nbtrsample = "40000";

    cfg.lmbd = configParameters["processor.l4a.smoothing-lambda"];
    if(cfg.lmbd.length() == 0) cfg.lmbd = "2";

    cfg.erode_radius = configParameters["processor.l4a.erode-radius"];
    if(cfg.erode_radius.length() == 0) cfg.erode_radius = "1";

    cfg.alpha = configParameters["processor.l4a.mahalanobis-alpha"];
    if(cfg.alpha.length() == 0) cfg.alpha = "0.01";

    cfg.tileThreadsHint = configParameters["processor.l4a.tile-threads-hint"].toInt();
    auto maxParallelism = configParameters["processor.l4a.max-parallelism"].toInt();
    cfg.maxParallelism = maxParallelism > 0 ? std::experimental::optional<int>(maxParallelism) : std::experimental::nullopt;
}

QList<std::reference_wrapper<TaskToSubmit>> CropMaskHandler::CreateTasks(const CropMaskJobConfig &jobCfg,
                                                                         QList<TaskToSubmit> &outAllTasksList) {
    QList<std::reference_wrapper<const TaskToSubmit>> cmParentTasks;
    if(jobCfg.referenceDataSource == "earth_signature") {
        outAllTasksList.append(TaskToSubmit{ "earth-signature", {}} );
        cmParentTasks.append(outAllTasksList[0]);
    }
    outAllTasksList.append(TaskToSubmit{ "crop-mask-fused", cmParentTasks} );

    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    for(TaskToSubmit &task: outAllTasksList) {
        allTasksListRef.append(task);
    }
    return allTasksListRef;
}

NewStepList CropMaskHandler::CreateSteps(EventProcessingContext &ctx, const JobSubmittedEvent &event,
              QList<TaskToSubmit> &allTasksList, const CropMaskJobConfig &cfg) {
    int curTaskIdx = 0;
    NewStepList allSteps;
    if(cfg.referenceDataSource == "earth_signature") {
        TaskToSubmit &esTask = allTasksList[curTaskIdx++];
        const QStringList &esArgs = GetEarthSignatureTaskArgs(cfg, esTask);
        allSteps.append(CreateTaskStep(esTask, "EarthSignature", esArgs));
    }

    TaskToSubmit &cropMaskTask = allTasksList[curTaskIdx++];

    QStringList corpTypeArgs = GetCropMaskTaskArgs(ctx, event, cfg, cropMaskTask);
    allSteps.append(CreateTaskStep(cropMaskTask, "CropMaskFused", corpTypeArgs));
    return allSteps;
}

QStringList CropMaskHandler::GetEarthSignatureTaskArgs(const CropMaskJobConfig &cfg, TaskToSubmit &earthSignatureTask) {

    const auto &workingDir = earthSignatureTask.GetFilePath("");
    QStringList esArgs = {
                            "--site-id", QString::number(cfg.siteId),
                            "--working-dir", workingDir,
                            "--out-shp", cfg.referencePolygons,
                            "--start-date", cfg.startDate.toString("yyyy-MM-dd"),
                            "--end-date", cfg.endDate.toString("yyyy-MM-dd")
                         };
    return esArgs;
}

QStringList CropMaskHandler::GetCropMaskTaskArgs(EventProcessingContext &ctx, const JobSubmittedEvent &event,
                          const CropMaskJobConfig &cfg, TaskToSubmit &cropMaskTask) {

    const TilesTimeSeries  &mapTiles = GroupL2ATiles(ctx, cfg.productDetails);

    const auto &outputDir = cropMaskTask.GetFilePath("");
    const auto &outPropsPath = cropMaskTask.GetFilePath(PRODUCT_FORMATTER_OUT_PROPS_FILE);

    const auto &targetFolder = GetFinalProductFolder(ctx, cfg.jobId, cfg.siteId);
    QStringList cropMaskArgs = { "-ratio", cfg.sampleRatio,
                                 "-nbtrsample", cfg.nbtrsample,
                                 "-classifier", cfg.classifier,
                                 "-rseed", cfg.randomSeed,
                                 "-pixsize", QString::number(cfg.resolution),
                                 "-rfnbtrees", cfg.classifierRfNbTrees,
                                 "-rfmax", cfg.classifierRfMaxDepth,
                                 "-rfmin", cfg.classifierRfMinSamples,
                                 "-window", cfg.window,
                                 "-lmbd", cfg.lmbd,
                                 "-eroderad", cfg.erode_radius,
                                 "-alpha", cfg.alpha,
                                 "-nbcomp", cfg.nbcomp,
                                 "-spatialr", cfg.spatialr,
                                 "-ranger", cfg.ranger,
                                 "-minsize", cfg.minsize,
                                 "-minarea", cfg.minarea,
                                 "-tile-threads-hint", QString::number(cfg.tileThreadsHint),
                                 "-siteid", QString::number(cfg.siteId),
                                 "-outdir", outputDir,
                                 "-targetfolder", targetFolder,
                                 "-outprops", outPropsPath};

    if(cfg.referencePolygons.length() > 0) {
        cropMaskArgs += "-refp";
        cropMaskArgs += cfg.referencePolygons;
    } else if(cfg.referenceRaster.length() > 0) {
        cropMaskArgs += "-refr";
        cropMaskArgs += cfg.referenceRaster;
    }

    if (cfg.maxParallelism) {
        cropMaskArgs += "-max-parallelism";
        cropMaskArgs += QString::number(cfg.maxParallelism.value());
    }

    // normally, we can use only one list by we want (not necessary) to have the
    // secondary satellite tiles after the main satellite tiles
    QStringList tilePrimarySatFiles;
    QStringList tileSecondarySatFiles;
    for(auto tileId : mapTiles.GetTileIds())
    {
        // get the temporal infos for the current tile
        const TileTimeSeriesInfo &listTemporalTiles = mapTiles.GetTileTimeSeriesInfo(tileId);
        for(const InfoTileFile &fileInfo: listTemporalTiles.temporalTilesFileInfos) {
           if(fileInfo.satId == listTemporalTiles.primarySatelliteId) {
                if(!tilePrimarySatFiles.contains(fileInfo.metaFile)) {
                    tilePrimarySatFiles.append(fileInfo.metaFile);
                }
           } else {
               if(!tileSecondarySatFiles.contains(fileInfo.metaFile)) {
                    tileSecondarySatFiles.append(fileInfo.metaFile);
               }
           }
        }
    }
    // TODO: Provide here also via a parameter also the validity masks
    cropMaskArgs += "-input";
    cropMaskArgs += tilePrimarySatFiles;
    cropMaskArgs += tileSecondarySatFiles;

    if(cfg.strataShp.length() > 0) {
        cropMaskArgs += "-strata";
        cropMaskArgs += cfg.strataShp;
    }

    if(cfg.lutPath.size() > 0) {
        cropMaskArgs += "-lut";
        cropMaskArgs += cfg.lutPath;
    }

    if (cfg.skipSegmentation) {
        cropMaskArgs += "-skip-segmentation";
    }

    return cropMaskArgs;
}

void CropMaskHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                              const JobSubmittedEvent &event)
{
    CropMaskJobConfig cfg;
    GetJobConfig(ctx, event, cfg);

    QList<TaskToSubmit> allTasksList;
    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef = CreateTasks(cfg, allTasksList);
    SubmitTasks(ctx, cfg.jobId, allTasksListRef);
    NewStepList allSteps = CreateSteps(ctx, event, allTasksList, cfg);
    ctx.SubmitSteps(allSteps);
}

void CropMaskHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                             const TaskFinishedEvent &event)
{
    if (event.module == "crop-mask-fused") {
        const QString &prodName = GetOutputProductName(ctx, event);
        const QString &productFolder = GetFinalProductFolder(ctx, event.jobId, event.siteId) + "/" + prodName;
        if(prodName != "" && GenericHighLevelProductHelper(productFolder).HasValidStructure()) {
            ctx.MarkJobFinished(event.jobId);
            const QString &quicklook = GetProductFormatterQuicklook(ctx, event);
            const QString &footPrint = GetProductFormatterFootprint(ctx, event);
            // Insert the product into the database
            GenericHighLevelProductHelper prdHelper(productFolder);
            ctx.InsertProduct({ ProductType::L4AProductTypeId, event.processorId,
                                event.siteId, event.jobId, productFolder, prdHelper.GetAcqDate(),
                                prodName, quicklook, footPrint, std::experimental::nullopt, TileIdList(), ProductIdsList() });
        } else {
            ctx.MarkJobFailed(event.jobId);
            Logger::error(QStringLiteral("Cannot insert into database the product with name %1 and folder %2").arg(prodName).arg(productFolder));
        }
        // Now remove the job folder containing temporary files
        RemoveJobFolder(ctx, event.jobId, "l4a");
    }
}

ProcessorJobDefinitionParams CropMaskHandler::GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues)
{
    ProcessorJobDefinitionParams params;

    QDateTime seasonStartDate;
    QDateTime seasonEndDate;
    // extract the scheduled date
    QDateTime qScheduledDate = QDateTime::fromTime_t(scheduledDate);
    bool success = GetSeasonStartEndDates(ctx, siteId, seasonStartDate, seasonEndDate, qScheduledDate, requestOverrideCfgValues);
    // if cannot get the season dates
    if(!success) {
        Logger::debug(QStringLiteral("Scheduler CropMask: Error getting season start dates for site %1 for scheduled date %2!")
                      .arg(siteId)
                      .arg(qScheduledDate.toString()));
        return params;
    }

    QDateTime limitDate = seasonEndDate.addMonths(2);
    if(qScheduledDate > limitDate) {
        Logger::debug(QStringLiteral("Scheduler CropMask: Error scheduled date %1 greater than the limit date %2 for site %3!")
                      .arg(qScheduledDate.toString())
                      .arg(limitDate.toString())
                      .arg(siteId));
        return params;
    }

    ConfigurationParameterValueMap cfgValues = ctx.GetConfigurationParameters("processor.l4a.", siteId, requestOverrideCfgValues);
    QString siteName = ctx.GetSiteShortName(siteId);
    // Get the reference dir
    QString refMap = cfgValues["processor.l4a.reference-map"].value;
    QString refDir = cfgValues["processor.l4a.reference_data_dir"].value;
    refDir = refDir.replace("{site}", siteName);

    // we might have an offset in days from starting the downloading products to start the L4A production
    int startSeasonOffset = cfgValues["processor.l4a.start_season_offset"].value.toInt();
    seasonStartDate = seasonStartDate.addDays(startSeasonOffset);

    QString shapeFile;
    QString referenceRasterFile;
    QString strataShapeFile;
    // if none of the reference files were found, cannot run the CropMask
    if(!ProcessorHandlerHelper::GetCropReferenceFile(refDir, shapeFile, referenceRasterFile, strataShapeFile) && !QFile::exists(refMap)) {
        params.schedulingFlags = SchedulingFlags::SCH_FLG_RETRY_LATER;
        Logger::debug(QStringLiteral("Scheduled job for L4A and site ID %1 with scheduled date %2 will "
                                     "not be executed (retried later due to no reference file found)!")
                      .arg(siteId)
                      .arg(qScheduledDate.toString()));
        return params;
    }
    if (!shapeFile.isEmpty()) {
        params.jsonParameters = "{ \"reference_polygons\": \"" + shapeFile + "\"";
    } else if (!refMap.isEmpty()) {
        params.jsonParameters = "{ \"reference_raster\": \"" + refMap + "\"";
    } else {
        params.jsonParameters = "{ \"reference_raster\": \"" + referenceRasterFile + "\"";
    }
    if(!strataShapeFile.isEmpty()) {
        params.jsonParameters.append(", \"strata_shape\": \"" + strataShapeFile + "\"}");
    } else {
        params.jsonParameters.append("}");
    }

    // Get the start and end date for the production
    QDateTime endDate = qScheduledDate;
    QDateTime startDate = seasonStartDate;

    // get from the database the moving window for 1 year periodicity
    QString movingWindowStr = cfgValues["processor.l4a.moving_window_value"].value;
    int movingWindow = 0;
    if(movingWindowStr.length() > 0) {
        movingWindow = movingWindowStr.toInt();
    } else {
        movingWindow = 12;
    }
    // ensure we have at least one month of data
    if (seasonStartDate.addMonths(1) > endDate) {
        endDate = seasonStartDate.addMonths(1);
    }
    // we have monthly production so we use the season date or the moving window of 12 months (or specified)
    if(endDate.addMonths(-movingWindow) > seasonStartDate) {
        startDate = endDate.addMonths(-movingWindow);
    } else {
        startDate = seasonStartDate;
    }

    if (!CheckAllAncestorProductCreation(ctx, siteId, ProductType::L4AProductTypeId, startDate, endDate)) {
        // do not trigger yet the schedule.
        params.schedulingFlags = SchedulingFlags::SCH_FLG_RETRY_LATER;
        Logger::debug(QStringLiteral("Scheduled job for L4A and site ID %1 with start date %2 and end date %3 will "
                                     "not be executed (retried later due to no products or not all inputs pre-processed)!")
                      .arg(siteId)
                      .arg(startDate.toString())
                      .arg(endDate.toString()));
    } else {
        params.productList = ctx.GetProducts(siteId, (int)ProductType::L2AProductTypeId, startDate, endDate);
        params.isValid = true;
        Logger::debug(QStringLiteral("Executing scheduled job. Scheduler extracted for L4A a number "
                                     "of %1 products for site ID %2 with start date %3 and end date %4!")
                      .arg(params.productList.size())
                      .arg(siteId)
                      .arg(startDate.toString())
                      .arg(endDate.toString()));
    }
    return params;
}

