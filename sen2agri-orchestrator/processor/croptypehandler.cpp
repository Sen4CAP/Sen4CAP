#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "croptypehandler.hpp"
#include "processorhandlerhelper.h"
#include "logger.hpp"

#include "products/generichighlevelproducthelper.h"
using namespace orchestrator::products;

void CropTypeHandler::GetJobConfig(EventProcessingContext &ctx,const JobSubmittedEvent &event,CropTypeJobConfig &cfg) {
    auto configParameters = ctx.GetJobConfigurationParameters(event.jobId, "processor.l4b.");
    auto l4aConfigParameters = ctx.GetJobConfigurationParameters(event.jobId, "processor.l4a.reference_data_dir");
    auto resourceParameters = ctx.GetJobConfigurationParameters(event.jobId, "resources.working-mem");
    const auto &parameters = QJsonDocument::fromJson(event.parametersJson.toUtf8()).object();

    cfg.jobId = event.jobId;
    cfg.siteId = event.siteId;
    cfg.resolution = 0;
    if(!ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "resolution", cfg.resolution) ||
            cfg.resolution == 0) {
        cfg.resolution = 10;
    }

    cfg.referencePolygons = parameters["reference_polygons"].toString();
    cfg.strataShp = parameters["strata_shape"].toString();
    // if the strata is not set by the user, try to take it from the database
    if(cfg.strataShp.size() == 0) {
        QString siteName = ctx.GetSiteShortName(event.siteId);
        // Get the reference dir
        QString refDir = l4aConfigParameters["processor.l4a.reference_data_dir"];
        refDir = refDir.replace("{site}", siteName);
        QString strataFile;
        if(ProcessorHandlerHelper::GetStrataFile(refDir, strataFile) &&
                QFile::exists(strataFile)) {
            cfg.strataShp = strataFile;
        }
    }

    // get the crop mask
    cfg.cropMask = parameters["crop_mask"].toString();

    cfg.appsMem = resourceParameters["resources.working-mem"];
    cfg.lutPath = configParameters["processor.l4b.lut_path"];

    cfg.randomSeed = configParameters["processor.l4b.random_seed"];
    if(cfg.randomSeed.isEmpty())  cfg.randomSeed = "0";

    cfg.sampleRatio = configParameters["processor.l4b.sample-ratio"];
    if(cfg.sampleRatio.length() == 0) cfg.sampleRatio = "0.75";

    cfg.temporalResamplingMode = configParameters["processor.l4b.temporal_resampling_mode"];
    if(cfg.temporalResamplingMode != "resample") cfg.temporalResamplingMode = "gapfill";

    cfg.classifier = configParameters["processor.l4b.classifier"];
    if(cfg.classifier.length() == 0) cfg.classifier = "rf";

    cfg.fieldName = configParameters["processor.l4b.classifier.field"];
    if(cfg.fieldName.length() == 0) cfg.fieldName = "CODE";

    cfg.classifierRfNbTrees = configParameters["processor.l4b.classifier.rf.nbtrees"];
    if(cfg.classifierRfNbTrees.length() == 0) cfg.classifierRfNbTrees = "100";

    cfg.classifierRfMinSamples = configParameters["processor.l4b.classifier.rf.min"];
    if(cfg.classifierRfMinSamples.length() == 0) cfg.classifierRfMinSamples = "25";

    cfg.classifierRfMaxDepth = configParameters["processor.l4b.classifier.rf.max"];
    if(cfg.classifierRfMaxDepth.length() == 0) cfg.classifierRfMaxDepth = "25";

    cfg.classifierSvmKernel = configParameters["processor.l4b.classifier.svm.k"];
    cfg.classifierSvmOptimize = configParameters["processor.l4b.classifier.svm.opt"];

    cfg.tileThreadsHint = configParameters["processor.l4b.tile-threads-hint"].toInt();
    auto maxParallelism = configParameters["processor.l4b.max-parallelism"].toInt();
    cfg.maxParallelism = maxParallelism > 0 ? std::experimental::optional<int>(maxParallelism) : std::experimental::nullopt;
}

QList<std::reference_wrapper<TaskToSubmit>> CropTypeHandler::CreateTasks(QList<TaskToSubmit> &outAllTasksList) {
    outAllTasksList.append(TaskToSubmit{ "crop-type-fused", {}} );

    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    for(TaskToSubmit &task: outAllTasksList) {
        allTasksListRef.append(task);
    }
    return allTasksListRef;
}

NewStepList CropTypeHandler::CreateSteps(EventProcessingContext &ctx, const JobSubmittedEvent &event,
            QList<TaskToSubmit> &allTasksList, const CropTypeJobConfig &cfg, const QList<ProductDetails> &productDetails) {
    int curTaskIdx = 0;
    NewStepList allSteps;
    TaskToSubmit &cropTypeTask = allTasksList[curTaskIdx++];

    QStringList corpTypeArgs = GetCropTypeTaskArgs(ctx, event, cfg, productDetails, cropTypeTask);
    allSteps.append(CreateTaskStep(cropTypeTask, "CropTypeFused", corpTypeArgs));
    return allSteps;
}

QStringList CropTypeHandler::GetCropTypeTaskArgs(EventProcessingContext &ctx, const JobSubmittedEvent &event,
                    const CropTypeJobConfig &cfg, const QList<ProductDetails> &productDetails, TaskToSubmit &cropTypeTask) {

    const TilesTimeSeries  &mapTiles = ProcessorHandlerHelper::GroupTiles(ctx, event.siteId, productDetails,
                                                                          ProductType::L2AProductTypeId);

    const auto &outputDir = cropTypeTask.GetFilePath("");
    const auto &outPropsPath = cropTypeTask.GetFilePath(PRODUCT_FORMATTER_OUT_PROPS_FILE);

    const auto &targetFolder = GetFinalProductFolder(ctx, cfg.jobId, cfg.siteId);
    QStringList cropTypeArgs = { "-refp", cfg.referencePolygons,
                                 "-ratio", cfg.sampleRatio,
                                 //"-trm", cfg.temporalResamplingMode,
                                 "-classifier", cfg.classifier,
                                 "-rseed", cfg.randomSeed,
                                 "-pixsize", QString::number(cfg.resolution),
                                 "-rfnbtrees", cfg.classifierRfNbTrees,
                                 "-rfmax", cfg.classifierRfMaxDepth,
                                 "-rfmin", cfg.classifierRfMinSamples,
                                 "-tile-threads-hint", QString::number(cfg.tileThreadsHint),
                                 "-siteid", QString::number(cfg.siteId),
                                 "-outdir", outputDir,
                                 "-targetfolder", targetFolder,
                                 "-outprops", outPropsPath};

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
    cropTypeArgs += "-input";
    cropTypeArgs += tilePrimarySatFiles;
    cropTypeArgs += tileSecondarySatFiles;

    if(cfg.cropMask.length() > 0) {
        cropTypeArgs += "-maskprod";
        cropTypeArgs += ctx.GetProductAbsolutePath(event.siteId, cfg.cropMask);
    }

    if(cfg.strataShp.length() > 0) {
        cropTypeArgs += "-strata";
        cropTypeArgs += cfg.strataShp;
    }

    if(cfg.lutPath.size() > 0) {
        cropTypeArgs += "-lut";
        cropTypeArgs += cfg.lutPath;
    }

    if (cfg.maxParallelism) {
        cropTypeArgs += "-max-parallelism";
        cropTypeArgs += QString::number(cfg.maxParallelism.value());
    }

    return cropTypeArgs;
}


void CropTypeHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                              const JobSubmittedEvent &event)
{
    const auto &parameters = QJsonDocument::fromJson(event.parametersJson.toUtf8()).object();
    const ProductList &prds = GetInputProducts(ctx, parameters, event.siteId);
    const QList<ProductDetails> &productDetails = ProcessorHandlerHelper::GetProductDetails(prds, ctx);
    if(productDetails.size() == 0) {
        // try to get the start and end date if they are given
        ctx.MarkJobFailed(event.jobId);
        throw std::runtime_error(
            QStringLiteral("No products provided at input or no products available in the specified interval").
                    toStdString());
    }

    CropTypeJobConfig cfg;
    GetJobConfig(ctx, event, cfg);

    const auto &referencePolygons = cfg.referencePolygons;
    if(referencePolygons.isEmpty()) {
        ctx.MarkJobFailed(event.jobId);
        throw std::runtime_error(
            QStringLiteral("No reference polygons provided!").
                    toStdString());
    }

    // get the crop mask
    if(cfg.cropMask.isEmpty()) {
        // determine the crop mask based on the input products
        QDateTime dtStartDate, dtEndDate;
        if(ProcessorHandlerHelper::GetIntevalFromProducts(prds, dtStartDate, dtEndDate)) {
            const ProductList &l4AProductList = ctx.GetProducts(event.siteId, (int)ProductType::L4AProductTypeId, dtStartDate, dtEndDate);
            if(l4AProductList.size() > 0) {
                // get the last L4A product
                cfg.cropMask = l4AProductList[l4AProductList.size()-1].fullPath;
            }
        }
    }

    QList<TaskToSubmit> allTasksList;
    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef = CreateTasks(allTasksList);
    SubmitTasks(ctx, cfg.jobId, allTasksListRef);
    NewStepList allSteps = CreateSteps(ctx, event, allTasksList, cfg, productDetails);
    ctx.SubmitSteps(allSteps);
}


void CropTypeHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                             const TaskFinishedEvent &event)
{
    if (event.module == "crop-type-fused") {
        const QString &prodName = GetOutputProductName(ctx, event);
        const QString &productFolder = GetFinalProductFolder(ctx, event.jobId, event.siteId) + "/" + prodName;
        GenericHighLevelProductHelper prdHelper(productFolder);
        if(prodName != "" && prdHelper.HasValidStructure()) {
            ctx.MarkJobFinished(event.jobId);
            const QString &quicklook = GetProductFormatterQuicklook(ctx, event);
            const QString &footPrint = GetProductFormatterFootprint(ctx, event);
            // Insert the product into the database
            ctx.InsertProduct({ ProductType::L4BProductTypeId, event.processorId, event.siteId,
                                event.jobId, productFolder, prdHelper.GetAcqDate(), prodName, quicklook,
                                footPrint, std::experimental::nullopt, TileIdList(), ProductIdsList() });

        } else {
            ctx.MarkJobFailed(event.jobId);
            Logger::error(QStringLiteral("Cannot insert into database the product with name %1 and folder %2").arg(prodName).arg(productFolder));
        }
        // Now remove the job folder containing temporary files
        RemoveJobFolder(ctx, event.jobId, "l4b");
    }
}

ProcessorJobDefinitionParams CropTypeHandler::GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues)
{
    ProcessorJobDefinitionParams params;
    params.isValid = false;

    QDateTime seasonStartDate;
    QDateTime seasonEndDate;
    // extract the scheduled date
    QDateTime qScheduledDate = QDateTime::fromTime_t(scheduledDate);
    bool success = GetSeasonStartEndDates(ctx, siteId, seasonStartDate, seasonEndDate, qScheduledDate, requestOverrideCfgValues);
    // if cannot get the season dates
    if(!success) {
        Logger::debug(QStringLiteral("Scheduler CropType: Error getting season start dates for site %1 for scheduled date %2!")
                      .arg(siteId)
                      .arg(qScheduledDate.toString()));
        return params;
    }

    QDateTime limitDate = seasonEndDate.addMonths(2);
    if(qScheduledDate > limitDate) {
        Logger::debug(QStringLiteral("Scheduler CropType: Error scheduled date %1 greater than the limit date %2 for site %3!")
                      .arg(qScheduledDate.toString())
                      .arg(limitDate.toString())
                      .arg(siteId));
        return params;
    }

    ConfigurationParameterValueMap l4aCfgValues = ctx.GetConfigurationParameters("processor.l4a.reference_data_dir", siteId, requestOverrideCfgValues);
    QString siteName = ctx.GetSiteShortName(siteId);
    // Get the reference dir
    QString refDir = l4aCfgValues["processor.l4a.reference_data_dir"].value;
    refDir = refDir.replace("{site}", siteName);

    ConfigurationParameterValueMap cfgValues = ctx.GetConfigurationParameters("processor.l4b.", siteId, requestOverrideCfgValues);
    // we might have an offset in days from starting the downloading products to start the L4B production
    int startSeasonOffset = cfgValues["processor.l4b.start_season_offset"].value.toInt();
    seasonStartDate = seasonStartDate.addDays(startSeasonOffset);

    QString shapeFile;
    QString referenceRasterFile;
    QString strataShapeFile;
    // if none of the reference files were found, cannot run the CropMask
    if(!ProcessorHandlerHelper::GetCropReferenceFile(refDir, shapeFile, referenceRasterFile, strataShapeFile)) {
        Logger::debug(QStringLiteral("Scheduler CropType: Error no shapefile found for site %1!")
                      .arg(siteId));
        return params;
    }
    QString refStr = "\"reference_polygons\": \"\"";
    if(!shapeFile.isEmpty()) {
        refStr = "\"reference_polygons\": \"" + shapeFile + "\"";
    }
    if(!strataShapeFile.isEmpty()) {
        refStr.append(", \"strata_shape\": \"" + strataShapeFile + "\"");
    }

    QString cropMaskFolder;
    QDateTime startDate = seasonStartDate;
    QDateTime endDate = qScheduledDate;
    ProductList l4AProductList = ctx.GetProducts(siteId, (int)ProductType::L4AProductTypeId, startDate, endDate);
    // get the last created Crop Mask
    QDateTime maxDate;
    for(int i = 0; i<l4AProductList.size(); i++) {
        if(!maxDate.isValid() || (maxDate < l4AProductList[i].created)) {
            cropMaskFolder = l4AProductList[i].fullPath;
            maxDate = l4AProductList[i].created;
        }
    }
    params.jsonParameters = "{ \"crop_mask\": \"" + cropMaskFolder + "\", " + refStr + "}";
    params.productList = ctx.GetProducts(siteId, (int)ProductType::L2AProductTypeId, startDate, endDate);
    // Normally, we need at least 1 product available, the crop mask and the shapefile in order to be able to create a L4B product
    // but if we do not return here, the schedule block waiting for products (that might never happen)
    bool waitForAvailProcInputs = (cfgValues["processor.l4b.sched_wait_proc_inputs"].value.toInt() != 0);
    if((waitForAvailProcInputs == false) ||
            ((params.productList.size() > 0) && !cropMaskFolder.isEmpty() && !shapeFile.isEmpty())) {
        params.isValid = true;
        Logger::debug(QStringLiteral("Executing scheduled job. Scheduler extracted for L4B a number "
                                     "of %1 products for site ID %2 with start date %3 and end date %4!")
                      .arg(params.productList.size())
                      .arg(siteId)
                      .arg(startDate.toString())
                      .arg(endDate.toString()));
    } else {
        Logger::debug(QStringLiteral("Scheduled job for L4B and site ID %1 with start date %2 and end date %3 will not be executed "
                                     "(productsNo = %4, cropMask = %5, shapeFile = %6)!")
                      .arg(siteId)
                      .arg(startDate.toString())
                      .arg(endDate.toString())
                      .arg(params.productList.size())
                      .arg(cropMaskFolder)
                      .arg(shapeFile));
    }

    return params;
}
