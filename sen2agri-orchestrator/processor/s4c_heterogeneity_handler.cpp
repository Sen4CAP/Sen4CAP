#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "logger.hpp"
#include "processorhandlerhelper.h"
#include "s4c_heterogeneity_handler.hpp"
#include "s4c_utils.hpp"
#include "stepexecutiondecorator.h"

#include "products/producthelperfactory.h"

#include "products/generichighlevelproducthelper.h"

using namespace orchestrator::products;

QList<QString> s2Bands = {"B02", "B03", "B04", "B08", "NDVI"};
QList<QString> s1Bands = {"ASC_VV_COHE", "ASC_VH_COHE", "DESC_VV_COHE", "DESC_VH_COHE",
                          "ASC_VV_BCK", "ASC_VH_BCK", "DESC_VV_BCK", "DESC_VH_BCK"};

S4CHeterogeneityHandler::S4CHeterogeneityHandler()
{
}

QList<std::reference_wrapper<TaskToSubmit>>
S4CHeterogeneityHandler::CreateTasks(const S4CHeterogneneityJobConfig &cfg, QList<TaskToSubmit> &outAllTasksList)
{
    int curTaskIdx = -1;
    int ctTaskIdx = -1;
    QList<std::reference_wrapper<const TaskToSubmit>> prdFormatterParentTasks;
    QList<std::reference_wrapper<const TaskToSubmit>> s2ClustAnalysisTasks;
    QList<std::reference_wrapper<const TaskToSubmit>> s1ClustAnalysisTasks;
    QList<std::reference_wrapper<const TaskToSubmit>> finalAnalysisTasks;

    if (cfg.existingCTSARDir.size() == 0) {
        // Extract the L4A parcels and S1 products (tiles.csv and radar.csv). We use the same task ids like for L4A (s4c_croptypehandler)
        outAllTasksList.append(TaskToSubmit{ "s4c-l4a-extract-parcels", {} });
        outAllTasksList.append(TaskToSubmit{ "s4c-heterog-crop-type", { outAllTasksList[0] } });
        curTaskIdx += 2;
        ctTaskIdx = curTaskIdx;
    }

    // In parallel with the S1 steps, we do the S2 temporal resampling
    // create the gdalbuildvrt steps
    for(auto info : cfg.tileInfos.keys()) {
        QList<int> prevS2Ids/*, prevS1Ids*/;

        // Iterate all S2 masks, including NDVI, and create the vrts for rasters and masks + perform temporal resampling
        for(int i = 0; i<s2Bands.size(); i++) {
            outAllTasksList.append(TaskToSubmit{ "gdalbuildvrt", {} });
            outAllTasksList.append(TaskToSubmit{ "gdalbuildvrt", {} });
            outAllTasksList.append(TaskToSubmit{ "s4c-temporal-resampling", {outAllTasksList[++curTaskIdx], outAllTasksList[++curTaskIdx]} });
            prevS2Ids.append(++curTaskIdx);
        }
        // S1 extract S1 list and create a raster
//        for(int i = 0; i<s1Bands.size(); i++) {
//            outAllTasksList.append(TaskToSubmit{ "s4c-heterog-extract-s1-list", {outAllTasksList[ctTaskIdx]} });
//            curTaskIdx++;
//            outAllTasksList.append(TaskToSubmit{ "gdalbuildvrt", {outAllTasksList[curTaskIdx++]} });
//            outAllTasksList.append(TaskToSubmit{ "gdal_translate", {outAllTasksList[curTaskIdx++]} });
//            prevS1Ids.append(curTaskIdx);
//        }

        // Cluster preparation
        for (int period = 0; period < cfg.clusteringIntervals; period++) {
            outAllTasksList.append(TaskToSubmit{ "s4c-cluster-preparation-s2", {} });
            int clustPrepIdx = ++curTaskIdx;
            for (const auto &curIdx : prevS2Ids) {
                outAllTasksList[clustPrepIdx].parentTasks.append(outAllTasksList[curIdx]);
            }
            outAllTasksList.append(TaskToSubmit{ "s4c-remove-isolated-pixels", {outAllTasksList[curTaskIdx++]} });
            outAllTasksList.append(TaskToSubmit{ "s4c-spatial-connectivity", {outAllTasksList[curTaskIdx++]} });
            outAllTasksList.append(TaskToSubmit{ "s4c-cluster-analysis-s2", {outAllTasksList[curTaskIdx++]} });
            // prdFormatterParentTasks.append(outAllTasksList[curTaskIdx]);
            s2ClustAnalysisTasks.append(outAllTasksList[curTaskIdx]);

            // Add also the S1 steps
            if (ctTaskIdx == -1) {
                outAllTasksList.append(TaskToSubmit{ "s4c-cluster-preparation-s1", {} });
            } else {
                outAllTasksList.append(TaskToSubmit{ "s4c-cluster-preparation-s1", {outAllTasksList[ctTaskIdx]} });
            }
            curTaskIdx++;
//            for (const auto &curIdx : prevS1Ids) {
//                outAllTasksList[clustPrepS1Idx].parentTasks.append(outAllTasksList[curIdx]);
//            }
            outAllTasksList.append(TaskToSubmit{ "s4c-remove-isolated-pixels", {outAllTasksList[curTaskIdx++]} });
            outAllTasksList.append(TaskToSubmit{ "s4c-spatial-connectivity", {outAllTasksList[curTaskIdx++]} });
            outAllTasksList.append(TaskToSubmit{ "s4c-cluster-analysis-s1", {outAllTasksList[curTaskIdx++]} });

            // prdFormatterParentTasks.append(outAllTasksList[curTaskIdx]);
            s1ClustAnalysisTasks.append(outAllTasksList[curTaskIdx]);
        }
    }
    // merge cluster analysis for all tiles tasks
    for (int period = 0; period < cfg.clusteringIntervals; period++) {
        outAllTasksList.append(TaskToSubmit{ "s4c-cluster-tiles-analysis-merge", s2ClustAnalysisTasks });
        finalAnalysisTasks.append(outAllTasksList[++curTaskIdx]);
        outAllTasksList.append(TaskToSubmit{ "s4c-cluster-tiles-analysis-merge", s1ClustAnalysisTasks });
        finalAnalysisTasks.append(outAllTasksList[++curTaskIdx]);
    }

    // final period S2 analysis
    outAllTasksList.append(TaskToSubmit{ "s4c-heterog-period-analysis", finalAnalysisTasks });
    prdFormatterParentTasks.append(outAllTasksList[++curTaskIdx]);

    outAllTasksList.append(TaskToSubmit{ "product-formatter", prdFormatterParentTasks });

    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    for (TaskToSubmit &task : outAllTasksList) {
        allTasksListRef.append(task);
    }
    return allTasksListRef;
}

NewStepList S4CHeterogeneityHandler::CreateSteps(QList<TaskToSubmit> &allTasksList, const S4CHeterogneneityJobConfig &cfg)
{
    int curTaskIdx = 0;
    NewStepList allSteps;
    QStringList prdFormatterFiles;
    QMap<int, QStringList> s2PeriodAnalysisTiles;
    QMap<int, QStringList> s1PeriodAnalysisTiles;

    QString ctFilesPath;
    if (cfg.existingCTSARDir.size() == 0) {
        TaskToSubmit &extractParcelsTask = allTasksList[curTaskIdx++];
        TaskToSubmit &cropTypeTask = allTasksList[curTaskIdx++];

        const QString &parcelsPath = extractParcelsTask.GetFilePath("parcels.csv");
        const QString &lutPath = extractParcelsTask.GetFilePath("lut.csv");
        const QString &tilesPath = extractParcelsTask.GetFilePath("tiles.csv");
        const QString &opticalPath = extractParcelsTask.GetFilePath("optical.csv");
        const QString &radarPath = extractParcelsTask.GetFilePath("radar.csv");
        const QString &lpisPath = extractParcelsTask.GetFilePath("lpis.txt");

        const QStringList &extractParcelsArgs = GetExtractParcelsTaskArgs(cfg, parcelsPath, lutPath, tilesPath,
                                                                          opticalPath, radarPath, lpisPath);
        allSteps.append(CreateTaskStep(extractParcelsTask, "S4CCropTypeExtractParcels", extractParcelsArgs));

        ctFilesPath= cropTypeTask.GetFilePath("");
        const QStringList &cropTypeArgs = GetCropTypeTaskArgs(cfg, ctFilesPath, tilesPath, radarPath, lpisPath);
        allSteps.append(CreateTaskStep(cropTypeTask, "S4CCropType", cropTypeArgs));
    } else {
        ctFilesPath = cfg.existingCTSARDir;
    }

    for(auto tile : cfg.tileInfos.keys()) {
        const auto &mapRasters = cfg.tileInfos.value(tile).mapRasters;
        const auto &mapMasks = cfg.tileInfos.value(tile).mapMasks;
        const auto &mapDates = cfg.tileInfos.value(tile).mapDates;

        QStringList temporalResamplingFiles;
        // We iterate by the S2 bands and not the keys to preserve the order and have NDVI always last
        QString ndviResampledRaster;
        for(auto band: s2Bands) {
            if (!mapRasters.keys().contains(band)) {
                Logger::error(QStringLiteral("Heterogeneity: Cannot find band %1 into map keys %2").arg(band).arg(mapRasters.keys().join(",")));
                continue; // theoretically this should not happen
            }
            const auto &listRasters = mapRasters.value(band);
            const auto &listMasks = mapMasks.value(band);
            const auto &listDates = mapDates.value(band);

            TaskToSubmit &s2ExtractInputsTask = allTasksList[curTaskIdx++];
            TaskToSubmit &s2ExtractMasksTask = allTasksList[curTaskIdx++];
            TaskToSubmit &s2TemporalResTask = allTasksList[curTaskIdx++];

            const QString &s2InputsPath = s2ExtractInputsTask.GetFilePath(tile + "_" + band + "_S2.vrt");
            const QString &s2MskPath = s2ExtractMasksTask.GetFilePath(tile + "_" + band + "_msks.vrt");

            const QStringList &s2InputsTaskArgs = GetGdalBuidVrtTaskArgs(listRasters, s2InputsPath);
            const QStringList &s2MasksTaskArgs = GetGdalBuidVrtTaskArgs(listMasks, s2MskPath);
            allSteps.append(CreateTaskStep(s2ExtractInputsTask, "BuildRastersVrt", s2InputsTaskArgs));
            allSteps.append(CreateTaskStep(s2ExtractMasksTask, "BuildMasksVrt", s2MasksTaskArgs));

            const QString &outPath = s2TemporalResTask.GetFilePath(tile + "_" + band + "_S2_all.tif");
            const QStringList &s2TemporalResArgs = GetS2TemporalResTaskArgs(cfg, s2InputsPath, s2MskPath, listDates, outPath);
            allSteps.append(CreateTaskStep(s2TemporalResTask, "TemporalResampling", s2TemporalResArgs));

            temporalResamplingFiles.append(outPath);
            if (band == "NDVI") {
                ndviResampledRaster = outPath;
            }
        }

//        QStringList s1TemporalResamplingFiles;
//        for(auto band: s1Bands) {
//            TaskToSubmit &s1ExtractPrdsListTask = allTasksList[curTaskIdx++];
//            TaskToSubmit &s1BuildVrtTask = allTasksList[curTaskIdx++];
//            TaskToSubmit &s1GdalTranslateTask = allTasksList[curTaskIdx++];

//            const QString &s1PrdsListPath = s1ExtractPrdsListTask.GetFilePath(tile + "_" + band + "_S1_prds_list.txt");
//            const QStringList &s1ExtrListArgs = GetS1ProductsListArgs(workingPath, band, tile, s1PrdsListPath);
//            allSteps.append(CreateTaskStep(s1ExtractPrdsListTask, "ExtractS1List", s1ExtrListArgs));

//            const QString &s1VrtPath = s1BuildVrtTask.GetFilePath(tile + "_" + band + "_S1_prds.vrt");
//            const QStringList &s1VrtArgs = GetS1VrtTaskArgs(s1PrdsListPath, s1VrtPath);
//            allSteps.append(CreateTaskStep(s1BuildVrtTask, "CreateS1VRT", s1VrtArgs));

//            const QString &s1OutPath = s1GdalTranslateTask.GetFilePath(tile + "_" + band + "_S1_all.tif");
//            const QStringList &s1RasterBuildArgs = GetS1RasterBuildTaskArgs(s1VrtPath, s1OutPath);
//            allSteps.append(CreateTaskStep(s1GdalTranslateTask, "S1RasterCreation", s1RasterBuildArgs));

//            s1TemporalResamplingFiles.append(s1OutPath);
//        }

        for (int period = 1; period <= cfg.clusteringIntervals; period++) {
            TaskToSubmit &s2ClusterPrepTask = allTasksList[curTaskIdx++];
            TaskToSubmit &s2IsolatedPixelsTask = allTasksList[curTaskIdx++];
            TaskToSubmit &s2SpatialConnectivityTask = allTasksList[curTaskIdx++];
            TaskToSubmit &s2ClusterAnalysisTask = allTasksList[curTaskIdx++];

            TaskToSubmit &s1ClusterPrepTask = allTasksList[curTaskIdx++];
            TaskToSubmit &s1IsolatedPixelsTask = allTasksList[curTaskIdx++];
            TaskToSubmit &s1SpatialConnectivityTask = allTasksList[curTaskIdx++];
            TaskToSubmit &s1ClusterAnalysisTask = allTasksList[curTaskIdx++];

            // Cluster preparation step
            const QString &outClusterFile = s2ClusterPrepTask.GetFilePath(tile + "_cluster_prep_S2_" + QString::number(period) + ".tif");
            const QStringList &s2ClustPrepResArgs = GetS2ClusterPrepTaskArgs(cfg, temporalResamplingFiles, tile, period, outClusterFile);
            allSteps.append(CreateTaskStep(s2ClusterPrepTask, "S2ClusterPreparation", s2ClustPrepResArgs));

            // Remove isolated pixels
            const QString &s2IsoPixelsRemClustPath = s2IsolatedPixelsTask.GetFilePath(QStringLiteral("L4D_ClS2_") + QString::number(cfg.event.siteId) +
                                                                                      "_" + tile + "_" + QString::number(period) + ".tif");
            const QStringList &s2IsolatedPixArgs = GetIsolatedPixelsTaskArgs(cfg, outClusterFile, Satellite::Sentinel2, s2IsoPixelsRemClustPath);
            allSteps.append(CreateTaskStep(s2IsolatedPixelsTask, "S2IsolatedPixels", s2IsolatedPixArgs));
            prdFormatterFiles.append(s2IsoPixelsRemClustPath);

            // Spatial connectivity
            const QString &s2SpatialConPath = s2SpatialConnectivityTask.GetFilePath(QStringLiteral("L4D_ClS2_") + QString::number(cfg.event.siteId) +
                                                                                    "_" + tile + "_" + QString::number(period) + "_Connect.tif");
            const QStringList &s2SpatialConnArgs = GetSpatialConnectivityTaskArgs(cfg, s2IsoPixelsRemClustPath, Satellite::Sentinel2,  s2SpatialConPath);
            allSteps.append(CreateTaskStep(s2SpatialConnectivityTask, "S2SpatialConnectivity", s2SpatialConnArgs));
            prdFormatterFiles.append(s2SpatialConPath);

            // Cluster analysis step
            const QString &s2ClustAnalysisPath = s2ClusterAnalysisTask.GetFilePath(QStringLiteral("L4D_ClS2_") + QString::number(cfg.event.siteId) +
                                                                                   "_" + tile + "_" + QString::number(period) + ".csv");
            const QStringList &s2ClustAnalysisArgs = GetS2ClusterAnalysisTaskArgs(cfg, ndviResampledRaster, tile, period,
                                                                                  s2IsoPixelsRemClustPath, s2SpatialConPath, s2ClustAnalysisPath);
            allSteps.append(CreateTaskStep(s2ClusterAnalysisTask, "S2ClusterAnalysis", s2ClustAnalysisArgs));
            s2PeriodAnalysisTiles[period].append(s2ClustAnalysisPath);

            // S1
            // Cluster preparation step
            const QString &outS1ClusterFile = s1ClusterPrepTask.GetFilePath(tile + "_cluster_prep_S1_" + QString::number(period) + ".tif");
            const QStringList &s1ClustPrepResArgs = GetS1ClusterPrepTaskArgs(cfg, ctFilesPath, tile, period, outS1ClusterFile);
            allSteps.append(CreateTaskStep(s1ClusterPrepTask, "S1ClusterPreparation", s1ClustPrepResArgs));

            // Remove isolated pixels
            const QString &s1IsoPixelsRemClustPath = s1IsolatedPixelsTask.GetFilePath(QStringLiteral("L4D_ClS1_") + QString::number(cfg.event.siteId) +
                                                                                      "_" + tile + "_" + QString::number(period) + ".tif");
            const QStringList &s1IsolatedPixArgs = GetIsolatedPixelsTaskArgs(cfg, outS1ClusterFile, Satellite::Sentinel1, s1IsoPixelsRemClustPath);
            allSteps.append(CreateTaskStep(s1IsolatedPixelsTask, "S1IsolatedPixels", s1IsolatedPixArgs));
            prdFormatterFiles.append(s1IsoPixelsRemClustPath);

            // Spatial connectivity
            const QString &s1SpatialConPath = s1SpatialConnectivityTask.GetFilePath(QStringLiteral("L4D_ClS1_") + QString::number(cfg.event.siteId) +
                                                                                    "_" + tile + "_" + QString::number(period) + "_Connect.tif");
            const QStringList &s1SpatialConnArgs = GetSpatialConnectivityTaskArgs(cfg, s1IsoPixelsRemClustPath, Satellite::Sentinel1,  s1SpatialConPath);
            allSteps.append(CreateTaskStep(s1SpatialConnectivityTask, "S1SpatialConnectivity", s1SpatialConnArgs));
            prdFormatterFiles.append(s1SpatialConPath);

            // Cluster analysis step
            const QString &s1ClustAnalysisPath = s1ClusterAnalysisTask.GetFilePath(QStringLiteral("L4D_ClS1_") + QString::number(cfg.event.siteId) +
                                                                                   "_" + tile + "_" + QString::number(period) + ".csv");
            const QStringList &s1ClustAnalysisArgs = GetS1ClusterAnalysisTaskArgs(cfg, tile, period,
                                                                                  s1IsoPixelsRemClustPath, s1SpatialConPath, s1ClustAnalysisPath);
            allSteps.append(CreateTaskStep(s1ClusterAnalysisTask, "S1ClusterAnalysis", s1ClustAnalysisArgs));
            s1PeriodAnalysisTiles[period].append(s1ClustAnalysisPath);
            prdFormatterFiles.append(s1ClustAnalysisPath);
        }
    }

    // merge cluster analysis for all tiles
    QStringList periodAnalysisFiles;
    QList<int> periods;
    for (int period = 1; period <= cfg.clusteringIntervals; period++) {
        TaskToSubmit &clustTilesAnalysisMergeS2 = allTasksList[curTaskIdx++];
        const QString &s2ClustAnalysisPath = clustTilesAnalysisMergeS2.GetFilePath(QStringLiteral("L4D_HeteS2_") + QString::number(cfg.event.siteId) +
                                                                                   "_" + QString::number(cfg.year) + "_" + QString::number(period) + ".csv");
        const QStringList &clustTilesAnalysisMergeS2Args = GetTilesAnalysisMergeTaskArgs(s2PeriodAnalysisTiles[period], s2ClustAnalysisPath);
        allSteps.append(CreateTaskStep(clustTilesAnalysisMergeS2, "TilesAnalysisMerge", clustTilesAnalysisMergeS2Args));
        prdFormatterFiles.append(s2ClustAnalysisPath);

        TaskToSubmit &clustTilesAnalysisMergeS1 = allTasksList[curTaskIdx++];
        const QString &s1ClustAnalysisPath = clustTilesAnalysisMergeS1.GetFilePath(QStringLiteral("L4D_HeteS1_") + QString::number(cfg.event.siteId) +
                                                                                   "_" + QString::number(cfg.year) + "_" + QString::number(period) + ".csv");
        const QStringList &clustTilesAnalysisMergeS1Args = GetTilesAnalysisMergeTaskArgs(s1PeriodAnalysisTiles[period], s1ClustAnalysisPath);
        allSteps.append(CreateTaskStep(clustTilesAnalysisMergeS1, "TilesAnalysisMerge", clustTilesAnalysisMergeS1Args));
        prdFormatterFiles.append(s1ClustAnalysisPath);

        // add the period and S2 cluster analysis
        periods.append(period);
        periodAnalysisFiles.append(s2ClustAnalysisPath);
    }
    // final period S2 analysis
    TaskToSubmit &heterogPeriodAnalysisTask = allTasksList[curTaskIdx++];
    const QString &periodAnalysisPath = heterogPeriodAnalysisTask.GetFilePath(QStringLiteral("L4D_HeteDecisionS2_") + QString::number(cfg.event.siteId) +
                                                                               "_" + QString::number(cfg.year) + ".csv");
    const QStringList &periodAnalysisArgs = GetPeriodAnalysisTaskArgs(cfg, periods, periodAnalysisFiles, periodAnalysisPath);
    allSteps.append(CreateTaskStep(heterogPeriodAnalysisTask, "PeriodAnalysis", periodAnalysisArgs));
    prdFormatterFiles.append(periodAnalysisPath);

    TaskToSubmit &productFormatterTask = allTasksList[curTaskIdx++];

    const QStringList &productFormatterArgs = GetProductFormatterArgs(productFormatterTask, cfg, prdFormatterFiles);
    allSteps.append(CreateTaskStep(productFormatterTask, "ProductFormatter", productFormatterArgs));

    return allSteps;
}

void S4CHeterogeneityHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                                const JobSubmittedEvent &event)
{
    S4CHeterogneneityJobConfig cfg(&ctx, event);
    if (cfg.existingCTSARDir.size() > 0) {
        Logger::info("Heterogeneity: Using existing CT product " + cfg.existingCTSARDir);
    }

    QList<TaskToSubmit> allTasksList;
    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef = CreateTasks(cfg, allTasksList);
    SubmitTasks(ctx, cfg.event.jobId, allTasksListRef);
    NewStepList allSteps = CreateSteps(allTasksList, cfg);
    ctx.SubmitSteps(allSteps);
}

void S4CHeterogeneityHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
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
            int prdId = ctx.InsertProduct({ ProductType::S4CHeterogeneityProductTypeId, event.processorId,
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
#if QT_VERSION >= QT_VERSION_CHECK(5, 14, 0)
                stream << prdId << ";" << productFolder << Qt::endl;
#else
                stream << prdId << ";" << productFolder << endl;
#endif
            }
            ctx.MarkJobFinished(event.jobId);
            // Now remove the job folder containing temporary files

            // TODO: Reinsert this - made only for urgent issue
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

ProcessorJobDefinitionParams S4CHeterogeneityHandler::GetProcessingDefinitionImpl(
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
        ctx.GetConfigurationParameters(S4C_HETEROGENEITY_CFG_PREFIX, siteId, requestOverrideCfgValues);
    // we might have an offset in days from starting the downloading products to start the S4C L4A
    // production
    int startSeasonOffset = cfgValues[QStringLiteral(S4C_HETEROGENEITY_CFG_PREFIX) + "start_season_offset"].value.toInt();
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
        (cfgValues[QStringLiteral(S4C_HETEROGENEITY_CFG_PREFIX) + "sched_wait_proc_inputs"].value.toInt() != 0);
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

QStringList S4CHeterogeneityHandler::GetProductFormatterArgs(TaskToSubmit &productFormatterTask,
                                                     const S4CHeterogneneityJobConfig &cfg, const QStringList &listFiles) {
    QString strTimePeriod = cfg.startDate.toString("yyyyMMddTHHmmss").append("_").append(cfg.endDate.toString("yyyyMMddTHHmmss"));
    QStringList additionalArgs = {"-processor.generic.files"};
    additionalArgs += listFiles;
    return GetDefaultProductFormatterArgs(*(cfg.pCtx), productFormatterTask, cfg.event.jobId, cfg.event.siteId, "S4C_HETEROGENEITY", strTimePeriod,
                                         "generic", additionalArgs, true);
}

QStringList S4CHeterogeneityHandler::GetExtractParcelsTaskArgs(const S4CHeterogneneityJobConfig &cfg,
                                                    const QString &parcelsPath, const QString &lutPath,
                                                    const QString &tilesPath, const QString &opticalPath,
                                                    const QString &radarPath,  const QString &lpisPath)
{
    QStringList extractParcelsArgs = { "-s",
                                 QString::number(cfg.event.siteId),
                                 "--season-start",
                                 cfg.startDate.toString("yyyy-MM-dd"),
                                 "--season-end",
                                 cfg.endDate.toString("yyyy-MM-dd")};
    extractParcelsArgs.append("--");
    extractParcelsArgs.append(parcelsPath);
    extractParcelsArgs.append(lutPath);
    extractParcelsArgs.append(tilesPath);
    extractParcelsArgs.append(opticalPath);
    extractParcelsArgs.append(radarPath);
    extractParcelsArgs.append(lpisPath);

    return extractParcelsArgs;
}

QStringList S4CHeterogeneityHandler::GetCropTypeTaskArgs(const S4CHeterogneneityJobConfig &cfg,
                                                         const QString &workingPath, const QString &tilesPath,
                                                         const QString &radarPath, const QString &lpisPath)
{

    QStringList cropTypeArgs = { "-s", QString::number(cfg.event.siteId),
                                 "--working-path", workingPath,
                                 "--tile-footprints", tilesPath,
                                 "--radar-products", radarPath,
                                 "--lpis-path", lpisPath
                               };
    return cropTypeArgs;
}


QStringList S4CHeterogeneityHandler::GetGdalBuidVrtTaskArgs(const QStringList &files, const QString &vrtFile)
{
    QStringList args = {"-separate", vrtFile};
    args += files;

    return args;
}

QStringList S4CHeterogeneityHandler::GetS2TemporalResTaskArgs(const S4CHeterogneneityJobConfig &cfg, const QString &in,
                                                              const QString &inMsk, const QList<int> &inDates,
                                                              const QString &out)
{
    QStringList inStrDates;
    std::transform(std::begin(inDates),
                   std::end(inDates),
                   std::back_inserter(inStrDates),
                   [](int d) { return QString::number(d); }
                  );
    QStringList args = { "TemporalResampling",
                                 "-in", in,
                                 "-mask", inMsk,
                                 "-out", out + "?gdal:co:COMPRESS=DEFLATE",
                                 "-bv", QString::number(cfg.maskValue),
                                 "-nan", QString::number(cfg.nanValue),
                                 "-maxdist", QString::number(cfg.s2MaxDist),
                                 "-winradius", QString::number(cfg.winRadius)
                               };
    args += "-indates";
    args += inStrDates;
    args += "-outdates";
    args += cfg.outDates;

    return args;
}

QStringList S4CHeterogeneityHandler::GetS1ProductsListArgs(const QString &s1RastersDir, const QString &band,
                                                           const QString &tile, const QString &s1PrdsListPath)
{
    // TODO: This should be changed and updated at script level
    return { "--input-dir", QDir::cleanPath(s1RastersDir + QDir::separator() + "sar"),
             "--tile", tile,
             "--filter-str", band,
             "--out", s1PrdsListPath
    };
}

//QStringList S4CHeterogeneityHandler::GetS1VrtTaskArgs(const QString &s1PrdsListPath, const QString &s1VrtPath)
//{
//    return {"-separate", "-input_file_list", s1PrdsListPath, s1VrtPath};
//}

//QStringList S4CHeterogeneityHandler::GetS1RasterBuildTaskArgs(const QString &s1VrtPath, const QString &s1OutPath)
//{
//    return {s1VrtPath, s1OutPath + "?gdal:co:COMPRESS=DEFLATE"};
//}

QStringList S4CHeterogeneityHandler::GetS2ClusterPrepTaskArgs(const S4CHeterogneneityJobConfig &cfg, const QStringList &inFiles,
                                                              const QString &tile, int periodIdx, const QString &out)
{
    const QMap<QString, QString> &tiledRasters = cfg.lpisInfos.s2TiledRasters;
    if (!tiledRasters.contains(tile)) {
        cfg.pCtx->MarkJobFailed(cfg.event.jobId);
        throw std::runtime_error(QStringLiteral("Heterogeneity: Cannot find 5m buffered raster for tile %1 in LPIS %2")
                .arg(tile)
                .arg(cfg.lpisInfos.productPath).toStdString());
    }
    const QString &lpisRaster = tiledRasters[tile];
    QStringList args = {"--input-images"};
    args += inFiles;
    args+= {"--lpis-buffered-raster", lpisRaster};
    args+= {"--number-of-clusters", QString::number(cfg.GetNoOfClusters(Satellite::Sentinel2))};
    args+= {"--number-of-images", QString::number(cfg.GetNoOfImagesInPeriod(Satellite::Sentinel2))};
    args+= {"--period", QString::number(periodIdx)};
    args+= {"--output", out};

    return args;
}

QStringList S4CHeterogeneityHandler::GetS1ClusterPrepTaskArgs(const S4CHeterogneneityJobConfig &cfg, const QString &inS1CTDir,
                                                              const QString &tile, int periodIdx, const QString &out)
{
    // The S1 LPIS rasters are at 20m resolutions but during heterogeneity benchmarking the S1 AMP and COHE were created at 10m resolution
    const QMap<QString, QString> &tiledRasters = cfg.lpisInfos.s1TiledRasters;
    if (!tiledRasters.contains(tile)) {
        cfg.pCtx->MarkJobFailed(cfg.event.jobId);
        throw std::runtime_error(QStringLiteral("Heterogeneity: Cannot find 5m buffered raster for tile %1 in LPIS %2")
                .arg(tile)
                .arg(cfg.lpisInfos.productPath).toStdString());
    }
    const QString &lpisRaster = tiledRasters[tile];
    QStringList args = {"--input-dir", inS1CTDir};
    args+= {"--lpis-buffered-raster", lpisRaster};
    args+= {"--number-of-clusters", QString::number(cfg.GetNoOfClusters(Satellite::Sentinel1))};
    args+= {"--number-of-images", QString::number(cfg.GetNoOfImagesInPeriod(Satellite::Sentinel1))};
    args+= {"--period", QString::number(periodIdx)};
    args+= {"--year", QString::number(cfg.year)};
    args+= {"--tile", tile};
    args+= {"--output", out};

    return args;
}

QStringList S4CHeterogeneityHandler::GetIsolatedPixelsTaskArgs(const S4CHeterogneneityJobConfig &cfg, const QString &in, Satellite sat,
                                                              const QString &out)
{
    int nbClusters = cfg.GetNoOfClusters(sat);
    int noDataVal = 0;      // TODO - to be determined by satellite and product type (-10000 for L2A, 0 for NDVI and S1)

    QStringList args = {"HeterogeneityRemoveIsolated", "-in", in};
    args += {"-threshold", QString::number(cfg.isolatedPixelsThr)};
    args += {"-radius", QString::number(cfg.smoothingRadius)};
    args += {"-nodata", QString::number(noDataVal)};
    args += "-classes";

    // List of classes depend on the number of clusters – range from 1 to n_cl+1
    for (int cls = 1; cls <= nbClusters; cls++) {
        args += QString::number(cls);
    }

    args += {"-out", out + "?gdal:co:COMPRESS=DEFLATE"};

    return args;
}

QStringList S4CHeterogeneityHandler::GetSpatialConnectivityTaskArgs(const S4CHeterogneneityJobConfig &cfg, const QString &in,
                                                               Satellite sat, const QString &out)
{
    QStringList args = {"HeterogeneityLocalClassConnectivityIndex", "-in", in};
    args += {"-fullconnectivity", QString::number(cfg.fullConnectivity)};
    args += {"-radius", QString::number(sat == Satellite::Sentinel1 ? cfg.searchRadiusS1 : cfg.searchRadiusS2)};
    args += {"-out", out + "?gdal:co:COMPRESS=DEFLATE"};

    return args;
}

QStringList S4CHeterogeneityHandler::GetS2ClusterAnalysisTaskArgs(const S4CHeterogneneityJobConfig &cfg, const QString &inNdvi,
                                                                  const QString &tile, int periodIdx,
                                                                const QString &smoothedRaster, const QString &localConRaster,
                                                                const QString &out)
{
    const QString &lpisRaster = cfg.lpisInfos.s2TiledRasters[tile];
    const QString &lpisCsv = cfg.lpisInfos.csvPath;

    QStringList args = {"--ndvi-image", inNdvi};
    args+= {"--lpis-csv", lpisCsv};
    args+= {"--lpis-buffered-raster", lpisRaster};
    args+= {"--smooted-raster", smoothedRaster};
    args+= {"--local-conn-raster",localConRaster};
    args+= {"--number-of-images", QString::number(cfg.GetNoOfImagesInPeriod(Satellite::Sentinel2))};
    args+= {"--period", QString::number(periodIdx)};
    args+= {"--min-s2-pixels", QString::number(cfg.clustPixNumS2)};
    args+= {"--ndvi-thr-dist", QString::number(cfg.ndviThrDist)};
    args+= {"--s2-compactness-threshold", QString::number(cfg.compactnessThrS2)};
    args+= {"--percentage-heterogeneity", QString::number(cfg.percentHetero)};
    args+= {"--output", out};

    return args;
}

QStringList S4CHeterogeneityHandler::GetS1ClusterAnalysisTaskArgs(const S4CHeterogneneityJobConfig &cfg,
                                                                  const QString &tile, int periodIdx,
                                                                const QString &smoothedRaster, const QString &localConRaster,
                                                                const QString &out)
{
    // we do not use here the S1 rasters as they are at 20m resolution while all processing is at 10m resolution
    // and we do not want a resampling
    const QString &lpisRaster = cfg.lpisInfos.s2TiledRasters[tile];
    const QString &lpisCsv = cfg.lpisInfos.csvPath;

    QStringList args = {"--lpis-csv", lpisCsv};
    args+= {"--lpis-buffered-raster", lpisRaster};
    args+= {"--smooted-raster", smoothedRaster};
    args+= {"--local-conn-raster",localConRaster};
    args+= {"--number-of-images", QString::number(cfg.GetNoOfImagesInPeriod(Satellite::Sentinel1))};
    args+= {"--period", QString::number(periodIdx)};
    args+= {"--min-s1-pixels", QString::number(cfg.clustPixNumS1)};
    args+= {"--s1-compactness-threshold", QString::number(cfg.compactnessThrS1)};
    args+= {"--percentage-heterogeneity", QString::number(cfg.percentHetero)};
    args+= {"--output", out};

    return args;
}

QStringList S4CHeterogeneityHandler::GetTilesAnalysisMergeTaskArgs(const QStringList &inputFiles, const QString &outFile)
{
    QStringList args = {"--output", outFile,
                        "--input-files"};
    for (const QString &inFile: inputFiles) {
        args += inFile;
    }

    return args;
}

QStringList S4CHeterogeneityHandler::GetPeriodAnalysisTaskArgs(const S4CHeterogneneityJobConfig &cfg, const QList<int> periods,
                                                               const QStringList &periodAnalysisFiles, const QString &outFile)
{
    const QString &lpisCsv = cfg.lpisInfos.csvPath;

    QStringList args = {"--lpis-csv", lpisCsv};
    args.append("--periods");
    for (int period: periods) {
        args += QString::number(period);
    }

    args.append("--periods-files");
    for (const QString &periodFile: periodAnalysisFiles) {
        args += periodFile;
    }

    args += {"--output", outFile};

    return args;
}

void S4CHeterogeneityHandler::S4CHeterogneneityJobConfig::UpdateLpisInfos() {
    const ProductList &lpisPrds = S4CUtils::GetLpisProduct(pCtx, event.siteId);
    if (lpisPrds.size() == 0) {
        pCtx->MarkJobFailed(event.jobId);
        throw std::runtime_error(QStringLiteral("No LPIS product found in database for the Heterogeneity execution for site %1.").
                                 arg(siteShortName).toStdString());
    }
    LpisInfosExtractor extractor;
    extractor.SetLpisProducts(lpisPrds);
    const QMap<int, LpisInfos> &allLpisInfos = extractor.GetLpisInfos();
    QMap<int, LpisInfos>::const_iterator i = allLpisInfos.find(year);
    if (i == allLpisInfos.end()) {
        pCtx->MarkJobFailed(event.jobId);
        throw std::runtime_error(QStringLiteral("No LPIS product found in database for the Heterogeneity execution for site %1 and year %2")
                                 .arg(siteShortName)
                                 .arg(year).toStdString());
    }
    this->lpisInfos = i.value();
}

void S4CHeterogeneityHandler::S4CHeterogneneityJobConfig::UpdateOpticalPrdsTileBandMaps()
{
    const TileList &siteTiles = pCtx->GetSiteTiles(event.siteId, (int)Satellite::Sentinel2);
    std::vector<QString> siteTileIds;
    std::transform(siteTiles.begin(), siteTiles.end(),
        std::back_inserter(siteTileIds), [](Tile const& t) {
            return t.tileId;
        }
    );
    for (const ProductDetails &prd: l2aProductDetails) {
        std::unique_ptr<ProductHelper> prdHelper = ProductHelperFactory::GetProductHelper(prd);
        const QStringList &tileIds = prdHelper->GetTileIdsFromProduct();
        if (tileIds.size() != 1) {
            Logger::error(QStringLiteral("Heterogeneity: The L2A product with name %1 has incorrect number of tiles (%2). Ignoring it ...")
                    .arg(prd.GetProduct().name)
                    .arg(tileIds.size()));
            continue;
        }
        const QString &tile = tileIds[0];
        if (std::find_if(siteTiles.begin(), siteTiles.end(),
                         [&](const Tile& t){return t.tileId == tile;}) == siteTiles.end()) {
            Logger::error(QStringLiteral("Heterogeneity: The L2A product with name %1 has tile (%2) which is not present in site tiles. Ignoring it ...")
                    .arg(prd.GetProduct().name)
                    .arg(tileIds.size()));
            continue;
        }


        TileInfoMaps &tileInfoMaps = tileInfos[tile];
        for (QString s2Band: s2Bands) {
            if (s2Band == "NDVI") {
                continue;
            }
            const QStringList &rasterFiles = prdHelper->GetProductFiles(s2Band);
            const QStringList &maskFiles = prdHelper->GetProductMasks("10M");
            if (rasterFiles.size() != 1 || maskFiles.size() != 1) {
                Logger::error(QStringLiteral("Heterogeneity: The L2A product with name %1 has incorrect number of rasters (%2) or masks (%3) for band %4. Ignoring it ...")
                        .arg(prd.GetProduct().name)
                        .arg(rasterFiles.size())
                        .arg(maskFiles.size())
                        .arg(s2Band));
                continue;
            }

            tileInfoMaps.mapRasters[s2Band].append(rasterFiles[0]);
            tileInfoMaps.mapMasks[s2Band].append(maskFiles[0]);
            tileInfoMaps.mapDates[s2Band].append(prd.GetProduct().created.date().dayOfYear());
        }
    }

    // extract also the masks for the NDVI
    QMap<QString, QString>::const_iterator pos1, pos2;
    for (const ProductDetails &prd: l3bProductDetails) {
        std::unique_ptr<ProductHelper> prdHelper = ProductHelperFactory::GetProductHelper(prd);
        const QMap<QString, QString> &tileFiles = prdHelper->GetProductFilesByTile("SNDVI");
        const QMap<QString, QString> &maskFiles = prdHelper->GetProductFilesByTile("MMONODFLG", true);
        Logger::info(QStringLiteral("Heterogeneity: NDVI tiles %1").arg(tileFiles.keys().join(",")));
        for (const QString &tile: tileFiles.keys()) {

            if (std::find_if(siteTiles.begin(), siteTiles.end(),
                             [&](const Tile& t){return t.tileId == tile;}) == siteTiles.end()) {
                Logger::error(QStringLiteral("Heterogeneity: The L3B product with name %1 has NDVI for tile %2 which is not present in site tiles.  Ignoring it ...")
                        .arg(prd.GetProduct().name)
                        .arg(tile));
                continue;
            }

            TileInfoMaps &tileInfoMaps = tileInfos[tile];
            pos1 = tileFiles.find(tile);
            pos2 = maskFiles.find(tile);
            if (pos1 == tileFiles.end() || pos2 == maskFiles.end()) {
                Logger::error(QStringLiteral("Heterogeneity: The L3B product with name %1 does not have raster or mask for tile %2. Ignoring it ...")
                        .arg(prd.GetProduct().name)
                        .arg(tile));
                continue;
            }

            tileInfoMaps.mapRasters["NDVI"].append(pos1.value());
            tileInfoMaps.mapMasks["NDVI"].append(pos2.value());
            tileInfoMaps.mapDates["NDVI"].append(prd.GetProduct().created.date().dayOfYear());
        }
    }

    // compute the out dates
    int seasonStartDoy = startDate.date().dayOfYear();
    int seasonEndDoy = endDate.date().dayOfYear();
    for (int doy = seasonStartDoy; doy <= seasonEndDoy; doy+=s2TemporalResamplingInterval) {
        outDates.append(QString::number(doy));
    }
    if (outDates[outDates.size()-1] != seasonEndDoy) {
        outDates.append(QString::number(seasonEndDoy));
    }
}

int S4CHeterogeneityHandler::S4CHeterogneneityJobConfig::GetNoOfImagesInPeriod(Satellite sat) const
{
    switch (sat) {
    case Satellite::Sentinel2:
        return s2NumImages;
    case Satellite::Sentinel1:
        return s1NumImages;
    default:
        pCtx->MarkJobFailed(event.jobId);
        throw std::runtime_error(QStringLiteral("Unsupported satellite %1")
                                 .arg((int)sat).toStdString());
    }
}

int S4CHeterogeneityHandler::S4CHeterogneneityJobConfig::GetNoOfClusters(Satellite sat) const
{
    switch (sat) {
    case Satellite::Sentinel2:
        return s2ClustersNo;
    case Satellite::Sentinel1:
        return s1ClustersNo;
    default:
        pCtx->MarkJobFailed(event.jobId);
        throw std::runtime_error(QStringLiteral("Unsupported satellite %1")
                                 .arg((int)sat).toStdString());
    }
}


void S4CHeterogeneityHandler::S4CHeterogneneityJobConfig::FilterOpticalProductDetails()
{
    // filter the reprocessed L2A products
    QList<ProductDetails> ret;
    bool ignore = false;
    Logger::info(QStringLiteral("Heterogeneity: Filtering reprocessed L2A input products. Having initially %1 products").arg(l2aProductDetails.size()));
    // We assume the product details are sorted by creation date
    for(int i = 0; i < l2aProductDetails.size(); i++) {
        const ProductDetails &prdDetails = l2aProductDetails[i];
        const Product &prdOld = l2aProductDetails[i].GetProductRef();
        ignore = false;
        // check if the current product. If its date does not exist or if the exists but its name is higher than
        // any of the products from the same date, orbit and tile,
        for (int j = i+1; j < l2aProductDetails.size(); j++) {
            const Product &prdCur = l2aProductDetails[j].GetProductRef();
            if (prdOld.created == prdCur.created && prdOld.tiles == prdCur.tiles && prdOld.orbitId == prdCur.orbitId &&
                    prdOld.name < prdCur.name) {
                ignore = true;
                break;
            }
        }
        if (!ignore) {
            ret.append(prdDetails);
        }
    }
    Logger::info(QStringLiteral("Heterogeneity: Having %1 products after filtering reprocessed L2A input products").arg(ret.size()));
    l2aProductDetails = ret;
}
