#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "logger.hpp"
#include "processorhandlerhelper.h"
#include "s4c_change_detection_handler.hpp"
#include "s4c_baresoil_steps_builder.hpp"
#include "stepexecutiondecorator.h"

#include "products/generichighlevelproducthelper.h"
using namespace orchestrator::products;

QList<std::reference_wrapper<TaskToSubmit>>
S4CChangeDetectionHandler::CreateTasks(const S4CChangeDetectionJobConfig &, QList<TaskToSubmit> &outAllTasksList)
{
    int curTaskIdx = 0;
    int changeDetCompIdxs[2];

    outAllTasksList.append(TaskToSubmit{ "s4c-change-detection-extract-common-parcels", {} });
    int extrCommonParcelsTaskIdx = curTaskIdx++;

    // create tasks for both reference site and for current site
    for (int i = 0; i<2; i++) {
        outAllTasksList.append(TaskToSubmit{ "s4c-change-detection-filter-lpis-cols", {{outAllTasksList[extrCommonParcelsTaskIdx]}} });
        int lpisFilterTaskIdx = curTaskIdx++;
        outAllTasksList.append(TaskToSubmit{ "s4c-change-detection-lai-outliers", {outAllTasksList[lpisFilterTaskIdx]}  });
        int laiOutliersTaskIdx = curTaskIdx++;
        outAllTasksList.append(TaskToSubmit{ "s4c-change-detection-veg-growth-markers", {outAllTasksList[lpisFilterTaskIdx]}  });
        int vegGrowthTaskIdx = curTaskIdx++;
        outAllTasksList.append(TaskToSubmit{ "s4c-change-detection-bs-markers", {outAllTasksList[lpisFilterTaskIdx]}  });
        int bsMarkersTaskIdx = curTaskIdx++;
        outAllTasksList.append(TaskToSubmit{ "s4c-change-detection-computation", {
                                                 outAllTasksList[laiOutliersTaskIdx],
                                                                                  outAllTasksList[vegGrowthTaskIdx],
                                                                                  outAllTasksList[bsMarkersTaskIdx]}  });
        changeDetCompIdxs[i] = curTaskIdx++;
    }
    outAllTasksList.append(TaskToSubmit{ "s4c-change-detection-consolidation", {
                                             outAllTasksList[changeDetCompIdxs[0]],
                                             outAllTasksList[changeDetCompIdxs[1]]}  });
    int cdConsolidationIdx = curTaskIdx++;


    outAllTasksList.append(TaskToSubmit{ "product-formatter", {outAllTasksList[cdConsolidationIdx]} });

    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    for (TaskToSubmit &task : outAllTasksList) {
        allTasksListRef.append(task);
    }
    return allTasksListRef;
}

NewStepList S4CChangeDetectionHandler::CreateSteps(QList<TaskToSubmit> &allTasksList,const S4CChangeDetectionJobConfig &cfg,
                                                   NewStepList &allSteps)
{
    int curTaskIdx = 0;
    QStringList prdFormatterFiles;
    QString changeDetFiles[2];
    QString refLpisFilteredPath;

    TaskToSubmit &extrCommonParcelsTask = allTasksList[curTaskIdx++];
    QString mappingFile = extrCommonParcelsTask.GetFilePath("sites_newids_mapping.csv");
    const QStringList &extractCommonParcelsArgs = GeCommonParcelsExtractionTaskArgs(cfg, mappingFile);
    allSteps.append(CreateTaskStep(extrCommonParcelsTask, "ExtractNewIDsMapping", extractCommonParcelsArgs));
    // if a custom mapping file is provided, then use this one instead
    if (cfg.sitesIdsMappingFile.length() > 0) {
        mappingFile = cfg.sitesIdsMappingFile;
    }

    for (int i = 0; i<2; i++) {
        bool isRefSite = (i == 0);
        TaskToSubmit &filterLpisColsTask = allTasksList[curTaskIdx++];
        TaskToSubmit &laiOutliersTask = allTasksList[curTaskIdx++];
        TaskToSubmit &vegGrowthMarkersTask = allTasksList[curTaskIdx++];
        TaskToSubmit &bsMarkersTask = allTasksList[curTaskIdx++];
        TaskToSubmit &changeDetComputeTask = allTasksList[curTaskIdx++];

        // Resulting files from tasks
        const QString &lpisFilteredPath = filterLpisColsTask.GetFilePath("lpis_filtered_P" + QString::number(i+1) + ".csv");
        if (i == 0) {
            refLpisFilteredPath = lpisFilteredPath;
        }
        const QString &laiOutliersPath = laiOutliersTask.GetFilePath("lai_outliers_P" + QString::number(i+1) + ".csv");

        const QString &vegGrowthPath = vegGrowthMarkersTask.GetFilePath("veg_growth_markes_P" + QString::number(i+1) + ".csv");
        const QString &bsMarkersPath = bsMarkersTask.GetFilePath("bs_markers_P" + QString::number(i+1) + ".csv");
        const QString &changeDetComputePath = changeDetComputeTask.GetFilePath("change_detection_P" + QString::number(i+1) + ".csv");

        // Inputs extraction and reflectances stack tif creation
        const QStringList &lpisFilteringArgs = GetLpisFilteringTaskArgs(cfg, isRefSite, mappingFile, lpisFilteredPath);
        allSteps.append(CreateTaskStep(filterLpisColsTask, "LpisFiltering", lpisFilteringArgs ));

        const QStringList &laiOutliersArgs = GetLaiOutliersTaskArgs(cfg, isRefSite, lpisFilteredPath, laiOutliersPath);
        allSteps.append(CreateTaskStep(laiOutliersTask, "LAIOutliers", laiOutliersArgs));

        const QStringList &vegGrowthMarkersArgs = GetVegGrowthMarkersTaskArgs(cfg, isRefSite, lpisFilteredPath, vegGrowthPath);
        allSteps.append(CreateTaskStep(vegGrowthMarkersTask, "VegetationGrowthMarkers", vegGrowthMarkersArgs));

        const QStringList &bsMarkersArgs = GetBSFilteredMarkersTaskArgs(cfg, isRefSite, lpisFilteredPath, bsMarkersPath);
        allSteps.append(CreateTaskStep(bsMarkersTask, "BareSoilMarkersFilter", bsMarkersArgs));

        const QStringList &changeDetComputeArgs = GetChangeDetectionComputationTaskArgs(cfg, lpisFilteredPath, isRefSite,
                                                                                        refLpisFilteredPath, laiOutliersPath,
                                                                                        vegGrowthPath, bsMarkersPath,
                                                                                        mappingFile, changeDetComputePath);
        allSteps.append(CreateTaskStep(changeDetComputeTask, "ChangeDetectionComputation", changeDetComputeArgs));
        changeDetFiles[i] = changeDetComputePath;
        prdFormatterFiles.append(changeDetComputePath);
    }
    TaskToSubmit &changeDetConsolidationTask = allTasksList[curTaskIdx++];
    const QString &changeDetFinalPath = changeDetConsolidationTask.GetFilePath("change_detection_P1_P2.csv");
    const QStringList &changeDetComputeArgs = GetChangeDetectionConsolidationTaskArgs(cfg, changeDetFiles[0], changeDetFiles[1],
                                                                                      mappingFile, changeDetFinalPath);
    allSteps.append(CreateTaskStep(changeDetConsolidationTask, "ChangeDetectionConsolidation", changeDetComputeArgs));
    prdFormatterFiles.append(changeDetFinalPath);

    TaskToSubmit &productFormatterTask = allTasksList[curTaskIdx++];
    const QStringList &productFormatterArgs = GetProductFormatterArgs(productFormatterTask, cfg, prdFormatterFiles);
    allSteps.append(CreateTaskStep(productFormatterTask, "ProductFormatter", productFormatterArgs));

    return allSteps;
}

QStringList S4CChangeDetectionHandler::GeCommonParcelsExtractionTaskArgs(const S4CChangeDetectionJobConfig &cfg,
                                                                         const QString &outPath)
{
    QStringList args = {
                "--site-id-p1", QString::number(cfg.refSiteCfg.siteId),
                "--site-id-p2", QString::number(cfg.currentSiteCfg.siteId),
                "--year-p1", QString::number(cfg.refSiteCfg.year),
                "--year-p2", QString::number(cfg.currentSiteCfg.year),
                "--output", outPath
    };

    return args;
}

QStringList S4CChangeDetectionHandler::GetLpisFilteringTaskArgs(const S4CChangeDetectionJobConfig &cfg, bool isRefSite,
                                                                const QString &filteringIdsFile, const QString &outPath)
{
    const QString &inFile = isRefSite ? cfg.refSiteCfg.lpisCsvPath : cfg.currentSiteCfg.lpisCsvPath;
    const QString &filteringIdsColName = isRefSite ? "NewID_ref" : "NewID";
    QStringList args = {
                "--input", inFile,
                "--columns-to-keep", "NewID", "LC", "PGrass", "CTnum", "CTnumL4A",
                "--filtering-ids-file", filteringIdsFile,
                "--filtering-ids-col-name", filteringIdsColName,
                "--output", outPath
    };

    return args;
}

QStringList S4CChangeDetectionHandler::GetLaiOutliersTaskArgs(const S4CChangeDetectionJobConfig &cfg, bool isRefSite,
                                                              const QString &lpisCsv, const QString &outFile)
{
    const SiteConfig &siteConfig = (isRefSite ? cfg.refSiteCfg : cfg.currentSiteCfg);
    QStringList args = {    "--input", siteConfig.mdb1PrdPath,
                "--output", outFile,
                "--lpis-csv", lpisCsv
    };

    if (siteConfig.mdb1IdsMappingFile.length() > 0) {
        args += {"--mapping-file", siteConfig.mdb1IdsMappingFile};
        args += {"--decl-newid", "NewID", "--markers-newid", "NewID_marker"};
    }

    return args;
}

QStringList S4CChangeDetectionHandler::GetVegGrowthMarkersTaskArgs(const S4CChangeDetectionJobConfig &cfg, bool isRefSite,
                                                   const QString &lpisCsv, const QString &out)
{
    const QString &inFile = isRefSite ? cfg.refSiteCfg.mdbL4OptMainPrdPath : cfg.currentSiteCfg.mdbL4OptMainPrdPath;
    QStringList args =  {
                "--input-mdb4", inFile,
                "--output", out,
                "--lpis-csv", lpisCsv
    };

    return args;
}

QStringList S4CChangeDetectionHandler::GetBSFilteredMarkersTaskArgs(const S4CChangeDetectionJobConfig &cfg, bool isRefSite,
                                                                    const QString &lpisCsv, const QString &out)
{
    const SiteConfig &siteConfig = (isRefSite ? cfg.refSiteCfg : cfg.currentSiteCfg);
    QStringList args =  {    "--input-bs-markers", siteConfig.bareSoilPrdPath,
                "--lpis-csv", lpisCsv,
                "--output", out
    };
    if (siteConfig.bsIdsMappingFile.length() > 0) {
        args += {"--mapping-file", siteConfig.bsIdsMappingFile};
        args += {"--decl-newid", "NewID", "--markers-newid", "NewID_marker"};
    }
    return args;
}

QStringList S4CChangeDetectionHandler::GetChangeDetectionComputationTaskArgs(const S4CChangeDetectionJobConfig &cfg, const QString &lpisCsv,
                                                   bool isRefSite, const QString &refLpisFilteredPath, const QString &laiOutliers,
                                                   const QString &vegGrowthMarkers, const QString &bsMarkers,
                                                   const QString &idsMappingFile, const QString &out)
{
    const SiteConfig &siteConfig = (isRefSite ? cfg.refSiteCfg : cfg.currentSiteCfg);
    int changePeriod = (isRefSite ? 1 : 2);
    QStringList args =  {
                "--input-lpis", lpisCsv,
                "--input-outliers", laiOutliers,
                "--input-veg-growth-markers", vegGrowthMarkers,
                "--input-bs-markers", bsMarkers,
                "--change-period", QString::number(changePeriod),
                "--grassland-ttdayss2-thr", QString::number(siteConfig.grassland_ttdayss2_thr),
                "--grassland-ttdayss2-incr", QString::number(siteConfig.grassland_ttdayss2_incr),
                "--grassland-ratiostab-min-thr", QString::number(siteConfig.grassland_ratiostab_min_thr),
                "--grassland-ratiostab-max-thr", QString::number(siteConfig.grassland_ratiostab_max_thr),
                "--grassland-ratiostab-min-incr", QString::number(siteConfig.grassland_ratiostab_min_incr),
                "--grassland-ratiostab-max-incr", QString::number(siteConfig.grassland_ratiostab_max_incr),
                "--grassland-consecstab-thr", QString::number(siteConfig.grassland_consecstab_thr),
                "--grassland-consecstab-incr", QString::number(siteConfig.grassland_consecstab_incr),
                "--permcrops-ttdayss2-thr", QString::number(siteConfig.permcrops_ttdayss2_thr),
                "--permcrops-ttdayss2-incr", QString::number(siteConfig.permcrops_ttdayss2_incr),
                "--permcrops-areaveg-thr", QString::number(siteConfig.permcrops_areaveg_thr),
                "--permcrops-areaveg-incr", QString::number(siteConfig.permcrops_areaveg_incr),
                "--permcrops-ratiostab-thr", QString::number(siteConfig.permcrops_ratiostab_thr),
                "--permcrops-ratiostab-incr", QString::number(siteConfig.permcrops_ratiostab_incr),
                "--arableland-ttdayss2-thr", QString::number(siteConfig.arableland_ttdayss2_thr),
                "--arableland-ttdayss2-incr", QString::number(siteConfig.arableland_ttdayss2_incr),
                "--output", out
    };
    if (changePeriod == 2) {
        args += {"--ref-lpis", refLpisFilteredPath,
                 "--mapping-file", idsMappingFile,
                 "--ref-decl-newid", cfg.refIdsMappingColName,
        };
    }

    return args;
}

QStringList S4CChangeDetectionHandler::GetChangeDetectionConsolidationTaskArgs(const S4CChangeDetectionJobConfig &cfg,
                                                   const QString &changeDetRef, const QString &changeDetCurrent,
                                                   const QString &idsMappingFile, const QString &out)
{
    return {    "--input-p1", changeDetRef,
                "--input-p2", changeDetCurrent,
                "--output", out,
                "--mapping-file", idsMappingFile,
                "--ref-decl-newid", cfg.refIdsMappingColName,
    };
}

void S4CChangeDetectionHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                                const JobSubmittedEvent &event)
{
    S4CChangeDetectionJobConfig jobCfg(&ctx, event);

    QList<TaskToSubmit> allTasksList;
    CreateTasks(jobCfg, allTasksList);

    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    for(TaskToSubmit &task: allTasksList) {
        allTasksListRef.append(task);
    }
    SubmitTasks(ctx, event.jobId, allTasksListRef);
    NewStepList allSteps;
    CreateSteps(allTasksList, jobCfg, allSteps);
    ctx.SubmitSteps(allSteps);
}

void S4CChangeDetectionHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
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
            int prdId = ctx.InsertProduct({ ProductType::S4CBareSoilProductTypeId, event.processorId,
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
            // TODO: check why it still remove the folder even if the key is set to 1
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

ProcessorJobDefinitionParams S4CChangeDetectionHandler::GetProcessingDefinitionImpl(
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
        Logger::debug(QStringLiteral("Scheduler Change Detection: Error getting season start dates for "
                                     "site %1 for scheduled date %2!")
                          .arg(siteId)
                          .arg(qScheduledDate.toString()));
        return params;
    }

    QDateTime limitDate = seasonEndDate.addMonths(2);
    if (qScheduledDate > limitDate) {
        Logger::debug(QStringLiteral("Scheduler Change Detection: Error scheduled date %1 greater than the "
                                     "limit date %2 for site %3!")
                          .arg(qScheduledDate.toString())
                          .arg(limitDate.toString())
                          .arg(siteId));
        return params;
    }

    ConfigurationParameterValueMap cfgValues =
        ctx.GetConfigurationParameters(S4C_CHANGE_DETECTION_CFG_PREFIX, siteId, requestOverrideCfgValues);
    // we might have an offset in days from starting the downloading products to start the S4C L4A
    // production
    int startSeasonOffset = cfgValues[QStringLiteral(S4C_CHANGE_DETECTION_CFG_PREFIX) + "start_season_offset"].value.toInt();
    seasonStartDate = seasonStartDate.addDays(startSeasonOffset);

    QDateTime startDate = seasonStartDate;
    QDateTime endDate = qScheduledDate;
    // do not pass anymore the product list but the dates
    params.jsonParameters.append("{ \"scheduled_job\": \"1\", \"start_date\": \"" + startDate.toString("yyyyMMdd") + "\", " +
                                 "\"end_date\": \"" + endDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_start_date\": \"" + seasonStartDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_end_date\": \"" + seasonEndDate.toString("yyyyMMdd") + "\"}");

    // Normally, we need at least 1 product available, the crop mask and the shapefile in order to
    // be able to create a S4C Change Detection product but if we do not return here, the schedule block waiting
    // for products (that might never happen)
    bool waitForAvailProcInputs =
        (cfgValues[QStringLiteral(S4C_CHANGE_DETECTION_CFG_PREFIX) + "sched_wait_proc_inputs"].value.toInt() != 0);
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
        Logger::debug(QStringLiteral("Scheduled job for S4C Change Detection and site ID %1 with start date %2 "
                                     "and end date %3 will not be executed "
                                     "(productsNo = %4)!")
                          .arg(siteId)
                          .arg(startDate.toString())
                          .arg(endDate.toString())
                          .arg(params.productList.size()));
    }

    return params;
}

QStringList S4CChangeDetectionHandler::GetProductFormatterArgs(TaskToSubmit &productFormatterTask,
                                                     const S4CChangeDetectionJobConfig &cfg, const QStringList &listFiles) {
    QString strTimePeriod = cfg.currentSiteCfg.startDate.toString("yyyyMMddTHHmmss").append("_").append(cfg.currentSiteCfg.endDate.toString("yyyyMMddTHHmmss"));
    QStringList additionalArgs = {"-processor.generic.files"};
    additionalArgs += listFiles;
    return GetDefaultProductFormatterArgs(*(cfg.pCtx), productFormatterTask, cfg.event.jobId, cfg.event.siteId, "S4C_CHANGEDECT", strTimePeriod,
                                         "generic", additionalArgs, true);
}

