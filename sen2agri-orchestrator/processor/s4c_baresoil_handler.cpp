#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "logger.hpp"
#include "processorhandlerhelper.h"
#include "s4c_baresoil_handler.hpp"
#include "stepexecutiondecorator.h"

#include "products/generichighlevelproducthelper.h"
using namespace orchestrator::products;

QList<std::reference_wrapper<TaskToSubmit>>
S4CBareSoilHandler::CreateTasks(const S4CBareSoilJobConfig &, QList<TaskToSubmit> &outAllTasksList)
{
    int curTaskIdx = 0;
    outAllTasksList.append(TaskToSubmit{ "s4c-bare-soil-s2-calibration", {} });
    int s2CalibIdx = curTaskIdx++;
    outAllTasksList.append(TaskToSubmit{ "s4c-bare-soil-s1-calibration", {outAllTasksList[s2CalibIdx]}  });
    int s1CalibIdx = curTaskIdx++;
    outAllTasksList.append(TaskToSubmit{ "s4c-bare-soil-s2-model", {outAllTasksList[s2CalibIdx]}  });
    int s2ModelIdx = curTaskIdx++;
    outAllTasksList.append(TaskToSubmit{ "s4c-bare-soil-s1-model", {outAllTasksList[s1CalibIdx]}  });
    int s1ModelIdx = curTaskIdx++;

    outAllTasksList.append(TaskToSubmit{ "s4c-bare-soil-markers", {outAllTasksList[s2ModelIdx], outAllTasksList[s1ModelIdx]}  });
    int markersIdx = curTaskIdx++;

    outAllTasksList.append(TaskToSubmit{ "product-formatter", {outAllTasksList[markersIdx]} });

    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    for (TaskToSubmit &task : outAllTasksList) {
        allTasksListRef.append(task);
    }
    return allTasksListRef;
}

NewStepList S4CBareSoilHandler::CreateSteps(QList<TaskToSubmit> &allTasksList,const S4CBareSoilJobConfig &cfg)
{
    int curTaskIdx = 0;
    NewStepList allSteps;
    QStringList prdFormatterFiles;

    TaskToSubmit &s2CalibTask = allTasksList[curTaskIdx++];
    TaskToSubmit &s1CalibTask = allTasksList[curTaskIdx++];
    TaskToSubmit &s2ModelTask = allTasksList[curTaskIdx++];
    TaskToSubmit &s1ModelTask = allTasksList[curTaskIdx++];
    TaskToSubmit &markersTask = allTasksList[curTaskIdx++];
    TaskToSubmit &productFormatterTask = allTasksList[curTaskIdx++];

    // Resulting files from tasks
    const QString &s2CalibPath = s2CalibTask.GetFilePath("L4E_BS_CalibrationS2.csv");
    const QString &s1CalibPath = s1CalibTask.GetFilePath("L4E_BS_CalibrationS1.csv");

    const QString &s2ResultsPath = s2ModelTask.GetFilePath("L4E_BS_S2results.csv");
    const QString &s2ModelPath = s2ModelTask.GetFilePath("L4E_BSmodelS2.sav");
    const QString &s2FigImportancePath = s2ModelTask.GetFilePath("BSmodelS2featuresimportance.png");

    const QString &s1ResultsPath = s1ModelTask.GetFilePath("L4E_BS_S1results.csv");
    const QString &s1ModelPath = s1ModelTask.GetFilePath("L4E_BSmodelS1.sav");
    const QString &s1FigImportancePath = s1ModelTask.GetFilePath("BSmodelS1featuresimportance.png");

    const QString &s2MarkersPath = markersTask.GetFilePath("L4E_BS_MarkersS2.csv");
    const QString &s1MarkersPath = markersTask.GetFilePath("L4E_BS_MarkersS1.csv");
    const QString &allMarkersPath = markersTask.GetFilePath("L4E_BS_MarkersAll.csv");

    // Inputs extraction and reflectances stack tif creation
    const QStringList &s2CalibrationArgs = GetS2CalibrationTaskArgs(cfg, s2CalibPath);
    allSteps.append(CreateTaskStep(s2CalibTask, "S2Calibration", s2CalibrationArgs ));

    const QStringList &s1CalibrationArgs = GetS1CalibrationTaskArgs(cfg, s2CalibPath, s1CalibPath);
    allSteps.append(CreateTaskStep(s1CalibTask, "S1Calibration", s1CalibrationArgs));

    const QStringList &s2ModelArgs = GetS2ModelTaskArgs(cfg, s2CalibPath, s2ResultsPath, s2ModelPath, s2FigImportancePath);
    allSteps.append(CreateTaskStep(s2ModelTask, "S2Model", s2ModelArgs));

    const QStringList &s1ModelArgs = GetS1ModelTaskArgs(cfg, s1CalibPath, s1ResultsPath, s1ModelPath, s1FigImportancePath);
    allSteps.append(CreateTaskStep(s1ModelTask, "S1Model", s1ModelArgs));

    const QStringList &markersArgs = GetMarkersTaskArgs(cfg, s2ResultsPath, s1ResultsPath, s2MarkersPath, s1MarkersPath, allMarkersPath);
    allSteps.append(CreateTaskStep(markersTask, "MarkersExtraction", markersArgs));

    prdFormatterFiles.append(s2CalibPath);
    prdFormatterFiles.append(s1CalibPath);
    prdFormatterFiles.append(s2ResultsPath);
    prdFormatterFiles.append(s2ModelPath);
    prdFormatterFiles.append(s2FigImportancePath);
    prdFormatterFiles.append(s1ResultsPath);
    prdFormatterFiles.append(s1ModelPath);
    prdFormatterFiles.append(s1FigImportancePath);

    prdFormatterFiles.append(s2MarkersPath);
    prdFormatterFiles.append(s1MarkersPath);
    prdFormatterFiles.append(allMarkersPath);

    const QStringList &productFormatterArgs = GetProductFormatterArgs(productFormatterTask, cfg, prdFormatterFiles);
    allSteps.append(CreateTaskStep(productFormatterTask, "ProductFormatter", productFormatterArgs));

    return allSteps;
}

QStringList S4CBareSoilHandler::GetS2CalibrationTaskArgs(const S4CBareSoilJobConfig &cfg, const QString &s2CalibPath)
{
    QStringList args = {
                "--input", cfg.mdb1PrdPath,
                "--output", s2CalibPath,
                "--lpis", cfg.lpisPath,
                "--thr-bs-ndvi", QString::number(cfg.calibBSNdviThr),
                "--thr-nbs-ndvi", QString::number(cfg.calibNBSNdviThr),
                "--thr-bs-ndwi", QString::number(cfg.calibBSNdwiThr),
                "--thr-nbs-ndwi", QString::number(cfg.calibNBSNdwiThr),
                "--thr-bs-ndti", QString::number(cfg.calibBSNdtiThr),
                "--thr-nbs-ndti", QString::number(cfg.calibNBSNdtiThr),
                "--thr-nbs-fcover", QString::number(cfg.calibNBSFcoverThr),
                "--s2-pix-thr", QString::number(cfg.calibS2PixThr),
                "--tiles"
    };
    args += cfg.siteTiles;

    return args;
}

QStringList S4CBareSoilHandler::GetS1CalibrationTaskArgs(const S4CBareSoilJobConfig &cfg, const QString &s2CalibPath, const QString &s1CalibPath)
{
    return {    "--input", cfg.mdbL4SarMainPrdPath,
                "--output", s1CalibPath,
                "--s2-bs-calib", s2CalibPath
    };
}

QStringList S4CBareSoilHandler::GetS2ModelTaskArgs(const S4CBareSoilJobConfig &cfg, const QString &s2CalibPath,
                                                   const QString &s2Results, const QString &outputModel,
                                                   const QString &figImportancePath)
{
    QStringList args =  {
                "--input", cfg.mdb1PrdPath,
                "--output", s2Results,
                "--s2-bs-calib", s2CalibPath,
                "--estimators-number", QString::number(cfg.modelEstimatorsNo),
                "--output-model", outputModel,
                "--output-fig-importance", figImportancePath,
                "--tiles"
    };
    args += cfg.siteTiles;

    return args;
}

QStringList S4CBareSoilHandler::GetS1ModelTaskArgs(const S4CBareSoilJobConfig &cfg, const QString &s1CalibPath,
                                                   const QString &s1Results, const QString &outputModel,
                                                   const QString &figImportancePath)
{
    return {    "--input", cfg.mdbL4SarMainPrdPath,
                "--output", s1Results,
                "--s1-bs-calib", s1CalibPath,
                "--estimators-number", QString::number(cfg.modelEstimatorsNo),
                "--output-model", outputModel,
                "--output-fig-importance", figImportancePath
    };
}

QStringList S4CBareSoilHandler::GetMarkersTaskArgs(const S4CBareSoilJobConfig &cfg, const QString &s2Results,
                                                   const QString &s1Results, const QString &outputS2Markers,
                                                   const QString &outputS1Markers, const QString &outputAllMarkers)
{
    return {    "--input-s2", s2Results,
                "--input-s1", s1Results,
                "--year", QString::number(cfg.year),
                "--out-markers-s2", outputS2Markers,
                "--out-markers-s1", outputS1Markers,
                "--out-markers-all", outputAllMarkers,
                "--start-date", cfg.startDate.toString("yyyy-MM-dd"),
                "--end-date", cfg.endDate.addDays(1).toString("yyyy-MM-dd"),
                "--p-long", QString::number(cfg.markersLongPeriod),
                "--p-short", QString::number(cfg.markersShortPeriod),
                "--s2-periods", QString::number(cfg.markersS2PeriodsNo),
                "--s1-periods", QString::number(cfg.markersS1PeriodsNo),
                "--thr-bs-s2", QString::number(cfg.markersS2BSThreshold),
                "--thr-nbs-s2", QString::number(cfg.markersS2NBSThreshold),
                "--thr-bs-s1",  QString::number(cfg.markersS1BSThreshold),
                "--thr-nbs-s1",  QString::number(cfg.markersS1NBSThreshold)
    };
}



void S4CBareSoilHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                                const JobSubmittedEvent &event)
{
    S4CBareSoilJobConfig cfg(&ctx, event);
    QList<TaskToSubmit> allTasksList;
    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef = CreateTasks(cfg, allTasksList);
    SubmitTasks(ctx, cfg.event.jobId, allTasksListRef);
    NewStepList allSteps = CreateSteps(allTasksList, cfg);
    ctx.SubmitSteps(allSteps);
}

void S4CBareSoilHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
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

ProcessorJobDefinitionParams S4CBareSoilHandler::GetProcessingDefinitionImpl(
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
        Logger::debug(QStringLiteral("Scheduler Bare Soil: Error getting season start dates for "
                                     "site %1 for scheduled date %2!")
                          .arg(siteId)
                          .arg(qScheduledDate.toString()));
        return params;
    }

    QDateTime limitDate = seasonEndDate.addMonths(2);
    if (qScheduledDate > limitDate) {
        Logger::debug(QStringLiteral("Scheduler Bare Soil: Error scheduled date %1 greater than the "
                                     "limit date %2 for site %3!")
                          .arg(qScheduledDate.toString())
                          .arg(limitDate.toString())
                          .arg(siteId));
        return params;
    }

    ConfigurationParameterValueMap cfgValues =
        ctx.GetConfigurationParameters(S4C_BARE_SOIL_CFG_PREFIX, siteId, requestOverrideCfgValues);
    // we might have an offset in days from starting the downloading products to start the S4C L4A
    // production
    int startSeasonOffset = cfgValues[QStringLiteral(S4C_BARE_SOIL_CFG_PREFIX) + "start_season_offset"].value.toInt();
    seasonStartDate = seasonStartDate.addDays(startSeasonOffset);

    QDateTime startDate = seasonStartDate;
    QDateTime endDate = qScheduledDate;
    // do not pass anymore the product list but the dates
    params.jsonParameters.append("{ \"scheduled_job\": \"1\", \"start_date\": \"" + startDate.toString("yyyyMMdd") + "\", " +
                                 "\"end_date\": \"" + endDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_start_date\": \"" + seasonStartDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_end_date\": \"" + seasonEndDate.toString("yyyyMMdd") + "\"}");

    // Normally, we need at least 1 product available, the crop mask and the shapefile in order to
    // be able to create a S4C Bare soil product but if we do not return here, the schedule block waiting
    // for products (that might never happen)
    bool waitForAvailProcInputs =
        (cfgValues[QStringLiteral(S4C_BARE_SOIL_CFG_PREFIX) + "sched_wait_proc_inputs"].value.toInt() != 0);
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
        Logger::debug(QStringLiteral("Scheduled job for S4C Bare Soil and site ID %1 with start date %2 "
                                     "and end date %3 will not be executed "
                                     "(productsNo = %4)!")
                          .arg(siteId)
                          .arg(startDate.toString())
                          .arg(endDate.toString())
                          .arg(params.productList.size()));
    }

    return params;
}

QStringList S4CBareSoilHandler::GetProductFormatterArgs(TaskToSubmit &productFormatterTask,
                                                     const S4CBareSoilJobConfig &cfg, const QStringList &listFiles) {
    QString strTimePeriod = cfg.startDate.toString("yyyyMMddTHHmmss").append("_").append(cfg.endDate.toString("yyyyMMddTHHmmss"));
    QStringList additionalArgs = {"-processor.generic.files"};
    additionalArgs += listFiles;
    return GetDefaultProductFormatterArgs(*(cfg.pCtx), productFormatterTask, cfg.event.jobId, cfg.event.siteId, "S4C_BARESOIL", strTimePeriod,
                                         "generic", additionalArgs, true);
}

