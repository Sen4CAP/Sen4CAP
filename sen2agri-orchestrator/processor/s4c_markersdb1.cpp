#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "s4c_markersdb1.hpp"
#include "processorhandlerhelper.h"
#include "json_conversions.hpp"
#include "logger.hpp"
#include "s4c_utils.hpp"

S4CMarkersDB1Handler::S4CMarkersDB1Handler()
{
}

void S4CMarkersDB1Handler::CreateTasks(QList<TaskToSubmit> &outAllTasksList, const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder)
{
    int curTaskIdx = 0;
    const QList<MarkerType> &enabledMarkers = dataExtrStepsBuilder.GetEnabledMarkers();
    for (const auto &marker: enabledMarkers) {
        // Create data extraction tasks if needed
        int minDataExtrIndex = curTaskIdx;
        dataExtrStepsBuilder.CreateTasks(marker, outAllTasksList, curTaskIdx);
        int maxDataExtrIndex = curTaskIdx-1;

        // tasks for merge and export
        outAllTasksList.append(TaskToSubmit{ "mdb1-csv-merge", {} });
        int mergeTaskIdx = curTaskIdx++;
        // if at least one task was added for the data extraction for this marker, then add it in the parrent list
        if (minDataExtrIndex <= maxDataExtrIndex) {
            for (int i = minDataExtrIndex; i <= maxDataExtrIndex; i++) {
                outAllTasksList[mergeTaskIdx].parentTasks.append(outAllTasksList[i]);
            }
        }
        outAllTasksList.append(TaskToSubmit{ "mdb-csv-to-ipc-export", {outAllTasksList[mergeTaskIdx]} });
        curTaskIdx++;
        // Markers DB 2 are computed only for AMP
        if (marker.marker == "AMP") {
            // Add also the tasks to create the markers database 2.
            // These can be performed in parallel with the export to IPC of MDB1
            outAllTasksList.append(TaskToSubmit{ "mdb2-csv-extract", {outAllTasksList[mergeTaskIdx]} });
            int exportMdb2TaskIdx = curTaskIdx++;
            outAllTasksList.append(TaskToSubmit{ "mdb-csv-to-ipc-export", {outAllTasksList[exportMdb2TaskIdx]} });
            curTaskIdx++;
        }
    }
}

void S4CMarkersDB1Handler::CreateSteps(QList<TaskToSubmit> &allTasksList, const MDB1JobPayload &jobCfg,
                                        const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder, NewStepList &steps)
{
    int curTaskIdx = 0;
    const QList<MarkerType> &enabledMarkers = dataExtrStepsBuilder.GetEnabledMarkers();

    // if only data extraction is needed, then we create the filter ids step into the general configured directory
    for (const auto &marker: enabledMarkers) {
        QStringList dataExtrDirs;
        // Create the data extraction steps if needed
        dataExtrStepsBuilder.CreateSteps(marker, allTasksList, steps, curTaskIdx, dataExtrDirs);

        // If scheduled jobs, force adding the data extraction directories for all markers as data extraction source
        if (jobCfg.isScheduledJob) {
            // add a data extraction dir corresponding to the scheduled date which is saved as jobCfg.maxPrdDate
            for (const auto &marker: enabledMarkers) {
                const QString &dataExtrDirName = dataExtrStepsBuilder.GetDataExtractionDir(marker.marker);
                if (!dataExtrDirs.contains(dataExtrDirName)) {
                    QDir().mkpath(dataExtrDirName);
                    dataExtrDirs.append(dataExtrDirName);
                }
            }
        }
        // Steps to merge and create the MDB1 IPC
        const QString &mergedFile = CreateStepsForFilesMerge(marker, dataExtrDirs, steps,
                                                                 allTasksList, curTaskIdx);
        CreateStepsForExportIpc(jobCfg, marker, mergedFile, steps, allTasksList, curTaskIdx, "MDB1");

        if (marker.marker == "AMP") {
            // Steps to merge and create the MDB2 IPC
            const QString &exportedFile = CreateStepsForMdb2Export(marker, mergedFile, steps,
                                                                   allTasksList, curTaskIdx);
            CreateStepsForExportIpc(jobCfg, marker, exportedFile, steps, allTasksList, curTaskIdx, "MDB2");
        }
    }
}

void S4CMarkersDB1Handler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                              const JobSubmittedEvent &evt)
{
    S4CMarkersDB1DataExtractStepsBuilder dataExtrStepsBuilder;
    dataExtrStepsBuilder.Initialize(ctx, evt);

    MDB1JobPayload jobCfg(&ctx, evt);
    // initialize the payload min and max date
    dataExtrStepsBuilder.GetDataExtractionInterval(jobCfg.minDate, jobCfg.maxDate);

    QList<TaskToSubmit> allTasksList;
    CreateTasks(allTasksList, dataExtrStepsBuilder);

    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    QList<std::reference_wrapper<const TaskToSubmit>> allTasksListRef2;
    for(TaskToSubmit &task: allTasksList) {
        allTasksListRef.append(task);
        allTasksListRef2.append(task);
    }
    SubmitTasks(ctx, evt.jobId, allTasksListRef);
    NewStepList allSteps;
    CreateSteps(allTasksList, jobCfg, dataExtrStepsBuilder, allSteps);
    ctx.SubmitSteps(allSteps);

    // create the end of all steps marker
    TaskToSubmit endOfJobDummyTask{"end-of-job", {}};
    endOfJobDummyTask.parentTasks.append(allTasksListRef2);
    SubmitTasks(ctx, evt.jobId, {endOfJobDummyTask});
    jobCfg.pCtx->SubmitSteps({endOfJobDummyTask.CreateStep("EndOfJob", QStringList())});
}

void S4CMarkersDB1Handler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                              const TaskFinishedEvent &event)
{
    if (event.module == "mdb-csv-to-ipc-export") {

        const QString &productPath = GetProductFormatterOutputProductPath(ctx, event);
        const QString &prodName = GetProductFormatterProductName(ctx, event);
        QFileInfo fileInfo(prodName);
        const QString &prdNameNoExt = fileInfo.baseName ();
        if(prdNameNoExt != "") {
            const QString &footPrint = GetProductFormatterFootprint(ctx, event);
            // Insert the product into the database
            QDateTime minDate, maxDate;
            ProcessorHandlerHelper::GetHigLevelProductAcqDatesFromName(prdNameNoExt, minDate, maxDate);
            ProductType prdType = ProductType::S4MDB1ProductTypeId;
            if(prdNameNoExt.contains("MDB2")) {
                prdType = ProductType::S4MDB2ProductTypeId;
            }
            ctx.InsertProduct({ prdType, event.processorId, event.siteId,
                                event.jobId, productPath, maxDate,
                                prdNameNoExt, "", footPrint, std::experimental::nullopt, TileIdList() });
        } else {
            Logger::error(QStringLiteral("Cannot insert into database the product with name %1 and path %2").arg(prdNameNoExt).arg(productPath));
        }
    } else if (event.module == "end-of-job") {
        ctx.MarkJobFinished(event.jobId);
        // Now remove the job folder containing temporary files
        RemoveJobFolder(ctx, event.jobId, processorDescr.shortName);
    }
}

ProcessorJobDefinitionParams S4CMarkersDB1Handler::GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues)
{
    ProcessorJobDefinitionParams params;
    params.isValid = false;

    QDateTime seasonStartDate;
    QDateTime seasonEndDate;
    // extract the scheduled date
    QDateTime qScheduledDate = QDateTime::fromTime_t(scheduledDate);
    GetSeasonStartEndDates(ctx, siteId, seasonStartDate, seasonEndDate, qScheduledDate, requestOverrideCfgValues);
    QDateTime limitDate = seasonEndDate.addMonths(2);
    if(qScheduledDate > limitDate) {
        return params;
    }

    ConfigurationParameterValueMap mapCfg = ctx.GetConfigurationParameters(QString(MDB1_CFG_PREFIX),
                                                                           siteId, requestOverrideCfgValues);
    std::map<QString, QString> configParams;
    for (const auto &p : mapCfg) {
        configParams.emplace(p.key, p.value);
    }
    // we might have an offset in days from starting the downloading products to start the S4C_L4C production
    // TODO: Is this really needed
    int startSeasonOffset = mapCfg["processor.s4c_l4c.start_season_offset"].value.toInt();
    QDateTime startDate = seasonStartDate.addDays(startSeasonOffset);
    params.jsonParameters.append("{ \"scheduled_job\": \"1\", \"start_date\": \"" + startDate.toString("yyyyMMdd") + "\", " +
                                 "\"end_date\": \"" + qScheduledDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_start_date\": \"" + seasonStartDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_end_date\": \"" + seasonEndDate.toString("yyyyMMdd") + "\"");
    params.jsonParameters.append(", \"execution_operation\": \"all\"}");

    params.isValid = true;

    return params;
}

void S4CMarkersDB1Handler::HandleProductAvailableImpl(EventProcessingContext &ctx,
                                const ProductAvailableEvent &event)
{
    // Get the product description from the database
    const Product &prd = ctx.GetProduct(event.productId);
    QJsonObject parameters;
    auto configParameters = ctx.GetConfigurationParameters(MDB1_CFG_PREFIX, prd.siteId);

    // Check that the product type is L3B, AMP or COHE
    const QString &prdTypeShortName = GetShortNameForProductType(prd.productTypeId);
    if (prdTypeShortName.size() == 0) {
        return;
    }

//    QString errMsg;
//    const QString &siteShortName = ctx.GetSiteShortName(prd.siteId);
//    const QString &yearStr = MarkersDB1JobCfg::GetYear(parameters, configParameters, siteShortName);
//    if (!CheckExecutionPreconditions(&ctx, parameters, configParameters, prd.siteId, siteShortName,
//                                     yearStr, errMsg)) {
//        Logger::info(QStringLiteral("MarkersDB1 - HandleProductAvailable - Cannot trigger data extraction job "
//                     "for product %1 and siteid = %2. The error was: %3").arg(prd.fullPath).arg(QString::number((int)prd.siteId))
//                     .arg(errMsg));
//        return;
//    }

    // check if the NRT data extraction is configured for the site
    bool nrtDataExtrEnabled = ProcessorHandlerHelper::GetBoolConfigValue(parameters, configParameters, "nrt_data_extr_enabled", MDB1_CFG_PREFIX);
    if (nrtDataExtrEnabled) {
        // Create a new JOB
        NewJob newJob;
        newJob.processorId = processorDescr.processorId;  //send the job to this processor
        newJob.siteId = prd.siteId;
        newJob.startType = JobStartType::Triggered;

        QJsonObject processorParamsObj;
        QJsonArray prodsJsonArray;
        prodsJsonArray.append(prd.fullPath);

        const QString &prdKey = "input_" + prdTypeShortName;
        processorParamsObj[prdKey] = prodsJsonArray;
        processorParamsObj["execution_operation"] = "DataExtraction";
        newJob.parametersJson = jsonToString(processorParamsObj);
        ctx.SubmitJob(newJob);
        Logger::info(QStringLiteral("MarkersDB1 - HandleProductAvailable - Submitted data extraction trigger job "
                                    "for product %1 and siteid = %2").arg(prd.fullPath).arg(QString::number((int)prd.siteId)));
    }
}

ProductList S4CMarkersDB1Handler::GetLpisProduct(ExecutionContextBase *pCtx, int siteId) {
    // We take it the last LPIS product for this site.
    QDate  startDate, endDate;
    startDate.setDate(1970, 1, 1);
    QDateTime startDateTime(startDate);
    endDate.setDate(2050, 12, 31);
    QDateTime endDateTime(endDate);
    return pCtx->GetProducts(siteId, (int)ProductType::S4CLPISProductTypeId, startDateTime, endDateTime);
}

QString S4CMarkersDB1Handler::GetShortNameForProductType(const ProductType &prdType) {
    switch(prdType) {
        case ProductType::L3BProductTypeId:         return "L3B";
        case ProductType::S4CS1L2AmpProductTypeId:  return "AMP";
        case ProductType::S4CS1L2CoheProductTypeId: return "COHE";
        default:                                    return "";
    }
}

bool S4CMarkersDB1Handler::CheckExecutionPreconditions(ExecutionContextBase *pCtx, const std::map<QString, QString> &configParameters,
                                                        int siteId, const QString &siteShortName, QString &errMsg) {
    errMsg = "";
    // We take it the last LPIS product for this site.
    const ProductList &lpisPrds = GetLpisProduct(pCtx, siteId);
    if (lpisPrds.size() == 0) {
        errMsg = QStringLiteral("ERROR Markers DB 1: No LPIS product found for site %1.").
                                 arg(siteShortName);
        return false;
    }

    return true;
}

QString S4CMarkersDB1Handler::CreateStepsForFilesMerge(const MarkerType &markerType,
                              const QStringList &dataExtrDirs, NewStepList &steps,
                              QList<TaskToSubmit> &allTasksList, int &curTaskIdx) {
    TaskToSubmit &mergeTask = allTasksList[curTaskIdx++];
    const QString &mergedFile = mergeTask.GetFilePath(BuildMergeResultFileName(markerType));
    const QStringList &mergeArgs = GetFilesMergeArgs(dataExtrDirs, mergedFile);
    steps.append(mergeTask.CreateStep("Markers1CsvMerge", mergeArgs));

    return mergedFile;
}

QString S4CMarkersDB1Handler::CreateStepsForMdb2Export(const MarkerType &markerType, const QString &mergedFile,
                                NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx) {
    TaskToSubmit &mdb2ExtractTask = allTasksList[curTaskIdx++];
    const QString &mdb2ExtractFile = mdb2ExtractTask.GetFilePath(BuildMdb2FileName(markerType));
    const QStringList &mdb2ExtractArgs =  { "Markers2Extractor", "-in", mergedFile, "-out", mdb2ExtractFile };
    steps.append(mdb2ExtractTask.CreateStep("Markers2Extractor", mdb2ExtractArgs));

    return mdb2ExtractFile;
}

QString S4CMarkersDB1Handler::CreateStepsForExportIpc(const MDB1JobPayload &jobCfg, const MarkerType &marker, const QString &inputFile,
                              NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx, const QString &prdType) {

    TaskToSubmit &exportTask = allTasksList[curTaskIdx++];

    const auto &targetFolder = GetFinalProductFolder(*jobCfg.pCtx, jobCfg.event.jobId, jobCfg.event.siteId);
    const QString &strTimePeriod = QString("%1_%2").arg(jobCfg.minDate.toString("yyyyMMdd"),
                                                        jobCfg.maxDate.toString("yyyyMMdd"));
    const QString &creationDateStr = QDateTime::currentDateTime().toString("yyyyMMddTHHmmss");
    const QString &prdName = QString("%1_%2_S%3_V%4_%5_%6").arg("SEN4CAP", prdType, QString::number(jobCfg.event.siteId), strTimePeriod,
                                                                  creationDateStr, marker.marker);
    const QString &exportedFile = QString("%1/%2/%3.ipc").arg(targetFolder, prdName, prdName);
    const auto &outPropsPath = exportTask.GetFilePath(PRODUCT_FORMATTER_OUT_PROPS_FILE);
    std::ofstream executionInfosFile;
    try {
        executionInfosFile.open(outPropsPath.toStdString().c_str(), std::ofstream::out);
        executionInfosFile << exportedFile.toStdString() << std::endl;
        executionInfosFile.close();
    } catch (...) {
    }
    QStringList exportArgs = { "--in", inputFile, "--out", exportedFile };
    steps.append(exportTask.CreateStep("MarkersDB1Export", exportArgs));

    return exportedFile;
}

QString S4CMarkersDB1Handler::BuildMergeResultFileName(const MarkerType &markerType)
{
    return QString(markerType.marker).append("_Extracted_Data.csv");
}

QString S4CMarkersDB1Handler::BuildMdb2FileName(const MarkerType &markerType)
{
    return QString(markerType.marker).append("_Extracted_Data_VVVH.csv");
}

QStringList S4CMarkersDB1Handler::GetFilesMergeArgs(const QStringList &listInputPaths, const QString &outFileName)
{
    QStringList retArgs = { "Markers1CsvMerge", "-out", outFileName, "-il" };
    retArgs += listInputPaths;
    // TODO: This could be interesting to be added when working on a directory
//    if (prdMaxDate.isValid()) {
//        retArgs += "-maxdate";
//        retArgs += prdMaxDate.toString("yyyy-MM-dd");
//    }
    return retArgs;
}
