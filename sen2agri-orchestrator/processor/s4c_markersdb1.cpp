#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "s4c_markersdb1.hpp"
#include "processorhandlerhelper.h"
#include "json_conversions.hpp"
#include "logger.hpp"
#include "s4c_utils.hpp"

#include "products/generichighlevelproducthelper.h"
using namespace orchestrator::products;

S4CMarkersDB1Handler::S4CMarkersDB1Handler()
{
}

void S4CMarkersDB1Handler::CreateTasks(QList<TaskToSubmit> &outAllTasksList, const MDB1JobPayload &jobCfg, const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder)
{
    int curTaskIdx = 0;
    const QList<MarkerType> &enabledMarkers = dataExtrStepsBuilder.GetEnabledMarkers();
    QList<int> dataExtrTaskIdxs;
    for (const auto &marker: enabledMarkers) {
        // Create data extraction tasks if needed
        int minDataExtrIndex = curTaskIdx;
        dataExtrStepsBuilder.CreateTasks(marker, outAllTasksList, curTaskIdx);
        int maxDataExtrIndex = curTaskIdx-1;

        // if at least one task was added for the data extraction for this marker, then add it in the parrent list for merge task
        if (minDataExtrIndex <= maxDataExtrIndex) {
            for (int i = minDataExtrIndex; i <= maxDataExtrIndex; i++) {
                dataExtrTaskIdxs.append(i);
            }
        }
    }
    // tasks for merge and export
    outAllTasksList.append(TaskToSubmit{ "mdb1-csv-merge", {} });
    int mergeTaskIdx = curTaskIdx++;
    for (int i: dataExtrTaskIdxs) {
        outAllTasksList[mergeTaskIdx].parentTasks.append(outAllTasksList[i]);
    }
    outAllTasksList.append(TaskToSubmit{ "mdb-csv-to-ipc-export", {outAllTasksList[mergeTaskIdx]} });
    curTaskIdx++;

    if (jobCfg.ampvvvhEnabled) {
        // Markers DB 2 are computed only for AMP
        // Add also the tasks to create the markers database 2.
        // These can be performed in parallel with the export to IPC of MDB1
        outAllTasksList.append(TaskToSubmit{ "mdb2-csv-extract", {outAllTasksList[mergeTaskIdx]} });
        int exportTaskParent = curTaskIdx++;
        outAllTasksList.append(TaskToSubmit{ "mdb-csv-to-ipc-export", {outAllTasksList[exportTaskParent]} });
        curTaskIdx++;
    }

    if (jobCfg.mdb3M1M5Enabled) {
        // Markers DB 3 are normally computed by the L4C processor but can be also extracted here
        // without other infos about harvest or practices
        outAllTasksList.append(TaskToSubmit{ "mdb3-input-tables-extract", {outAllTasksList[mergeTaskIdx]}});
        int inputTablesExtrIdx = curTaskIdx++;
        outAllTasksList.append(TaskToSubmit{ "mdb3-tsa", {outAllTasksList[inputTablesExtrIdx]}});
        int mdb3TsaIdx = curTaskIdx++;
        outAllTasksList.append(TaskToSubmit{ "mdb3-extract-markers", {outAllTasksList[mdb3TsaIdx]} });
        curTaskIdx++;
    }
}

void S4CMarkersDB1Handler::CreateSteps(QList<TaskToSubmit> &allTasksList, const MDB1JobPayload &jobCfg,
                                        const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder, NewStepList &steps)
{
    int curTaskIdx = 0;
    const QList<MarkerType> &enabledMarkers = dataExtrStepsBuilder.GetEnabledMarkers();

    // if only data extraction is needed, then we create the filter ids step into the general configured directory
    QStringList allDataExtrDirs;
    for (const auto &marker: enabledMarkers) {
        QStringList dataExtrDirs;
        // Create the data extraction steps if needed
        dataExtrStepsBuilder.CreateSteps(marker, allTasksList, steps, curTaskIdx, dataExtrDirs);
        allDataExtrDirs.append(dataExtrDirs);

        // If scheduled jobs, force adding the data extraction directories for all markers as data extraction source
        if (jobCfg.isScheduledJob) {
            // add a data extraction dir corresponding to the scheduled date which is saved as jobCfg.maxPrdDate
            const QString &dataExtrDirName = dataExtrStepsBuilder.GetDataExtractionDir(marker.marker);
            if (!allDataExtrDirs.contains(dataExtrDirName)) {
                QDir().mkpath(dataExtrDirName);
                allDataExtrDirs.append(dataExtrDirName);
            }
        }
    }
    // Steps to merge and create the MDB1 IPC
    const QString &mergedFile = CreateStepsForFilesMerge(allDataExtrDirs, steps,
                                                             allTasksList, curTaskIdx);
    CreateStepsForExportIpc(jobCfg, mergedFile, steps, allTasksList, curTaskIdx, "MDB1");

    if (jobCfg.ampvvvhEnabled) {
        // Steps to merge and create the MDB2 IPC
        const QString &exportedFile = CreateStepsForAmpVVVHExtraction(mergedFile, steps,
                                                               allTasksList, curTaskIdx);
        CreateStepsForExportIpc(jobCfg, exportedFile, steps, allTasksList, curTaskIdx, "MDB2");
    }

    if (jobCfg.mdb3M1M5Enabled) {
        const QDateTime &maxDate = dataExtrStepsBuilder.GetDataExtractionMaxDate();
        const Season &season = GetSeason(*(jobCfg.pCtx), jobCfg.event.siteId, maxDate);
        CreateMdb3Steps(jobCfg, season, mergedFile, steps, allTasksList, curTaskIdx);
    }

}

void S4CMarkersDB1Handler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                              const JobSubmittedEvent &evt)
{
    S4CMarkersDB1DataExtractStepsBuilder dataExtrStepsBuilder;
    const auto &parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
    dataExtrStepsBuilder.Initialize(processorDescr.shortName, ctx, parameters, evt.siteId, evt.jobId);

    MDB1JobPayload jobCfg(&ctx, evt, dataExtrStepsBuilder.GetDataExtractionMinDate(),
                          dataExtrStepsBuilder.GetDataExtractionMaxDate());

    QList<TaskToSubmit> allTasksList;
    CreateTasks(allTasksList, jobCfg, dataExtrStepsBuilder);

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
    jobCfg.pCtx->SubmitSteps({CreateTaskStep(endOfJobDummyTask, "EndOfJob", QStringList())});
}

void S4CMarkersDB1Handler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                              const TaskFinishedEvent &event)
{
    if (event.module == "mdb-csv-to-ipc-export" || event.module == "mdb3-extract-markers") {

        const QString &productPath = GetOutputProductPath(ctx, event);
        const QString &prodName = GetOutputProductName(ctx, event);
        QFileInfo fileInfo(prodName);
        const QString &prdNameNoExt = fileInfo.baseName ();
        if(prdNameNoExt != "") {
            const QString &footPrint = GetProductFormatterFootprint(ctx, event);
            // Insert the product into the database
            GenericHighLevelProductHelper prdHelper(productPath);
            ProductType prdType = ProductType::S4MDB1ProductTypeId;
            if(prdNameNoExt.contains("MDB2")) {
                prdType = ProductType::S4MDB2ProductTypeId;
            } else if(prdNameNoExt.contains("MDB3")) {
                prdType = ProductType::S4MDB3ProductTypeId;
            }
            ctx.InsertProduct({ prdType, event.processorId, event.siteId,
                                event.jobId, productPath, prdHelper.GetAcqDate(),
                                prdNameNoExt, "", footPrint, std::experimental::nullopt, TileIdList(), ProductIdsList() });
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
    if ((qScheduledDate > seasonStartDate.addDays(20)) &&
        (!CheckAllAncestorProductCreation(ctx, siteId, ProductType::L3BProductTypeId, seasonStartDate, qScheduledDate) ||
        !CheckAllAncestorProductCreation(ctx, siteId, ProductType::S4CS1L2AmpProductTypeId, seasonStartDate, qScheduledDate) ||
        !CheckAllAncestorProductCreation(ctx, siteId, ProductType::S4CS1L2CoheProductTypeId, seasonStartDate, qScheduledDate))) {
        params.schedulingFlags = SchedulingFlags::SCH_FLG_RETRY_LATER;
        Logger::error("MDB1 Scheduled job execution will be retried later: Not all input products were yet produced");
    } else {
        params.jsonParameters.append("{ \"scheduled_job\": \"1\", \"start_date\": \"" + startDate.toString("yyyyMMdd") + "\", " +
                                     "\"end_date\": \"" + qScheduledDate.toString("yyyyMMdd") + "\", " +
                                     "\"season_start_date\": \"" + seasonStartDate.toString("yyyyMMdd") + "\", " +
                                     "\"season_end_date\": \"" + seasonEndDate.toString("yyyyMMdd") + "\"");
        params.jsonParameters.append(", \"execution_operation\": \"all\"}");
    }
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

    // check if the NRT data extraction is configured for the site
    bool nrtDataExtrEnabled = ProcessorHandlerHelper::GetBoolConfigValue(parameters, configParameters, "nrt_data_extr_enabled", MDB1_CFG_PREFIX);
    if (!nrtDataExtrEnabled) {
        return;
    }

    // Check that the product type has a marker enabled
    if (!S4CMarkersDB1DataExtractStepsBuilder::HasAnyMarkerEnabled(prd.productTypeId, configParameters)) {
        return;
    }
    const QString &prdTypeShortName = ProductHelper::GetProductTypeShortName(prd.productTypeId);
    if (prdTypeShortName.size() == 0) {
        return;
    }
    const ProductList &lpisPrds = S4CUtils::GetLpisProduct(&ctx, prd.siteId);
    if (lpisPrds.size() == 0) {
        Logger::info(QStringLiteral("MarkersDB1 - HandleProductAvailable - No LPIS found in the database "
                                    "for product %1 and siteid = %2")
                     .arg(prd.fullPath).arg(QString::number(prd.siteId)));
        return;
    }

    // check also that the product has an LPIS imported
    bool hasLpis = false;
    int prdCreatedYear = prd.created.date().year();
    for(const Product &lpisPrd: lpisPrds) {
        // ignore LPIS products from a year where we already added an LPIS product newer
        if (prdCreatedYear == lpisPrd.created.date().year()) {
            hasLpis = true;
            break;
        }
    }
    if (!hasLpis) {
        Logger::info(QStringLiteral("MarkersDB1 - HandleProductAvailable - No LPIS found in the database "
                                    "for product %1 and siteid = %2 and year = %3")
                     .arg(prd.fullPath).arg(QString::number(prd.siteId)).arg(QString::number(prdCreatedYear)));
        return;
    }


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

QString S4CMarkersDB1Handler::CreateStepsForFilesMerge(const QStringList &dataExtrDirs, NewStepList &steps,
                              QList<TaskToSubmit> &allTasksList, int &curTaskIdx) {
    TaskToSubmit &mergeTask = allTasksList[curTaskIdx++];
    const QString &mergedFile = mergeTask.GetFilePath("MDB1_Extracted_Data.csv");
    const QStringList &mergeArgs = GetFilesMergeArgs(dataExtrDirs, mergedFile);
    steps.append(CreateTaskStep(mergeTask, "Markers1CsvMerge", mergeArgs));

    return mergedFile;
}

QString S4CMarkersDB1Handler::CreateStepsForAmpVVVHExtraction(const QString &mergedFile,
                                NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx) {
    TaskToSubmit &ampVVVHExtractTask = allTasksList[curTaskIdx++];
    const QString &mdb2ExtractFile = ampVVVHExtractTask.GetFilePath("MDB1_Extracted_Data_VVVH.csv");
    const QStringList &mdb2ExtractArgs =  { "Markers2Extractor", "-in", mergedFile, "-out", mdb2ExtractFile };
    steps.append(CreateTaskStep(ampVVVHExtractTask, "Markers2Extractor", mdb2ExtractArgs));

    return mdb2ExtractFile;
}

QString S4CMarkersDB1Handler::PrepareIpcExport(const MDB1JobPayload &jobCfg, TaskToSubmit &exportTask, const QString &prdType) {
    const auto &targetFolder = GetFinalProductFolder(*jobCfg.pCtx, jobCfg.event.jobId, jobCfg.event.siteId);
    const QString &strTimePeriod = QString("%1_%2").arg(jobCfg.minDate.toString("yyyyMMdd"),
                                                        jobCfg.maxDate.toString("yyyyMMdd"));
    const QString &creationDateStr = QDateTime::currentDateTime().toString("yyyyMMddTHHmmss");
    const QString &prdName = QString("SEN4CAP_%1_S%2_V%3_%4").arg(prdType, QString::number(jobCfg.event.siteId), strTimePeriod,
                                                                  creationDateStr);
    const QString &exportedFile = QString("%1/%2/%3.ipc").arg(targetFolder, prdName, prdName);
    WriteOutputProductPath(exportTask, exportedFile);

    return exportedFile;
}

QString S4CMarkersDB1Handler::CreateStepsForExportIpc(const MDB1JobPayload &jobCfg, const QString &inputFile,
                              NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx, const QString &prdType) {

    TaskToSubmit &exportTask = allTasksList[curTaskIdx++];
    const QString &exportedFile = PrepareIpcExport(jobCfg, exportTask, prdType);
    QStringList exportArgs = { "-i", inputFile, "-o", exportedFile,
                               "--int32-columns", "NewID"};
    steps.append(CreateTaskStep(exportTask, "MarkersDB1Export", exportArgs));

    return exportedFile;
}

QString S4CMarkersDB1Handler::CreateMdb3Steps(const MDB1JobPayload &jobCfg, const Season &season, const QString &mergedFile,
                                                  NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx)
{

    TaskToSubmit &inputTablesExtractTask = allTasksList[curTaskIdx++];
    const QString &inputTablesExtrFile = inputTablesExtractTask.GetFilePath("mdb3_input_tables.csv");
    QString yearStr = QString::number(season.startDate.year());
    const QStringList &inputTablesExtractTaskArgs = { "--year", yearStr,
                                                      "--site-short-name", jobCfg.siteShortName,
                                                      "--out", inputTablesExtrFile };
    steps.append(CreateTaskStep(inputTablesExtractTask, "MDB3InputTablesExtraction", inputTablesExtractTaskArgs));

    TaskToSubmit &mdb3TsaTask = allTasksList[curTaskIdx++];
    const QString &mdb3ExtrDir = mdb3TsaTask.GetFilePath("");
    const QStringList &mdb3TsaTaskArgs = { "TimeSeriesAnalysis", "-intype", "mcsv", "-debug", "0", "-allowgaps", "1", "-plotgraph", "1",
                                          "-rescontprd", "0", "-country", "NA", "-practice", "NA", "-year", yearStr,
                                          // TODO: for the next 3 lines values should be taken from config table  or other part
                                          "-optthrvegcycle", "350", "-ndvidw", "300", "-ndviup", "350", "-ndvistep", "5",
                                          "-optthrmin", "100", "-cohthrbase", "0.05", "-cohthrhigh", "0.15", "-cohthrabs", "0.75",
                                          "-ampthrmin", "0.1",
                                          "-harvestshp", inputTablesExtrFile,
                                          "-diramp", mergedFile, "-dircohe", mergedFile, "-dirndvi", mergedFile,
                                          "-outdir", mdb3ExtrDir };
    steps.append(CreateTaskStep(mdb3TsaTask, "TimeSeriesAnalysis", mdb3TsaTaskArgs));

    TaskToSubmit &markersExtractTask = allTasksList[curTaskIdx++];
    const QString &tsaResFileName = "Sen4CAP_L4C_NA_NA_" + yearStr + "_CSV.csv";
    const QString &tsaResFilePath = QDir(mdb3ExtrDir).filePath(tsaResFileName);
    const QString &mdb3IpcFile = PrepareIpcExport(jobCfg, markersExtractTask, "MDB3");
    const QString &dateStr = jobCfg.maxDate.toString("yyyyMMdd");
    QStringList mdb3MarkersExtrTaskArgs = { "-i", tsaResFilePath, "-d", dateStr, "-w", mdb3ExtrDir, "-o",  mdb3IpcFile};

    // We use previous MDB3 product only if scheduled jobs
    // TODO: we should also keep track of the scheduled jobs list
    if (jobCfg.isScheduledJob) {
        const ProductList &mdb3PrdsList = jobCfg.pCtx->GetProducts(jobCfg.event.siteId, (int)ProductType::S4MDB3ProductTypeId, jobCfg.minDate, jobCfg.maxDate);
        if (mdb3PrdsList.size() > 0) {
            mdb3MarkersExtrTaskArgs += "-p";
            mdb3MarkersExtrTaskArgs += mdb3PrdsList[mdb3PrdsList.size()-1].fullPath;
        }
    }
    steps.append(CreateTaskStep(markersExtractTask, "MDB3MarkersExtraction", mdb3MarkersExtrTaskArgs));

    return mdb3IpcFile;
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

