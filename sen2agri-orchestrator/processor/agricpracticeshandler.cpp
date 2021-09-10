#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "agricpracticeshandler.hpp"
#include "processorhandlerhelper.h"
#include "json_conversions.hpp"
#include "logger.hpp"
#include "s4c_utils.hpp"

#include "products/generichighlevelproducthelper.h"
using namespace orchestrator::products;

#define L4C_AP_GEN_CFG_PREFIX   "processor.s4c_l4c.cfg.gen."

#define L4C_AP_GEN_CC_CFG_PREFIX   "processor.s4c_l4c.cfg.gen.cc."
#define L4C_AP_GEN_FL_CFG_PREFIX   "processor.s4c_l4c.cfg.gen.fl."
#define L4C_AP_GEN_NFC_CFG_PREFIX   "processor.s4c_l4c.cfg.gen.nfc."
#define L4C_AP_GEN_NA_CFG_PREFIX   "processor.s4c_l4c.cfg.gen.na."

#define L4C_AP_TSA_CFG_PREFIX   "processor.s4c_l4c.cfg.tsa."
#define L4C_AP_TSA_CC_CFG_PREFIX   "processor.s4c_l4c.cfg.tsa.cc."
#define L4C_AP_TSA_FL_CFG_PREFIX   "processor.s4c_l4c.cfg.tsa.fl."
#define L4C_AP_TSA_NFC_CFG_PREFIX   "processor.s4c_l4c.cfg.tsa.nfc."
#define L4C_AP_TSA_NA_CFG_PREFIX   "processor.s4c_l4c.cfg.tsa.na."

#define L4C_AP_DEF_DATA_EXTR_ROOT   "/mnt/archive/agric_practices_files/{site}/{year}/data_extraction/"
#define L4C_AP_DEF_TS_ROOT          "/mnt/archive/agric_practices_files/{site}/{year}/ts_input_tables/"
#define L4C_AP_DEF_CFG_DIR          "/mnt/archive/agric_practices_files/{site}/{year}/config/"

#define SECS_TILL_EOD               86399   // 24 hour x 3600 + 59 minutes x 60 + 59 seconds

static QStringList ALL_PRACTICES_NAMES = {"CC", "FL", "NFC", "NA"};
static QStringList L4C_INPUT_MARKER_NAMES = {"NDVI", "AMP", "COHE"};

static bool compareL4CProductDates(const Product& prod1,const Product& prod2)
{
    return (prod1.created < prod2.created);
}

void AgricPracticesHandler::CreateTasks(const AgricPracticesJobPayload &jobCfg, QList<TaskToSubmit> &outAllTasksList,
                                        const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder)
{
    int curTaskIdx = 0;
    const QList<MarkerType> &enabledMarkers = dataExtrStepsBuilder.GetEnabledMarkers();
    QList<int> mergeTasksIndexes;
    for (const auto &marker: enabledMarkers) {
        // Create data extraction tasks if needed
        int minDataExtrIndex = curTaskIdx;
        dataExtrStepsBuilder.CreateTasks(marker, outAllTasksList, curTaskIdx);
        int maxDataExtrIndex = curTaskIdx-1;

        if (L4C_INPUT_MARKER_NAMES.contains(marker.marker)) {
            // create the merging tasks if needed
            int mergeTaskIdx = CreateMergeTasks(outAllTasksList, marker.marker.toLower() + "-data-extraction-merge",
                                                    minDataExtrIndex, maxDataExtrIndex, curTaskIdx);
            mergeTasksIndexes.push_back(mergeTaskIdx);
        }
    }

    if (IsOperationEnabled(jobCfg.execOper, catchCrop) || IsOperationEnabled(jobCfg.execOper, fallow) ||
        IsOperationEnabled(jobCfg.execOper, nfc) || IsOperationEnabled(jobCfg.execOper, harvestOnly)) {
        QList<int> practicesIdxs;
        for (const QString &practice: ALL_PRACTICES_NAMES) {
            int idx = CreateTSATasks(jobCfg, outAllTasksList, practice, mergeTasksIndexes, curTaskIdx);
            if (idx != -1) {
                practicesIdxs.append(idx);
            }
        }
        int productFormatterIdx = curTaskIdx++;
        outAllTasksList.append(TaskToSubmit{ "product-formatter", {} });
        // product formatter needs completion of time-series-analisys tasks
        for (int idx: practicesIdxs) {
            outAllTasksList[productFormatterIdx].parentTasks.append(outAllTasksList[idx]);
        }
        int exportPrdLauncherParentIdx = productFormatterIdx;
        if (jobCfg.isScheduledJob) {
            // add task for generating also the l4c markers db product
            outAllTasksList.append(TaskToSubmit{ "extract-l4c-markers", {outAllTasksList[productFormatterIdx]} });
            exportPrdLauncherParentIdx = curTaskIdx++;
        }
        // task for exporting the product as shp
        outAllTasksList.append(TaskToSubmit{ "export-product-launcher", {outAllTasksList[exportPrdLauncherParentIdx]} });
    }
}

void AgricPracticesHandler::CreateSteps(QList<TaskToSubmit> &allTasksList,
                                        const AgricPracticesJobPayload &jobCfg,
                                        const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder,
                                        NewStepList &steps)
{
    int curTaskIdx = 0;
    const QList<MarkerType> &enabledMarkers = dataExtrStepsBuilder.GetEnabledMarkers();
    // if only data extraction is needed, then we create the filter ids step into the general configured directory
    QMap<QString, QString> mergedFiles;
    for (const auto &marker: enabledMarkers) {
        QStringList dataExtrDirs;
        // Create the data extraction steps if needed
        dataExtrStepsBuilder.CreateSteps(marker, allTasksList, steps, curTaskIdx, dataExtrDirs);

        if (L4C_INPUT_MARKER_NAMES.contains(marker.marker)) {
            // If scheduled jobs, force adding the data extraction directories for all markers as data extraction source
            if (jobCfg.isScheduledJob) {
                // add a data extraction dir corresponding to the scheduled date which is saved as jobCfg.maxPrdDate
                const QString &dataExtrDirName = dataExtrStepsBuilder.GetDataExtractionDir(marker.marker);
                if (!dataExtrDirs.contains(dataExtrDirName)) {
                    QDir().mkpath(dataExtrDirName);
                    dataExtrDirs.append(dataExtrDirName);
                }
            }
            const QString &mergedFile = CreateStepsForFilesMerge(jobCfg, marker.prdType, dataExtrDirs, steps,
                                                                     allTasksList, curTaskIdx);
            mergedFiles[marker.marker] = mergedFile;
        }
    }

    if (IsOperationEnabled(jobCfg.execOper, catchCrop) || IsOperationEnabled(jobCfg.execOper, fallow) ||
        IsOperationEnabled(jobCfg.execOper, nfc) || IsOperationEnabled(jobCfg.execOper, harvestOnly)) {
        QStringList productFormatterFiles;
        for (const QString &practice: ALL_PRACTICES_NAMES) {
            productFormatterFiles += CreateTimeSeriesAnalysisSteps(jobCfg, practice, mergedFiles, steps,
                                                                           allTasksList, curTaskIdx);
        }
        TaskToSubmit &productFormatterTask = allTasksList[curTaskIdx++];
        const QStringList &productFormatterArgs = GetProductFormatterArgs(productFormatterTask, jobCfg,
                                                                          productFormatterFiles);
        steps.append(CreateTaskStep(productFormatterTask, "ProductFormatter", productFormatterArgs));

        const auto & productFormatterPrdFileIdFile = productFormatterTask.GetFilePath("prd_infos.txt");

        if (jobCfg.isScheduledJob) {
            CreateStepsForExportL4CMarkers(jobCfg, steps, allTasksList, curTaskIdx, productFormatterPrdFileIdFile);
        }
        TaskToSubmit &exportCsvToShpProductTask = allTasksList[curTaskIdx++];
        const QStringList &exportCsvToShpProductArgs = GetExportProductLauncherArgs(jobCfg, productFormatterPrdFileIdFile);
        steps.append(CreateTaskStep(exportCsvToShpProductTask, "export-product-launcher", exportCsvToShpProductArgs));
    }
}

void AgricPracticesHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                              const JobSubmittedEvent &evt)
{
    S4CMarkersDB1DataExtractStepsBuilder dataExtrStepsBuilder;
    const auto &parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
    dataExtrStepsBuilder.Initialize(processorDescr.shortName, ctx, parameters, evt.siteId, evt.jobId, {"NDVI", "AMP", "COHE"});

    AgricPracticesJobPayload jobCfg(&ctx, evt, dataExtrStepsBuilder.GetDataExtractionMinDate(),
                                    dataExtrStepsBuilder.GetDataExtractionMaxDate());
    if (jobCfg.isScheduledJob) {
        // fill also payload season start/end dates if possible
        ConfigurationParameterValueMap mapOverrides;
        GetSeasonStartEndDates(ctx.GetSiteSeasons(evt.siteId), jobCfg.seasonStartDate,
                               jobCfg.seasonEndDate, jobCfg.maxDate, mapOverrides);
    }
    qDebug() << "Using the year " << jobCfg.siteCfg.year;

    QString errMsg;
    if (!CheckExecutionPreconditions(jobCfg.pCtx, jobCfg.parameters, jobCfg.configParameters, jobCfg.event.siteId,
                                     jobCfg.siteShortName, jobCfg.siteCfg.year, errMsg)) {
        jobCfg.pCtx->MarkJobFailed(jobCfg.event.jobId);
        throw std::runtime_error(errMsg.toStdString());
    }

    jobCfg.siteCfg.practices = GetPracticeTableFiles(jobCfg.parameters, jobCfg.configParameters, jobCfg.siteShortName, jobCfg.siteCfg.year);

    if (!GetL4CConfigForSiteId(jobCfg)) {
        ctx.MarkJobFailed(evt.jobId);
        throw std::runtime_error(
            QStringLiteral("Cannot find L4C configuration file for site %1 and year %2\n")
                    .arg(jobCfg.siteShortName)
                    .arg(jobCfg.siteCfg.year).toStdString());
    }

    QList<TaskToSubmit> allTasksList;
    CreateTasks(jobCfg, allTasksList, dataExtrStepsBuilder);

    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    for(TaskToSubmit &task: allTasksList) {
        allTasksListRef.append(task);
    }
    SubmitTasks(ctx, evt.jobId, allTasksListRef);
    NewStepList allSteps;
    CreateSteps(allTasksList, jobCfg, dataExtrStepsBuilder, allSteps);
    ctx.SubmitSteps(allSteps);
}

void AgricPracticesHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                              const TaskFinishedEvent &event)
{
    if (event.module == "product-formatter") {

        const QString &prodName = GetOutputProductName(ctx, event);
        const QString &productFolder = GetFinalProductFolder(ctx, event.jobId, event.siteId) + "/" + prodName;
        if(prodName != "") {
            const QString &quicklook = GetProductFormatterQuicklook(ctx, event);
            const QString &footPrint = GetProductFormatterFootprint(ctx, event);
            // Insert the product into the database
            GenericHighLevelProductHelper prdHelper(productFolder);
            int prdId = ctx.InsertProduct({ ProductType::S4CL4CProductTypeId, event.processorId, event.siteId,
                                event.jobId, productFolder, prdHelper.GetAcqDate(),
                                prodName, quicklook, footPrint, std::experimental::nullopt, TileIdList(), ProductIdsList()  });

            const QString &prodFolderOutPath = ctx.GetOutputPath(event.jobId, event.taskId, event.module, processorDescr.shortName) +
                    "/" + "prd_infos.txt";

            QFile file( prodFolderOutPath );
            if ( file.open(QIODevice::ReadWrite) )
            {
                QTextStream stream( &file );
                stream << prdId << ";" << productFolder << endl;
            }
        } else {
            Logger::error(QStringLiteral("Cannot insert into database the product with name %1 and folder %2").arg(prodName).arg(productFolder));
        }
    } else if (event.module == "extract-l4c-markers") {

        const QString &productPath = GetOutputProductPath(ctx, event);
        const QString &prodName = GetOutputProductName(ctx, event);
        QFileInfo fileInfo(prodName);
        const QString &prdNameNoExt = fileInfo.baseName ();
        if(QFileInfo::exists(productPath) && prdNameNoExt != "") {
            const QString &footPrint = GetProductFormatterFootprint(ctx, event);
            // Insert the product into the database
            GenericHighLevelProductHelper prdHelper(productPath);
            ProductType prdType = ProductType::S4MDB3ProductTypeId;
            ctx.InsertProduct({ prdType, event.processorId, event.siteId,
                                event.jobId, productPath, prdHelper.GetAcqDate(),
                                prdNameNoExt, "", footPrint, std::experimental::nullopt, TileIdList(), ProductIdsList()  });
        } else {
            Logger::error(QStringLiteral("Cannot insert into database the product with name %1 and path %2").arg(prdNameNoExt).arg(productPath));
        }
    }
    else if ((event.module == "export-product-launcher") || (event.module.endsWith("-data-extraction-only"))) {
        ctx.MarkJobFinished(event.jobId);
        // Now remove the job folder containing temporary files
        RemoveJobFolder(ctx, event.jobId, processorDescr.shortName);
    }
}

QStringList AgricPracticesHandler::GetExportProductLauncherArgs(const AgricPracticesJobPayload &jobCfg,
                                                                const QString &productFormatterPrdFileIdFile) {
    // Get the path for the ogr2ogr
    const auto &paths = jobCfg.pCtx->GetJobConfigurationParameters(jobCfg.event.jobId, QStringLiteral("executor.module.path.ogr2ogr"));
    QString ogr2OgrPath = "ogr2ogr";
    for (const auto &p : paths) {
        ogr2OgrPath = p.second;
        break;
    }

    const QStringList &exportCsvToShpProductArgs = { "-f", productFormatterPrdFileIdFile,
                                                      "-o", "Sen4CAP_L4C_PRACTICE_" + jobCfg.siteCfg.country + "_" +
                                                     jobCfg.siteCfg.year + ".gpkg",
                                                     "-g", ogr2OgrPath
                                                };
    return exportCsvToShpProductArgs;
}

QString AgricPracticesHandler::CreateStepsForExportL4CMarkers(const AgricPracticesJobPayload &jobCfg,
                                                                  NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx,
                                                                const QString &productFormatterPrdFileIdFile) {

    TaskToSubmit &exportTask = allTasksList[curTaskIdx++];
    const auto &targetFolder = GetFinalProductFolder(*jobCfg.pCtx, jobCfg.event.jobId, jobCfg.event.siteId);
    const QString &strTimePeriod = QString("%1_%2").arg(jobCfg.minDate.toString("yyyyMMdd"),
                                                        jobCfg.maxDate.toString("yyyyMMdd"));
    const QString &creationDateStr = QDateTime::currentDateTime().toString("yyyyMMddTHHmmss");
    const QString &prdName = QString("SEN4CAP_MDB3_S%1_V%2_%3").arg(QString::number(jobCfg.event.siteId), strTimePeriod,
                                                                  creationDateStr);
    const QString &exportedFile = QString("%1/%2/%3.ipc").arg(targetFolder, prdName, prdName);
    WriteOutputProductPath(exportTask, exportedFile);

    const QString &schedPrdsHistFile = GetSchedL4CPrdsHistoryFile(jobCfg.parameters, jobCfg.configParameters, jobCfg.siteShortName, jobCfg.siteCfg.year);
    const QStringList &exportL4CMarkersProductArgs = { "--site", QString::number(jobCfg.event.siteId),
                                                     "--year", jobCfg.siteCfg.year,
                                                     "--new-prd-info-file", productFormatterPrdFileIdFile,
                                                     "--prds-history-files", schedPrdsHistFile,
                                                     "--add-no-data-rows", QString::number(jobCfg.siteCfg.bMarkersAddNoDataRows),
                                                     "-o", exportedFile
                                                };
    steps.append(CreateTaskStep(exportTask, "export-l4c-markers", exportL4CMarkersProductArgs));
    return exportedFile;
}

QStringList AgricPracticesHandler::GetProductFormatterArgs(TaskToSubmit &productFormatterTask, const AgricPracticesJobPayload &jobCfg,
                                                           const QStringList &listFiles) {
    // ProductFormatter /home/cudroiu/sen2agri-processors-build
    //    -vectprd 1 -destroot /mnt/archive_new/test/Sen4CAP_L4C_Tests/NLD_Validation_TSA/OutPrdFormatter
    //    -fileclass OPER -level S4C_L4C -baseline 01.00 -siteid 4 -timeperiod 20180101_20181231 -processor generic
    //    -processor.generic.files <files_list>

    const auto &targetFolder = GetFinalProductFolder(*jobCfg.pCtx, jobCfg.event.jobId, jobCfg.event.siteId);
    const auto &outPropsPath = productFormatterTask.GetFilePath(PRODUCT_FORMATTER_OUT_PROPS_FILE);
    const auto &executionInfosPath = productFormatterTask.GetFilePath("executionInfos.txt");
    QString strTimePeriod = jobCfg.minDate.toString("yyyyMMddTHHmmss").append("_").append(jobCfg.maxDate.toString("yyyyMMddTHHmmss"));
    QStringList productFormatterArgs = { "ProductFormatter",
                                         "-destroot", targetFolder,
                                         "-fileclass", "OPER",
                                         "-level", "S4C_L4C",
                                         "-vectprd", "1",
                                         "-baseline", "01.00",
                                         "-siteid", QString::number(jobCfg.event.siteId),
                                         "-timeperiod", strTimePeriod,
                                         "-processor", "generic",
                                         "-outprops", outPropsPath,
                                         "-gipp", executionInfosPath };
    productFormatterArgs += "-processor.generic.files";
    productFormatterArgs += listFiles;

    return productFormatterArgs;
}

ProcessorJobDefinitionParams AgricPracticesHandler::GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
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


    ConfigurationParameterValueMap mapCfg = ctx.GetConfigurationParameters(QString(L4C_AP_CFG_PREFIX),
                                                                           siteId, requestOverrideCfgValues);
    std::map<QString, QString> configParams;
    for (const auto &p : mapCfg) {
        configParams.emplace(p.key, p.value);
    }

    // If the execution conditions are not met, then exit
    QJsonObject parameters;
    const QString &siteShortName = ctx.GetSiteShortName(siteId);
    QString errMsg;

    const QString &yearStr = AgricPracticesJobPayload::GetYear(parameters, configParams, siteShortName);
    if (!CheckExecutionPreconditions(&ctx, parameters, configParams, siteId, siteShortName, yearStr, errMsg)) {
        Logger::error("Scheduled job execution: " + errMsg);
        return params;
    }

    // we might have an offset in days from starting the downloading products to start the S4C_L4C production
    // TODO: Is this really needed
    int startSeasonOffset = mapCfg["processor.s4c_l4c.start_season_offset"].value.toInt();
    seasonStartDate = seasonStartDate.addDays(startSeasonOffset);

    // Get the start and end date for the production
    QDateTime endDate = qScheduledDate;
    QDateTime startDate = seasonStartDate;

    params.jsonParameters.append("{ \"scheduled_job\": \"1\", \"start_date\": \"" + startDate.toString("yyyyMMdd") + "\", " +
                                 "\"end_date\": \"" + endDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_start_date\": \"" + seasonStartDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_end_date\": \"" + seasonEndDate.toString("yyyyMMdd") + "\"");
    if(requestOverrideCfgValues.contains("product_type")) {
        const ConfigurationParameterValue &productType = requestOverrideCfgValues["product_type"];
        params.jsonParameters.append(", \"execution_operation\": \"" + productType.value + "\"}");
    } else {
        params.jsonParameters.append("}");
    }

    params.isValid = true;

    return params;
}

bool AgricPracticesHandler::GetL4CConfigForSiteId(AgricPracticesJobPayload &jobCfg)
{
    const QString &siteCfgFilePath = GetL4CConfigFilePath(jobCfg);
    if (siteCfgFilePath == "") {
        return false;
    }

    return LoadL4CConfigFile(jobCfg, siteCfgFilePath);
}

QString AgricPracticesHandler::GetL4CConfigFilePath(AgricPracticesJobPayload &jobCfg)
{
    QString strCfgPath;
    const QString &strCfgDir = GetProcessorDirValue(jobCfg.parameters, jobCfg.configParameters,
                                                    "cfg_dir", jobCfg.siteShortName,
                                                    jobCfg.siteCfg.year, L4C_AP_DEF_CFG_DIR);
    QDir directory(strCfgDir);
    QString preferedCfgFileName = "S4C_L4C_Config_" + jobCfg.siteCfg.country + "_" + jobCfg.siteCfg.year + ".cfg";
    if (directory.exists()) {
        const QStringList &dirFiles = directory.entryList(QStringList() <<"*.cfg",QDir::Files);
        foreach(const QString &fileName, dirFiles) {
            if (strCfgPath.size() == 0) {
                // get the first available file
                strCfgPath = directory.filePath(fileName);
            }
            // check if the filename is a prefered file name
            if (fileName == preferedCfgFileName) {
                strCfgPath = directory.filePath(fileName);
                break;
            }
        }
    }
    // get the default config path
    if (strCfgPath.size() == 0 || strCfgPath == "N/A") {
        strCfgPath = ProcessorHandlerHelper::GetStringConfigValue(jobCfg.parameters, jobCfg.configParameters,
                                                                                   "default_config_path", L4C_AP_CFG_PREFIX);
    }

    if (strCfgPath.isEmpty() || strCfgPath == "N/A" || !QFileInfo::exists(strCfgPath)) {
        return "";
    }
    return strCfgPath;
}

bool AgricPracticesHandler::LoadL4CConfigFile(AgricPracticesJobPayload &jobCfg,
                                               const QString &siteCfgFilePath)
{
    AgricPracticesSiteCfg &cfg = jobCfg.siteCfg;

    // Now load the content of the file
    QSettings settings(siteCfgFilePath, QSettings::IniFormat);

    // Load the default practices
    const TQStrQStrMap &defTsaParams = LoadParamsFromFile(settings, "", "DEFAULT_TIME_SERIES_ANALYSIS_PARAMS", cfg);
    // Parameters used for practices tables extraction
    for (const QString &practice: cfg.practices.keys()) {
        TQStrQStrMap *pTsaParams;
        if (practice == "CC") {
            pTsaParams = &cfg.ccTsaParams;
        } else if (practice == "FL") {
            pTsaParams = &cfg.flTsaParams;
        } else if (practice == "NFC") {
            pTsaParams = &cfg.nfcTsaParams;
        } else if (practice == "NA") {
            pTsaParams = &cfg.naTsaParams;
        }

        (*pTsaParams) = LoadParamsFromFile(settings, practice, practice + "_TIME_SERIES_ANALYSIS_PARAMS", cfg);
        UpdatePracticesParams(defTsaParams, *pTsaParams);
    }

    jobCfg.siteCfg = cfg;
    return true;
}

QStringList AgricPracticesHandler::GetFilesMergeArgs(const QStringList &listInputPaths, const QString &outFileName,
                                                     const QDateTime &)
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

QStringList AgricPracticesHandler::GetTimeSeriesAnalysisArgs(const AgricPracticesJobPayload &jobCfg, const QString &practice,
                                                             const QString &practicesFile, const QMap<QString, QString> &inFiles,
                                                             const QString &outDir)
{
    const TQStrQStrMap *pTsaPracticeParams = 0;
    QString tsaExpectedPractice = GetTsaExpectedPractice(practice);
    if (practice == "CC") {
        pTsaPracticeParams = &(jobCfg.siteCfg.ccTsaParams);
    } else if (practice == "FL") {
        pTsaPracticeParams = &(jobCfg.siteCfg.flTsaParams);
    } else if (practice == "NFC") {
        pTsaPracticeParams = &(jobCfg.siteCfg.nfcTsaParams);
    } else if (practice == "NA") {
        pTsaPracticeParams = &(jobCfg.siteCfg.naTsaParams);
    }
    QStringList retArgs = { "TimeSeriesAnalysis", "-intype", "mcsv", "-debug", "0", "-allowgaps", "1", "-plotgraph", "1",
                            "-rescontprd", "0", "-country", jobCfg.siteCfg.country, "-practice", tsaExpectedPractice,
                            "-year", jobCfg.siteCfg.year, "-harvestshp", practicesFile,
                            "-diramp", inFiles["AMP"], "-dircohe", inFiles["COHE"], "-dirndvi", inFiles["NDVI"],
                            "-tillage", QString::number(jobCfg.siteCfg.bTillageMonitoring), "-outdir", outDir };
    if (jobCfg.siteCfg.tsaMinAcqsNo.size() > 0) {
        retArgs += "-minacqs";
        retArgs += jobCfg.siteCfg.tsaMinAcqsNo;
    }
    for (TQStrQStrMap::const_iterator it=pTsaPracticeParams->begin(); it!=pTsaPracticeParams->end(); ++it) {
        if (it->second.size() > 0) {
            retArgs += ("-" + it->first);
            retArgs += it->second;
        }
    }
    QString prevL4CPrd;
    if (ProcessorHandlerHelper::GetBoolConfigValue(jobCfg.parameters, jobCfg.configParameters, "use_prev_prd", L4C_AP_CFG_PREFIX) &&
            GetPrevL4CProduct(jobCfg, jobCfg.seasonStartDate, jobCfg.maxDate, prevL4CPrd)) {
        retArgs += "-prevprd";
        retArgs += prevL4CPrd;
    }
    return retArgs;
}

QString AgricPracticesHandler::BuildMergeResultFileName(const QString &country, const QString &year, const ProductType &prdsType)
{
    return QString(country).append("_").append(year).append("_").
            append(GetShortNameForProductType(prdsType)).append("_Extracted_Data.csv");
}

QString AgricPracticesHandler::BuildPracticesTableResultFileName(const QString &practice, const QString &year, const QString &country)
{
    QString ret = QString("Sen4CAP_L4C_").append(practice);
    if (country.size() > 0) {
        ret.append("_");
    }
    return ret.append(country).append("_").append(year).append(".csv");
}

int AgricPracticesHandler::CreateMergeTasks(QList<TaskToSubmit> &outAllTasksList, const QString &taskName,
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

int AgricPracticesHandler::CreateTSATasks(const AgricPracticesJobPayload &jobCfg, QList<TaskToSubmit> &outAllTasksList,
                                          const QString &practiceName,  const QList<int> &mergeTaskIdxs, int &curTaskIdx) {
    int ccTsaIdx = -1;
    if (mergeTaskIdxs.size() != 3) {
        jobCfg.pCtx->MarkJobFailed(jobCfg.event.jobId);
        throw std::runtime_error(QStringLiteral("The number of merging steps should be 3 but found %1 for site %2.").
                                 arg(mergeTaskIdxs.size()).arg(jobCfg.siteShortName).toStdString());
    }
    AgricPractOperation expectedOper = GetExecutionOperation(practiceName);
    if (jobCfg.siteCfg.practices.keys().contains(practiceName) && IsOperationEnabled(jobCfg.execOper, expectedOper)) {
        // this task is independent and can be executed before any others
        const QString &lowerPracticeName = practiceName.toLower();
        outAllTasksList.append(TaskToSubmit{ lowerPracticeName + "-time-series-analysis",
                                             {outAllTasksList[mergeTaskIdxs[0]],
                                              outAllTasksList[mergeTaskIdxs[1]],
                                              outAllTasksList[mergeTaskIdxs[2]]} });
        ccTsaIdx = curTaskIdx++;
    }
    return ccTsaIdx;
}

QString AgricPracticesHandler::CreateStepsForFilesMerge(const AgricPracticesJobPayload &jobCfg, const ProductType &prdType,
                              const QStringList &dataExtrDirs, NewStepList &steps,
                              QList<TaskToSubmit> &allTasksList, int &curTaskIdx) {
    TaskToSubmit &mergeTask = allTasksList[curTaskIdx++];
    const QString &mergedFile = mergeTask.GetFilePath(BuildMergeResultFileName(jobCfg.siteCfg.country, jobCfg.siteCfg.year, prdType));
    const QStringList &mergeArgs = GetFilesMergeArgs(dataExtrDirs, mergedFile, jobCfg.maxDate);
    steps.append(CreateTaskStep(mergeTask, "Markers1CsvMerge", mergeArgs));

    return mergedFile;
}

QStringList AgricPracticesHandler::CreateTimeSeriesAnalysisSteps(const AgricPracticesJobPayload &jobCfg, const QString &practice,
                                                                 const QMap<QString, QString> &mergedFiles, NewStepList &steps,
                                                                 QList<TaskToSubmit> &allTasksList, int &curTaskIdx)
{
    QStringList retList;
    AgricPractOperation oper = GetExecutionOperation(practice);
    if (jobCfg.siteCfg.practices.keys().contains(practice) && ((oper & jobCfg.execOper) != none)) {
        //const QString &practicesFile = CreateStepForLPISSelection(practice, jobCfg, allTasksList, steps, curTaskIdx);

        TaskToSubmit &timeSeriesAnalysisTask = allTasksList[curTaskIdx++];
        const QString &timeSeriesExtrDir = timeSeriesAnalysisTask.GetFilePath("");
        const QStringList &timeSeriesAnalysisArgs = GetTimeSeriesAnalysisArgs(jobCfg, practice,
                                                                              jobCfg.siteCfg.practices.value(practice),
                                                                              mergedFiles, timeSeriesExtrDir);
        steps.append(CreateTaskStep(timeSeriesAnalysisTask, "TimeSeriesAnalysis", timeSeriesAnalysisArgs));

        // Add the expected files to the productFormatterFiles
        const QString &tsaExpPractice = GetTsaExpectedPractice(practice);
        const QString &filesPrefix = "Sen4CAP_L4C_" + tsaExpPractice + "_" + jobCfg.siteCfg.country + "_" + jobCfg.siteCfg.year;
        const QString &mainFileName = filesPrefix + "_CSV.csv";
        const QString &plotFileName = filesPrefix + "_PLOT.xml";
        const QString &plotIdxFileName = plotFileName + ".idx";
        const QString &contPrdFileName = filesPrefix + "_CSV_ContinousProduct.csv";

        retList.append(QDir(timeSeriesExtrDir).filePath(mainFileName));
        retList.append(QDir(timeSeriesExtrDir).filePath(plotFileName));
        retList.append(QDir(timeSeriesExtrDir).filePath(plotIdxFileName));
        retList.append(QDir(timeSeriesExtrDir).filePath(contPrdFileName));
    }
    return retList;
}

TQStrQStrMap AgricPracticesHandler::LoadParamsFromFile(QSettings &settings, const QString &practicePrefix, const QString &sectionName,
                                                       const AgricPracticesSiteCfg &cfg) {
    TQStrQStrMap params;
    //const QString &sectionName = (practicePrefix.length() > 0 ? practicePrefix : QString("DEFAULT")) + "_PRACTICES_PARAMS/";
    QString keyPrefix;
    if(practicePrefix.length() > 0) {
        keyPrefix = practicePrefix + "_";
    }
    settings.beginGroup(sectionName);
    const QStringList &keys = settings.allKeys();
    foreach (const QString &key, keys) {
        QString keyNoPrefix = key.trimmed();
        if (keyNoPrefix.startsWith('#')) {
            // comment line that contains an '=' and falsely extracted as key - value pair
            continue;   // ignore it
        }
        keyNoPrefix.remove(0, keyPrefix.size());
        QString value = settings.value(key).toString();
        value.replace("${YEAR}", cfg.year);
        params.insert(TQStrQStrPair(keyNoPrefix.toLower(), value));
    }
    settings.endGroup();

    return params;
}

void AgricPracticesHandler::UpdatePracticesParams(const QJsonObject &parameters, std::map<QString, QString> &configParameters,
                                                  const TQStrQStrMap &cfgVals, const QString &prefix,
                                                  TQStrQStrMap *params) {
    for (TQStrQStrMap::const_iterator it=cfgVals.begin(); it!=cfgVals.end(); ++it) {
        QString key = it->first;
        // get the last part of the key, without prefix
        key.remove(0, prefix.size());
        // check if the value for this key is somehow provided in the parameters
        // otherwise, take it from the config parameters
        const QString &value = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, key, prefix).trimmed();
        if(value.size() > 0) {
            params->insert(TQStrQStrPair(key, value));
        }
    }
}

void AgricPracticesHandler::UpdatePracticesParams(const TQStrQStrMap &defVals,
                                                TQStrQStrMap &sectionVals) {
    for (TQStrQStrMap::const_iterator it=defVals.begin(); it!=defVals.end(); ++it) {
        if (sectionVals.find(it->first) == sectionVals.end()) {
            // insert the default value
            sectionVals.insert(TQStrQStrPair(it->first, it->second));
        }
    }
}
QString AgricPracticesHandler::GetTsaExpectedPractice(const QString &practice)
{
    QString retPractice = practice;
    if (practice == "CC") {
        retPractice = "CatchCrop";
    } else if (practice == "FL") {
        retPractice = "Fallow";
    }
    return retPractice;
}

AgricPractOperation AgricPracticesHandler::GetExecutionOperation(const QString &str)
{
    if (QString::compare(str, "AllTimeSeriesAnalysis", Qt::CaseInsensitive) == 0) {
        return timeSeriesAnalysis;
    } else if (QString::compare(str, "CatchCrop", Qt::CaseInsensitive) == 0 ||
               QString::compare(str, "CC", Qt::CaseInsensitive) == 0) {
        return catchCrop;
    } else if (QString::compare(str, "Fallow", Qt::CaseInsensitive) == 0 ||
               QString::compare(str, "FL", Qt::CaseInsensitive) == 0) {
        return fallow;
    } else if (QString::compare(str, "NFC", Qt::CaseInsensitive) == 0) {
        return nfc;
    } else if (QString::compare(str, "HarvestOnly", Qt::CaseInsensitive) == 0 ||
               QString::compare(str, "NA", Qt::CaseInsensitive) == 0) {
        return harvestOnly;
    } else if (QString::compare(str, "ALL", Qt::CaseInsensitive) == 0) {
        return all;
    }
    return none;
}

bool AgricPracticesHandler::IsOperationEnabled(AgricPractOperation oper, AgricPractOperation expected) {
    return ((oper & expected) != none);
}

void AgricPracticesHandler::HandleProductAvailableImpl(EventProcessingContext &ctx,
                                const ProductAvailableEvent &event)
{
    // Get the product description from the database
    const Product &prd = ctx.GetProduct(event.productId);
    QJsonObject parameters;
    auto configParameters = ctx.GetConfigurationParameters(L4C_AP_CFG_PREFIX, prd.siteId);
    const QString &siteShortName = ctx.GetSiteShortName(prd.siteId);

    // Check that the product type is NDVI, AMP or COHE
    const QString &prdTypeShortName = GetShortNameForProductType(prd.productTypeId);
    if (prdTypeShortName.size() == 0) {
        // Ignore silently to avoid poluting the log messages
        //Logger::error(QStringLiteral("Agric_practices - HandleProductAvailable - Unsupported product type %1. Ignoring it ...").arg(QString::number((int)prd.productTypeId)));
        return;
    }

    QString errMsg;
    const QString &yearStr = AgricPracticesJobPayload::GetYear(parameters, configParameters, siteShortName);
    if (!CheckExecutionPreconditions(&ctx, parameters, configParameters, prd.siteId, siteShortName,
                                     yearStr, errMsg)) {
        Logger::info(QStringLiteral("Agric_practices - HandleProductAvailable - Cannot trigger data extraction job "
                     "for product %1 and siteid = %2. The error was: %3").arg(prd.fullPath).arg(QString::number((int)prd.siteId))
                     .arg(errMsg));
        return;
    }

    // check if the NRT data extraction is configured for the site
    bool nrtDataExtrEnabled = ProcessorHandlerHelper::GetBoolConfigValue(parameters, configParameters, "nrt_data_extr_enabled", L4C_AP_CFG_PREFIX);
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
        Logger::info(QStringLiteral("Agric_practices - HandleProductAvailable - Submitted data extraction trigger job "
                                    "for product %1 and siteid = %2").arg(prd.fullPath).arg(QString::number((int)prd.siteId)));
    }
}

bool AgricPracticesHandler::GetPrevL4CProduct(const AgricPracticesJobPayload &jobCfg,  const QDateTime &seasonStart,
                                              const QDateTime &curDate, QString &prevL4cProd) {
    ProductList l4cPrds = jobCfg.pCtx->GetProducts(jobCfg.event.siteId, (int)ProductType::S4CL4CProductTypeId, seasonStart, curDate);
    ProductList l4cPrdsFiltered;
    for (const Product &prd: l4cPrds) {
        if (prd.created.addDays(3) > curDate) {
            continue;
        }
        l4cPrdsFiltered.append(prd);
    }
    if (l4cPrdsFiltered.size() > 0) {
        qSort(l4cPrdsFiltered.begin(), l4cPrdsFiltered.end(), compareL4CProductDates);
        // remove products that have the same day as the current one
        prevL4cProd = l4cPrdsFiltered[l4cPrdsFiltered.size()-1].fullPath;
        return true;
    }
    return false;
}

ProductList AgricPracticesHandler::GetLpisProduct(ExecutionContextBase *pCtx, int siteId) {
    // We take it the last LPIS product for this site.
    QDate  startDate, endDate;
    startDate.setDate(1970, 1, 1);
    QDateTime startDateTime(startDate);
    endDate.setDate(2050, 12, 31);
    QDateTime endDateTime(endDate);
    return pCtx->GetProducts(siteId, (int)ProductType::S4CLPISProductTypeId, startDateTime, endDateTime);
}

QString AgricPracticesHandler::GetShortNameForProductType(const ProductType &prdType) {
    switch(prdType) {
        case ProductType::L3BProductTypeId:         return "NDVI";
        case ProductType::S4CS1L2AmpProductTypeId:  return "AMP";
        case ProductType::S4CS1L2CoheProductTypeId: return "COHE";
        default:                                    return "";
    }
}

QString AgricPracticesHandler::GetProcessorDirValue(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                                    const QString &key, const QString &siteShortName, const QString &year,
                                                    const QString &defVal ) {
    QString dataExtrDirName = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, key, L4C_AP_CFG_PREFIX);

    if (dataExtrDirName.size() == 0) {
        dataExtrDirName = defVal;
    }
    dataExtrDirName = dataExtrDirName.replace("{site}", siteShortName);
    dataExtrDirName = dataExtrDirName.replace("{year}", year);
    dataExtrDirName = dataExtrDirName.replace("{processor}", processorDescr.shortName);

    return dataExtrDirName;

}

QString AgricPracticesHandler::GetTsInputTablesDir(const QJsonObject &parameters, const std::map<QString,
                                                   QString> &configParameters, const QString &siteShortName,
                                                   const QString &year, const QString &practice) {

    // we expect the value to be something like /mnt/archive/agric_practices_files/{site}/{year}/ts_input_tables/{practice}/
    QString val = GetProcessorDirValue(parameters, configParameters, "ts_input_tables_dir", siteShortName, year,
                                       L4C_AP_DEF_TS_ROOT + practice);
    if (val.indexOf("{practice}") == -1) {
        // force the practice to be added at the end of the directory
        return QDir(val).filePath(practice);
    }
    return val.replace("{practice}", practice);
}

QString AgricPracticesHandler::GetSchedL4CPrdsHistoryFile(const QJsonObject &parameters, const std::map<QString,
                                                   QString> &configParameters, const QString &siteShortName,
                                                   const QString &year) {

    // we expect the value to be something like /mnt/archive/agric_practices_files/{site}/{year}/l4c_scheduled_prds_history.txt
    return GetProcessorDirValue(parameters, configParameters, "sched_prds_hist_file", siteShortName, year,
                                       QString(L4C_AP_DEF_TS_ROOT) + "l4c_scheduled_prds_history.txt");
}

QMap<QString, QString> AgricPracticesHandler::GetPracticeTableFiles(const QJsonObject &parameters,
                                                                 const std::map<QString, QString> &configParameters,
                                                                 const QString &siteShortName, const QString &year) {
    QMap<QString, QString> retMap;
    const QStringList &practices = ProcessorHandlerHelper::GetStringConfigValue(parameters,
                                    configParameters, "practices", L4C_AP_CFG_PREFIX).split(",");
    for (const QString &strPractice: practices) {
        const QString &strTrimmedPractice = strPractice.trimmed();
        if (!ALL_PRACTICES_NAMES.contains(strTrimmedPractice)) {
            // ignore unknow practice names
            Logger::warn(QStringLiteral("Unknown practice name %1 configured for site %2. Just ignoring it ...").
                         arg(strTrimmedPractice).arg(siteShortName));
            continue;
        }
        const QString &tsInputTablesDir = GetTsInputTablesDir(parameters, configParameters, siteShortName, year, strTrimmedPractice);
        const QString &fileName = BuildPracticesTableResultFileName(strTrimmedPractice, year);
        const QString &practiceFilePath = QDir(tsInputTablesDir).filePath(fileName);
        if(QFileInfo(practiceFilePath).exists()) {
            retMap.insert(strTrimmedPractice, practiceFilePath);
        } else {
            // just to check the compatibility with the old naming, if the input tables are not regenerated
            // @Deprecated ... TODO to be removed in a future version
            const QString &country = ProcessorHandlerHelper::GetStringConfigValue(parameters,
                                               configParameters, "country", L4C_AP_CFG_PREFIX);
            const QString &fileName2 = BuildPracticesTableResultFileName(strTrimmedPractice, year, country);
            const QString &practiceFilePath2 = QDir(tsInputTablesDir).filePath(fileName2);
            if(QFileInfo(practiceFilePath2).exists()) {
                retMap.insert(strTrimmedPractice, practiceFilePath2);
            } else {
                // Just put an empty string and let the caller decide what to do
                retMap.insert(strTrimmedPractice, "");
            }
        }
    }

    return retMap;
}

bool AgricPracticesHandler::CheckExecutionPreconditions(ExecutionContextBase *pCtx, const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                                        int siteId, const QString &siteShortName, const QString &year, QString &errMsg) {
    errMsg = "";
    // We take it the last LPIS product for this site.
    const ProductList &lpisPrds = GetLpisProduct(pCtx, siteId);
    if (lpisPrds.size() == 0) {
        errMsg = QStringLiteral("ERROR Agric Practices: No LPIS product found for site %1.").
                                 arg(siteShortName);
        return false;
    }
    const QMap<QString, QString> &retMap = GetPracticeTableFiles(parameters, configParameters, siteShortName, year);
    for(const QString & practice : retMap.keys()) {
        // if it is not NA and a file does not exist, then return false
        // We allow the NA to have no files as this is activated by default in the system
        if (retMap.value(practice).size() == 0) {
            errMsg = QStringLiteral("Error checking S4C_L4C preconditions for site %1. "
                                    "The practice %2 does not have a configured practices file for year %3!.")
                    .arg(siteShortName).arg(practice).arg(year);
            return false;
        }
    }
    return true;
}

// ###################### AgricPracticesJobCfg functions ############################

AgricPractOperation AgricPracticesJobPayload::GetExecutionOperation(const QJsonObject &parameters, const std::map<QString, QString> &configParameters)
{
    const QString &execOper = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "execution_operation", L4C_AP_CFG_PREFIX);
    return AgricPracticesHandler::GetExecutionOperation(execOper);
}

QString AgricPracticesJobPayload::GetYear(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                                  const QString &siteShortName)
{
    QString year = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters,
                                                                    "year", L4C_AP_CFG_PREFIX);
    if (year.size() == 0) {
        year = S4CUtils::GetSiteYearFromDisk(parameters, configParameters, siteShortName, "config",
                                                     L4C_AP_CFG_PREFIX, "cfg_dir");
    }
    return year;
}
