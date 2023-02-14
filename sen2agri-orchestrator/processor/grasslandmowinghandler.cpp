#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "grasslandmowinghandler.hpp"
#include "processorhandlerhelper.h"
#include "json_conversions.hpp"
#include "logger.hpp"
#include "s4c_utils.hpp"

#include "products/generichighlevelproducthelper.h"
using namespace orchestrator::products;

using namespace grassland_mowing;

#define L4B_GM_DEF_CFG_DIR   "/mnt/archive/grassland_mowing_files/{site}/{year}/config/"

void GrasslandMowingHandler::CreateTasks(GrasslandMowingExecConfig &cfg,
                                         QList<TaskToSubmit> &outAllTasksList)
{
    int curTaskIdx = 0;
    // Create task for creating the input shapefile
    outAllTasksList.append(TaskToSubmit{ "s4c-grassland-gen-input-shp", {} });
    curTaskIdx++;

    // Create task for creating the input shapefile
    outAllTasksList.append(TaskToSubmit{ "s4c-grassland-extract-products", {} });
    curTaskIdx++;

    QList<int> prdFormatterParentTasks;
    if (cfg.inputPrdsType & L3B) {
        prdFormatterParentTasks.append(curTaskIdx++);
        outAllTasksList.append(TaskToSubmit{ "s4c-grassland-mowing", {outAllTasksList[0], outAllTasksList[1]} });
    }
    if (cfg.inputPrdsType & L2_S1) {
        // if we have also the S2, then put this task to be executed after the previous one
        prdFormatterParentTasks.append(curTaskIdx++);
        outAllTasksList.append(TaskToSubmit{ "s4c-grassland-mowing",
                         {((cfg.inputPrdsType & L3B) ? outAllTasksList[2] : outAllTasksList[0]), outAllTasksList[1]} });
    }

    int productFormatterIdx = curTaskIdx++;
    outAllTasksList.append(TaskToSubmit{ "product-formatter", {} });
    // product formatter needs completion of time-series-analisys tasks
    for (const auto &curIdx : prdFormatterParentTasks) {
        outAllTasksList[productFormatterIdx].parentTasks.append(outAllTasksList[curIdx]);
    }
}

void GrasslandMowingHandler::CreateSteps(GrasslandMowingExecConfig &cfg, QList<TaskToSubmit> &allTasksList,
                                        NewStepList &steps)
{
    int curTaskIdx = 0;
    QStringList productFormatterFiles;

    TaskToSubmit &genInputShpTask = allTasksList[curTaskIdx++];

    const QString &inputShpLocation = genInputShpTask.GetFilePath("SEN4CAP_L4B_GeneratedInputShp.shp");
    const QStringList &inShpGenArgs = GetInputShpGeneratorArgs(cfg, inputShpLocation);
    steps.append(CreateTaskStep(genInputShpTask, "MowingInputShpGenerator", inShpGenArgs));

    TaskToSubmit &exportPrdsTask = allTasksList[curTaskIdx++];
    const QString &l3bPrdsFile = exportPrdsTask.GetFilePath("l3b_products.csv");
    const QString &s1PrdsFile = exportPrdsTask.GetFilePath("s1_products.csv");
    const QStringList &exportPrdsArgs = GetExportProductsArgs(cfg, l3bPrdsFile, s1PrdsFile);
    steps.append(CreateTaskStep(exportPrdsTask, "ExportProducts", exportPrdsArgs));

    // It is assumed that the product formatter task it is the last one in the list
    TaskToSubmit &productFormatterTask = allTasksList[allTasksList.size()-1];

    QString s1InputShpLocation = inputShpLocation;
    if (cfg.inputPrdsType & L3B) {
        QString outShpFileName = ((cfg.inputPrdsType & L2_S1) ?
                                      "SEN4CAP_L4B_S1_S2_MowingDetection" :
                                      "SEN4CAP_L4B_S2_MowingDetection");
        TaskToSubmit &s2MowingDetectionTask = allTasksList[curTaskIdx++];
        const QString &s2MowingDetectionOutFile = productFormatterTask.GetFilePath(outShpFileName + ".shp");
        const QString &s2OutDir = s2MowingDetectionTask.GetFilePath("SEN4CAP_L4B_S2_OutputWorkingData");
        const QStringList &s2MowingDetectionArgs = GetMowingDetectionArgs(cfg, L3B, inputShpLocation, l3bPrdsFile,
                                                                          s2OutDir, s2MowingDetectionOutFile);
        steps.append(CreateTaskStep(s2MowingDetectionTask, "S2MowingDetection", s2MowingDetectionArgs));

        productFormatterFiles += s2MowingDetectionOutFile;
        // add also the dbf, prj and shx files
        productFormatterFiles += productFormatterTask.GetFilePath(outShpFileName + ".dbf");
        productFormatterFiles += productFormatterTask.GetFilePath(outShpFileName + ".prj");
        productFormatterFiles += productFormatterTask.GetFilePath(outShpFileName + ".shx");
        productFormatterFiles += productFormatterTask.GetFilePath(outShpFileName + ".cpg");
        // Add also the intermediate files
        // TODO: see if this path should be better configured in database as
        // the script might use it to detect if vrt and other files were already generated
        // by a previous execution
        if (!cfg.isScheduled) {
            productFormatterFiles += s2MowingDetectionTask.GetFilePath("");
        }
        // if both S2 and S1, set the input for the S1 as the output of S2
        s1InputShpLocation = s2MowingDetectionOutFile;
    }

    if (cfg.inputPrdsType & L2_S1) {
        QString outShpFileName = ((cfg.inputPrdsType & L3B) ?
                                      "SEN4CAP_L4B_S1_S2_MowingDetection" :
                                      "SEN4CAP_L4B_S1_MowingDetection");
        TaskToSubmit &s1MowingDetectionTask = allTasksList[curTaskIdx++];
        const QString &s1MowingDetectionOutFile = productFormatterTask.GetFilePath(outShpFileName + ".shp");
        const QString &s1OutDir = s1MowingDetectionTask.GetFilePath("SEN4CAP_L4B_S1_OutputWorkingData");
        const QStringList &s1MowingDetectionArgs = GetMowingDetectionArgs(cfg, L2_S1, s1InputShpLocation, s1PrdsFile,
                                                                          s1OutDir, s1MowingDetectionOutFile);
        steps.append(CreateTaskStep(s1MowingDetectionTask, "S1MowingDetection", s1MowingDetectionArgs));

        productFormatterFiles += s1MowingDetectionOutFile;
        // add also the dbf, prj and shx files
        productFormatterFiles += s1MowingDetectionTask.GetFilePath(outShpFileName + ".dbf");
        productFormatterFiles += s1MowingDetectionTask.GetFilePath(outShpFileName + ".prj");
        productFormatterFiles += s1MowingDetectionTask.GetFilePath(outShpFileName + ".shx");
        productFormatterFiles += s1MowingDetectionTask.GetFilePath(outShpFileName + ".cpg");

        // Add also the intermediate files
        // TODO: see if this path should be better configured in database as
        // the script might use it to detect if vrt and other files were already generated
        // by a previous execution
        if (!cfg.isScheduled) {
            productFormatterFiles += s1MowingDetectionTask.GetFilePath("");
        }
    }

    const QStringList &productFormatterArgs = GetProductFormatterArgs(productFormatterTask, cfg, productFormatterFiles);
    steps.append(CreateTaskStep(productFormatterTask, "ProductFormatter", productFormatterArgs));
}

bool GrasslandMowingHandler::CheckInputParameters(GrasslandMowingExecConfig &cfg, QString &err) {
    QString strStartDate, strEndDate;
    if(IsScheduledJobRequest(cfg.parameters)) {
        if (ProcessorHandlerHelper::GetParameterValueAsString(cfg.parameters, "start_date", strStartDate) &&
            ProcessorHandlerHelper::GetParameterValueAsString(cfg.parameters, "end_date", strEndDate) &&
            cfg.parameters.contains("input_products") && cfg.parameters["input_products"].toArray().size() == 0) {
            cfg.isScheduled = true;
            cfg.startDate = ProcessorHandlerHelper::GetDateTimeFromString(strStartDate);
            cfg.endDate = ProcessorHandlerHelper::GetDateTimeFromString(strEndDate);

            QString strSeasonStartDate, strSeasonEndDate;
            ProcessorHandlerHelper::GetParameterValueAsString(cfg.parameters, "season_start_date", strSeasonStartDate);
            ProcessorHandlerHelper::GetParameterValueAsString(cfg.parameters, "season_end_date", strSeasonEndDate);
            cfg.seasonStartDate = ProcessorHandlerHelper::GetDateTimeFromString(strSeasonStartDate);
            cfg.seasonEndDate = ProcessorHandlerHelper::GetDateTimeFromString(strSeasonEndDate);

            QString startDateOverride;
            bool found = ProcessorHandlerHelper::GetParameterValueAsString(
                        cfg.parameters, "mowing-start-date", startDateOverride);
            if (found && startDateOverride.size() > 0) {
                cfg.startDate = ProcessorHandlerHelper::GetDateTimeFromString(startDateOverride);
            }
        } else {
            err = "Invalid scheduled request. Start date, end date or request structure are invalid!";
            return false;
        }
    } else {
        cfg.isScheduled = false;
        const QString &startDateStr = ProcessorHandlerHelper::GetStringConfigValue(cfg.parameters, cfg.configParameters,
                                                                                   "start_date", L4B_GM_CFG_PREFIX);
        const QString &endDateStr = ProcessorHandlerHelper::GetStringConfigValue(cfg.parameters, cfg.configParameters,
                                                                                   "end_date", L4B_GM_CFG_PREFIX);
        cfg.startDate = ProcessorHandlerHelper::GetDateTimeFromString(startDateStr);
        cfg.endDate = ProcessorHandlerHelper::GetDateTimeFromString(endDateStr);

        // Custom request
        const QStringList &arrPrdsL3B = GetInputProductNames(cfg.parameters, ProductType::L3BProductTypeId);
        const QStringList &arrPrdsAmp = GetInputProductNames(cfg.parameters, ProductType::S4CS1L2AmpProductTypeId);
        const QStringList &arrPrdsCohe = GetInputProductNames(cfg.parameters, ProductType::S4CS1L2CoheProductTypeId);
        QDateTime startDate, endDate;
        UpdatePrdInfos(cfg, arrPrdsL3B, cfg.l3bPrds, startDate, endDate);
        UpdatePrdInfos(cfg, arrPrdsAmp, cfg.s1Prds, startDate, endDate);
        UpdatePrdInfos(cfg, arrPrdsCohe, cfg.s1Prds, startDate, endDate);
        if (!cfg.startDate.isValid()) {
            cfg.startDate = startDate;
        }
        if (!cfg.endDate.isValid()) {
            cfg.endDate = endDate;
        }

        // if we have no l3b products then we set the inputPrdsType in the configuration
        // to S1 type in order to avoid creation of L3B mowing detection task and steps
        if (cfg.l3bPrds.size() == 0) {
            cfg.inputPrdsType = L2_S1;
        }
        // if we have no S1 products then we set the inputPrdsType in the configuration
        // to L3B type in order to avoid creation of S1 mowing detection task and steps
        if (cfg.s1Prds.size() == 0) {
            if (cfg.inputPrdsType == L2_S1) {
                err = "Invalid custom request. No products were provided !";
                return false;
            } else {
                cfg.inputPrdsType = L3B;
            }
        }
    }
    return LoadConfigFileAdditionalValues(cfg, err);
}

void GrasslandMowingHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                              const JobSubmittedEvent &event)
{
    QString err;
    GrasslandMowingExecConfig cfg(&ctx, event.siteId, event.jobId, event.parametersJson);

    if (!CheckInputParameters(cfg, err)) {
        ctx.MarkJobFailed(event.jobId);
        throw std::runtime_error(
            QStringLiteral("Error producing S4C_L4B product for site %1. The error was %2!\n").
                    arg(cfg.siteShortName).arg(err).toStdString());
    }

    QList<TaskToSubmit> allTasksList;
    CreateTasks(cfg, allTasksList);

    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    for(TaskToSubmit &task: allTasksList) {
        allTasksListRef.append(task);
    }
    SubmitTasks(ctx, event.jobId, allTasksListRef);
    NewStepList allSteps;
    CreateSteps(cfg, allTasksList, allSteps);
    ctx.SubmitSteps(allSteps);
}


void GrasslandMowingHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                              const TaskFinishedEvent &event)
{
    if (event.module == "product-formatter") {
        ctx.MarkJobFinished(event.jobId);

        const QString &prodName = GetOutputProductName(ctx, event);
        const QString &productFolder = GetFinalProductFolder(ctx, event.jobId, event.siteId) + "/" + prodName;
        if(prodName != "") {
            const QString &quicklook = GetProductFormatterQuicklook(ctx, event);
            const QString &footPrint = GetProductFormatterFootprint(ctx, event);
            // Insert the product into the database
            GenericHighLevelProductHelper prdHelper(productFolder);
            ctx.InsertProduct({ ProductType::S4CL4BProductTypeId, event.processorId, event.siteId,
                                event.jobId, productFolder, prdHelper.GetAcqDate(),
                                prodName, quicklook, footPrint, std::experimental::nullopt, TileIdList(), ProductIdsList() });

            // Now remove the job folder containing temporary files
            RemoveJobFolder(ctx, event.jobId, processorDescr.shortName);
        } else {
            Logger::error(QStringLiteral("Cannot insert into database the product with name %1 and folder %2").arg(prodName).arg(productFolder));
        }
    }
}

QStringList GrasslandMowingHandler::GetProductFormatterArgs(TaskToSubmit &productFormatterTask,
                                                            GrasslandMowingExecConfig &cfg, const QStringList &listFiles) {
    QString strTimePeriod = cfg.startDate.toString("yyyyMMddTHHmmss").append("_").append(cfg.endDate.toString("yyyyMMddTHHmmss"));
    QStringList additionalArgs = {"-processor.generic.files"};
    additionalArgs += listFiles;
    return GetDefaultProductFormatterArgs(*(cfg.pCtx), productFormatterTask, cfg.jobId,
                                                     cfg.siteId, "S4C_L4B", strTimePeriod,
                                                     "generic", additionalArgs, true);
}

ProcessorJobDefinitionParams GrasslandMowingHandler::GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
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

    // we need at least 30 days for launching a job otherwise it will give errors
    if (qScheduledDate < seasonStartDate.addDays(30)) {
        params.schedulingFlags = SchedulingFlags::SCH_FLG_NOOP_AND_SCHEDULE_NEXT;
        params.isValid = true;
        return params;
    }

    ConfigurationParameterValueMap mapCfg = ctx.GetConfigurationParameters(QString(L4B_GM_CFG_PREFIX), siteId, requestOverrideCfgValues);
    std::map<QString, QString> configParams;
    for (const auto &p : mapCfg) {
        configParams.emplace(p.key, p.value);
    }

    // we might have an offset in days from starting the downloading products to start the S4C_L4B production
    int startSeasonOffset = mapCfg["processor.s4c_l4b.start_season_offset"].value.toInt();
    QDateTime startDate = seasonStartDate.addDays(startSeasonOffset);

    // Get the start and end date for the production
    QDateTime endDate = qScheduledDate;
    QJsonObject parameters;
    const QString &startDateStr = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParams,
                                                                               "start_date", L4B_GM_CFG_PREFIX);
    QDateTime tempDt = ProcessorHandlerHelper::GetDateTimeFromString(startDateStr);
    if (tempDt.isValid()) {
        startDate = tempDt;
    }

    params.jsonParameters.append("{ \"scheduled_job\": \"1\", \"start_date\": \"" + startDate.toString("yyyyMMdd") + "\", " +
                                 "\"end_date\": \"" + endDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_start_date\": \"" + seasonStartDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_end_date\": \"" + seasonEndDate.toString("yyyyMMdd") + "\"");

    InputProductsType prdType = all;
    if(requestOverrideCfgValues.contains("product_type")) {
        const ConfigurationParameterValue &productType = requestOverrideCfgValues["product_type"];
        const QString &trimmedPrdTypeVal = productType.value.trimmed();
        params.jsonParameters.append(", \"input_product_types\": \"" + trimmedPrdTypeVal + "\"}");
        prdType = GrasslandMowingExecConfig::GetInputProductsType(trimmedPrdTypeVal);
    } else {
        params.jsonParameters.append("}");
    }

    if ((prdType & L3B) != 0) {
        if (!CheckAllAncestorProductCreation(ctx, siteId, ProductType::L3BProductTypeId, startDate, endDate)) {
            // do not trigger yet the schedule.
            params.schedulingFlags = SchedulingFlags::SCH_FLG_RETRY_LATER;
            Logger::debug(QStringLiteral("Executing S4C_L4B scheduled job. Not all L3B products "
                                         "were processed for site ID %1 within interval [ %2 and %3]! "
                                         "The schedule for this date will be retried later ...")
                          .arg(siteId).arg(startDate.toString()).arg(endDate.toString()));
        }
    }
    if ((prdType & L2_S1) != 0) {
        // we might have at the begining of season some COHE products that do not have previous acq and in that
        // case they remain forever with status 2
        QDateTime s1Start = seasonStartDate.addDays(15);
        if (!CheckAllAncestorProductCreation(ctx, siteId, ProductType::S4CS1L2AmpProductTypeId, s1Start, endDate) ||
            !CheckAllAncestorProductCreation(ctx, siteId, ProductType::S4CS1L2CoheProductTypeId, s1Start, endDate)) {
            // do not trigger yet the schedule.
            params.schedulingFlags = SchedulingFlags::SCH_FLG_RETRY_LATER;
        }
    }

    params.isValid = true;

    return params;
}

QStringList GrasslandMowingHandler::GetInputShpGeneratorArgs(GrasslandMowingExecConfig &cfg,
                                                             const QString &outShpFile)
{
    QStringList retArgs =  {"--site-id", QString::number(cfg.siteId),
            "--year", cfg.year,
            "--path", outShpFile};
    if (cfg.ctNumFilter.size() > 0) {
        retArgs += "--filter-ctnum";
        retArgs += cfg.ctNumFilter;
    }

    if (cfg.additionalCols.size() > 0) {
        retArgs += "--add-decl-cols";
        retArgs += cfg.additionalCols;
    }

    return retArgs;
}

QStringList GrasslandMowingHandler::GetExportProductsArgs(GrasslandMowingExecConfig &cfg, const QString &l3bPrdsFile, const QString &s1PrdsFile)
{
    //-s 22 --season-start 2020-04-01 --season-end 2020-10-30 --out-s1-products-file ./s1_files.txt --out-l3b-products-file ./l3b_files.txt

    QStringList retArgs =  {"--site-id",
                            QString::number(cfg.siteId)
                           };
    if (cfg.inputPrdsType & L3B) {
        retArgs += "--out-l3b-products-file";
        retArgs += l3bPrdsFile;
    }
    if (cfg.inputPrdsType & L2_S1) {
        retArgs += "--out-s1-products-file";
        retArgs += s1PrdsFile;
    }

    if (cfg.isScheduled) {
        retArgs += "--season-start";
        retArgs += cfg.seasonStartDate.toString("yyyy-MM-dd");
        retArgs += "--season-end";
        retArgs += cfg.seasonEndDate.toString("yyyy-MM-dd");
    } else {
        if (cfg.inputPrdsType & L3B) {
            retArgs += "--l3b-products";
            retArgs += cfg.l3bPrds;
        }
        if (cfg.inputPrdsType & L2_S1) {
            retArgs += "--s1-products";
            retArgs += cfg.s1Prds;
        }
    }

    return retArgs;
}


QStringList GrasslandMowingHandler::GetMowingDetectionArgs(GrasslandMowingExecConfig &cfg, const InputProductsType &prdType,
                                                           const QString &inputShpLocation, const QString &inputPrdsFile,
                                                           const QString &outDataDir, const QString &outFile)
{
    QString keyScript("s2_py_script");
    QString inputPrdsFilePrefix("--l3b-products-file");
    if (prdType == L2_S1) {
        keyScript = "s1_py_script";
        inputPrdsFilePrefix = "--s1-products-file";
    }

    const QString &scriptToInvoke = ProcessorHandlerHelper::GetStringConfigValue(cfg.parameters, cfg.configParameters,
                                                                                 keyScript, L4B_GM_CFG_PREFIX);
    QString segParcelIdAttrName = ProcessorHandlerHelper::GetStringConfigValue(cfg.parameters, cfg.configParameters,
                                                                                 "seg-parcel-id-attribute", L4B_GM_CFG_PREFIX);
    if (segParcelIdAttrName.size() == 0) {
        segParcelIdAttrName = "NewID";
    }
    QDateTime startDate = cfg.startDate;
    int s1s2StartDateDiff = ProcessorHandlerHelper::GetIntConfigValue(cfg.parameters, cfg.configParameters,
                                                                                 "s1_s2_startdate_diff", L4B_GM_CFG_PREFIX);
    if ((prdType == L2_S1) && (s1s2StartDateDiff != 0)) {
        Logger::debug(QStringLiteral("S4C_L4B : Using an offset of %1 days for the S1 start date that is initially %2...")
                      .arg(s1s2StartDateDiff).arg(startDate.toString()));
        startDate = startDate.addDays(s1s2StartDateDiff);
    }

    QStringList retArgs = {
                            /*"--script-path", */ scriptToInvoke,
                            inputPrdsFilePrefix, inputPrdsFile,
                            "--config-file", cfg.l4bCfgFile,
                            "--input-shape-file", inputShpLocation,
                            "--output-data-dir", outDataDir,
                            "--start-date", startDate.toString("yyyy-MM-dd"),
                            "--end-date", cfg.endDate.toString("yyyy-MM-dd"),
                            "--seg-parcel-id-attribute", segParcelIdAttrName,
                            "--output-shapefile", outFile,
                            "--do-cmpl", "True",
                            "--test", "True"
                      };

    return retArgs;
}

bool GrasslandMowingHandler::IsScheduledJobRequest(const QJsonObject &parameters) {
    int jobVal;
    return ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "scheduled_job", jobVal) && (jobVal == 1);
}

void GrasslandMowingHandler::UpdatePrdInfos(GrasslandMowingExecConfig &cfg,
                                            const QStringList &prdNames, QStringList &whereToAdd,
                                            QDateTime &startDate, QDateTime &endDate)
{
    GenericHighLevelProductHelper prdHelper;
    for (const auto &prd: prdNames) {
        const QString &prdPath = cfg.pCtx->GetProductAbsolutePath(cfg.siteId, prd);
        prdHelper.SetProduct(prdPath);
        if (prdHelper.IsValid()) {
            ProcessorHandlerHelper::UpdateMinMaxTimes(prdHelper.GetAcqDate(), startDate, endDate);
        }
        whereToAdd.append(prdPath);
    }
}


QString GrasslandMowingHandler::GetProcessorDirValue(GrasslandMowingExecConfig &cfg,
                                                    const QString &key, const QString &defVal ) {
    QString dataExtrDirName = ProcessorHandlerHelper::GetStringConfigValue(cfg.parameters, cfg.configParameters,
                                                                           key, L4B_GM_CFG_PREFIX);

    if (dataExtrDirName.size() == 0) {
        dataExtrDirName = defVal;
    }
    dataExtrDirName = dataExtrDirName.replace("{site}", cfg.siteShortName);
    dataExtrDirName = dataExtrDirName.replace("{year}", cfg.year);
    dataExtrDirName = dataExtrDirName.replace("{processor}", processorDescr.shortName);

    return dataExtrDirName;
}

QString GrasslandMowingHandler::GetL4BConfigFilePath(GrasslandMowingExecConfig &jobCfg)
{
    QString strCfgPath;
    const QString &strCfgDir = GetProcessorDirValue(jobCfg, "cfg_dir", L4B_GM_DEF_CFG_DIR);
    QDir directory(strCfgDir);
    QString preferedCfgFileName = "S4C_L4B_Config_" + jobCfg.year + ".cfg";
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
                                                                                   "default_config_path", L4B_GM_CFG_PREFIX);
    }

    if (strCfgPath.isEmpty() || strCfgPath == "N/A" || !QFileInfo::exists(strCfgPath)) {
        return "";
    }
    return strCfgPath;
}

bool GrasslandMowingHandler::LoadConfigFileAdditionalValues(GrasslandMowingExecConfig &cfg, QString &err)
{
    cfg.l4bCfgFile = GetL4BConfigFilePath(cfg);
    if (cfg.l4bCfgFile == "") {
        err = "Cannot get L4B configuration file for site with short name " + cfg.siteShortName;
        return false;
    }

    Logger::info(QStringLiteral("S4C_L4B: Loading settings from file %1 ").arg(cfg.l4bCfgFile));
    QSettings settings(cfg.l4bCfgFile, QSettings::IniFormat);

    QString cmnSectionKey("GENERAL_CONFIG/");
    cfg.ctNumFilter = GetStringValue(settings, cmnSectionKey + "CTNUM_FILTER");
    cfg.additionalCols = GetStringValue(settings, cmnSectionKey + "ADDITIONAL_COLUMNS");

    return true;
}

QString GrasslandMowingHandler::GetStringValue(const QSettings &settings, const QString &key)
{
    QVariant value = settings.value(key);
    QString string;
    if (value.type() == QVariant::StringList) {
      string = value.toStringList().join(",");
    } else {
      string = value.toString();
    }
    return string;
}

// ###################### GrasslandMowingExecConfig functions ############################
InputProductsType GrasslandMowingExecConfig::GetInputProductsType(const QString &str)
{
    const QString &trimmedStr = str.trimmed();
    if (QString::compare(trimmedStr, "S1", Qt::CaseInsensitive) == 0) {
        return L2_S1;
    } else if (QString::compare(trimmedStr, "S2", Qt::CaseInsensitive) == 0) {
        return L3B;
    }
    return all;
}

InputProductsType GrasslandMowingExecConfig::GetInputProductsType(const QJsonObject &parameters, const std::map<QString, QString> &configParameters)
{
    const QString &inPrdsType = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters,
                                                                             "input_product_types", L4B_GM_CFG_PREFIX);
    return GetInputProductsType(inPrdsType);
}

