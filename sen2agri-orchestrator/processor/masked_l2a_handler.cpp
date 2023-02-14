#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "masked_l2a_handler.hpp"
#include "processorhandlerhelper.h"
#include "json_conversions.hpp"
#include "logger.hpp"
#include "s4c_utils.hpp"
#include <unordered_map>

#include "products/producthelperfactory.h"
using namespace orchestrator::products;

// For unordered map and QString as key
namespace std {
  template<> struct hash<QString> {
    std::size_t operator()(const QString& s) const noexcept {
      return (size_t) qHash(s);
    }
  };
}

static QList<int> gOutRes = {-1, 10, 20};

MaskedL2AHandler::MaskedL2AHandler()
{
}

void MaskedL2AHandler::CreateTasksAndSteps(EventProcessingContext &ctx, const JobSubmittedEvent &evt,
                                 const QList<InputPrdInfo> &prdInfos) {
    QList<TaskToSubmit> allTasksList;
    QList<std::reference_wrapper<const TaskToSubmit>> endOfJobParentsRefs;
    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    int cnt = 0;
    for (cnt = 0; cnt<prdInfos.size(); cnt++) {
        allTasksList.append(TaskToSubmit{ "validity-mask-extractor", {} });
        endOfJobParentsRefs.append(allTasksList[cnt]);
        allTasksListRef.append(allTasksList[cnt]);
    }
    allTasksList.append({"end-of-job", {}});
    allTasksList[cnt].parentTasks.append(endOfJobParentsRefs);
    allTasksListRef.append(allTasksList[cnt]);

    SubmitTasks(ctx, evt.jobId, allTasksListRef);

    const auto &parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
    auto cfgParams = ctx.GetConfigurationParameters(MASKED_L2A_CFG_PREFIX, evt.siteId);
    bool compressOutputs = ProcessorHandlerHelper::GetBoolConfigValue(parameters, cfgParams, "compress", MASKED_L2A_CFG_PREFIX);
    bool cogOutputs = ProcessorHandlerHelper::GetBoolConfigValue(parameters, cfgParams, "cog", MASKED_L2A_CFG_PREFIX);
    bool continueOnMissingPrd = ProcessorHandlerHelper::GetBoolConfigValue(parameters, cfgParams, "continue-on-missing-input", MASKED_L2A_CFG_PREFIX);

    NewStepList allSteps;

    cnt = 0;
    for(const InputPrdInfo &prdInfo: prdInfos) {
        TaskToSubmit &curTask = allTasksList[cnt++];
        CreateValidityMaskExtractorStep(ctx, evt, prdInfo, curTask, allSteps, compressOutputs, cogOutputs, continueOnMissingPrd);
    }
    // create the end of all steps marker
    allSteps.append(CreateTaskStep(allTasksList[allTasksList.size()-1], "EndOfJob", QStringList()));
    ctx.SubmitSteps(allSteps);
}

QList<MaskedL2AHandler::InputPrdInfo>
MaskedL2AHandler::GetInputProductsToProcess(EventProcessingContext &ctx,
                                                          const JobSubmittedEvent &evt,
                                                          const QJsonObject &parameters,
                                                          const ProductList &l2aPrdsList)
{
    QList<InputPrdInfo> retList;
    auto fmaskCfgParams = ctx.GetConfigurationParameters("processor.fmask.", evt.siteId);
    bool fmaskEnabled = ProcessorHandlerHelper::GetBoolConfigValue(parameters, fmaskCfgParams, "processor.fmask.enabled", "");
    Logger::info(QStringLiteral("MaskedL2A: found a number %1 of L2A products to process").
                 arg(l2aPrdsList.size()));

    if (fmaskEnabled) {
        // extract the downloader history ids for these products
        ProductIdsList dwnHistIds;
        std::unordered_map<int, ProductList> mapL2APrds;
        std::for_each(l2aPrdsList.begin(), l2aPrdsList.end(), [&dwnHistIds, &mapL2APrds](const Product &prd) {
            dwnHistIds.append(prd.downloaderHistoryId);
            mapL2APrds[prd.downloaderHistoryId].push_back(prd);
        });

        int maskValidVal = ProcessorHandlerHelper::GetIntConfigValue(parameters, fmaskCfgParams, "processor.fmask.valid_value", "");
        // Get  the FMask products that could correspond to the same L1C product
        const ProductList &fmaskPrds = ctx.GetL1DerivedProducts(evt.siteId, ProductType::FMaskProductTypeId, dwnHistIds);
        // iterate the fmask products and find the matching L2A
        std::for_each(fmaskPrds.begin(), fmaskPrds.end(), [&mapL2APrds, &ctx, &retList, maskValidVal, this](const Product &fmaskPrd) {
            std::unordered_map<int, ProductList>::const_iterator iter = mapL2APrds.find (fmaskPrd.downloaderHistoryId);
            if (iter != mapL2APrds.end()) {
                const ProductList &l2aPrds = iter->second;
                for (const Product& l2aPrd : l2aPrds) {
                    retList.push_back({l2aPrd, fmaskPrd, maskValidVal});
                    Logger::info(QStringLiteral("MaskedL2A: found pair L2A = %1, FMask=%2").
                                 arg(l2aPrd.name).arg(fmaskPrd.name));
                }
            }
        });
    } else {
        // otherwise just add the l2a product full paths
        std::for_each(l2aPrdsList.begin(), l2aPrdsList.end(), [&retList, this](const Product &l2aPrd) {
            retList.push_back({l2aPrd, Product(), 0});
        });
    }
    return retList;

}

void MaskedL2AHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                              const JobSubmittedEvent &evt)
{
    auto parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();

    // if a scheduled job, we are extracting all the not processed products
    ProductList l2aPrdsList;
    int ret = GetProductsFromSchedReq(ctx, evt, parameters, l2aPrdsList);
    // no products available from the scheduling ... mark also the job as failed
    // TODO: Maybe we should somehow delete completely the job
    if (ret == 0) {
        ctx.MarkJobFailed(evt.jobId);
        throw std::runtime_error(
                    QStringLiteral("L3B Scheduled job with id %1 for site %2 marked as done as no products are available for now to process").
                                         arg(evt.jobId).arg(evt.siteId).toStdString());
    } else if (ret == -1) {
        // custom job
        const QStringList &prdNames = GetInputProductNames(parameters);
        l2aPrdsList = ctx.GetProducts(evt.siteId, prdNames);
    }
    const QList<InputPrdInfo> &prdsToProcess = GetInputProductsToProcess(ctx, evt, parameters, l2aPrdsList);
    Logger::info(QStringLiteral("MaskedL2A: found a number %1 of products/pairs to process").
                 arg(prdsToProcess.size()));

    // filter the already processed or processing products
    const QList<InputPrdInfo> &filteredPrds = FilterInputProducts(ctx, evt.siteId, evt.jobId, prdsToProcess);
    Logger::info(QStringLiteral("MaskedL2A: A number %1 of products will be processed after filtering current running ones").
                 arg(filteredPrds.size()));

    CreateTasksAndSteps(ctx, evt, filteredPrds);
}

void MaskedL2AHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                              const TaskFinishedEvent &event)
{
    if (event.module == "validity-mask-extractor") {
        CreateMaskedL2AProduct(ctx, event);
    } else if (event.module == "end-of-job") {
        ctx.MarkJobFinished(event.jobId);
        // Now remove the job folder containing temporary files
        RemoveJobFolder(ctx, event.jobId, processorDescr.shortName);
    }
}

int MaskedL2AHandler::GetProductsFromSchedReq(EventProcessingContext &ctx,
                                                          const JobSubmittedEvent &event, QJsonObject &parameters,
                                                          ProductList &outPrdsList) {
    int jobVal;
    QString strStartDate, strEndDate;
    if(ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "scheduled_job", jobVal) && (jobVal == 1) &&
        ProcessorHandlerHelper::GetParameterValueAsString(parameters, "start_date", strStartDate) &&
        ProcessorHandlerHelper::GetParameterValueAsString(parameters, "end_date", strEndDate)) {
        const auto &startDate = ProcessorHandlerHelper::GetLocalDateTime(strStartDate);
        const auto &endDate = ProcessorHandlerHelper::GetLocalDateTime(strEndDate);

        Logger::info(QStringLiteral("MaskedL2A Scheduled job received for siteId = %1, startDate=%2, endDate=%3").
                     arg(event.siteId).arg(startDate.toString("yyyyMMddTHHmmss")).arg(endDate.toString("yyyyMMddTHHmmss")));
        outPrdsList = ctx.GetParentProductsNotInProvenance(event.siteId, {ProductType::L2AProductTypeId}, ProductType::MaskedL2AProductTypeId, startDate, endDate);
        return outPrdsList.size();
    }
    return -1;
}



ProcessorJobDefinitionParams MaskedL2AHandler::GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
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

    ConfigurationParameterValueMap mapCfg = ctx.GetConfigurationParameters(QString(MASKED_L2A_CFG_PREFIX),
                                                                           siteId, requestOverrideCfgValues);
    std::map<QString, QString> configParams;
    for (const auto &p : mapCfg) {
        configParams.emplace(p.key, p.value);
    }

    QDateTime minScheduleDate = seasonStartDate.addDays(14);
    // if scheduled date is less than the minimum scheduling date, return invalid to pass to the next date
    if (qScheduledDate >= minScheduleDate) {
        // check if the ancestor products were created
        if (!CheckAllAncestorProductCreation(ctx, siteId, ProductType::MaskedL2AProductTypeId, seasonStartDate, qScheduledDate) ||
             (qScheduledDate > seasonEndDate.addDays(1))) {
            // do not trigger anymore the schedule.
            params.schedulingFlags = SchedulingFlags::SCH_FLG_RETRY_LATER;
            Logger::error("Masked_L2A Scheduled job execution will be retried later: Not all input products were yet produced");
        }
    }

    params.jsonParameters.append("{ \"scheduled_job\": \"1\", \"start_date\": \"" + seasonStartDate.toString("yyyyMMdd") + "\", " +
                                 "\"end_date\": \"" + qScheduledDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_start_date\": \"" + seasonStartDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_end_date\": \"" + seasonEndDate.toString("yyyyMMdd") + "\"}");

    params.isValid = true;

    return params;
}

void MaskedL2AHandler::HandleProductAvailableImpl(EventProcessingContext &ctx,
                                const ProductAvailableEvent &event)
{
    // Get the product description from the database
    const ProductList &prds = ctx.GetProducts({event.productId});
    if (prds.size() == 0) {
        Logger::error(QStringLiteral("MaskedL2AHandler - HandleProductAvailable - Event received for product with %1 but no such product in the database").arg(event.productId));
        return;
    }
    const Product &prd = prds.back();
    if (prd.productTypeId != ProductType::L2AProductTypeId &&
            prd.productTypeId != ProductType::FMaskProductTypeId) {
        return;
    }

    QJsonObject parameters;
    auto cfgParams = ctx.GetConfigurationParameters(MASKED_L2A_CFG_PREFIX, prd.siteId);
    bool processorEnabled = ProcessorHandlerHelper::GetBoolConfigValue(parameters, cfgParams, "enabled", MASKED_L2A_CFG_PREFIX);
    if (!processorEnabled) {
        return;
    }

    bool checkPair = true;
    Product l2aPrd = prd;
    if (prd.productTypeId == ProductType::L2AProductTypeId) {
        // check if we have the FMask enabled for this site, otherwise, just send the job
        auto fmaskCfgParams = ctx.GetConfigurationParameters("processor.fmask.enabled", prd.siteId);
        bool fmaskEnabled = ProcessorHandlerHelper::GetBoolConfigValue(parameters, fmaskCfgParams, "enabled", "processor.fmask.");
        if (!fmaskEnabled) {
            checkPair = false;
        }
    }
    if (checkPair) {
        const ProductIdToDwnHistIdMap &map = ctx.GetDownloaderHistoryIds({event.productId});
        if (map.size() == 0) {
            Logger::error(QStringLiteral("MaskedL2AHandler - HandleProductAvailable - Could not retrieve the downloader_history_id for product %1").arg(prd.fullPath));
            return;
        }
        Product pairedPrd;
        ProductType pairedPrdType = (prd.productTypeId == ProductType::L2AProductTypeId) ?
                    ProductType::FMaskProductTypeId : ProductType::L2AProductTypeId;
        // Create the job and send the L2A only if there are both products (FMask and L2A)
        if (!GetPairedProduct(ctx, prd.siteId, pairedPrdType, map.value(event.productId), pairedPrd)) {
            // No Job to create
            Logger::error(QStringLiteral("MaskedL2AHandler - HandleProductAvailable - No pair for product %1").arg(prd.fullPath));
            return;
        }
        // Update the L2A product
        if (prd.productTypeId == ProductType::FMaskProductTypeId) {
            l2aPrd = pairedPrd;
        }
    }

    // Create a new JOB
    NewJob newJob;
    newJob.processorId = processorDescr.processorId;  //send the job to this processor
    newJob.siteId = prd.siteId;
    newJob.startType = JobStartType::Triggered;

    QJsonObject processorParamsObj;
    QJsonArray prodsJsonArray;
    prodsJsonArray.append(l2aPrd.name);

    const QString &prdKey = "input_products";
    processorParamsObj[prdKey] = prodsJsonArray;
    newJob.parametersJson = jsonToString(processorParamsObj);
    ctx.SubmitJob(newJob);
    Logger::info(QStringLiteral("MaskedL2AHandler - HandleProductAvailable - Submitted trigger job "
                                "for product %1 (triggered by %2) and siteid = %3").arg(l2aPrd.fullPath)
                 .arg(prd.fullPath)
                 .arg(QString::number((int)prd.siteId)));
}

bool MaskedL2AHandler::GetPairedProduct(EventProcessingContext &ctx, int siteId, ProductType targetPrdType,
                                       int dwnHistId, Product &outFMaskPrd)
{
    //
    const ProductList &pairedPrds = ctx.GetL1DerivedProducts(siteId, targetPrdType, {dwnHistId});
    if (pairedPrds.size() > 0) {
        outFMaskPrd = pairedPrds.back();
        return true;
    }
    return false;
}

bool MaskedL2AHandler::CreateMaskedL2AProduct(EventProcessingContext &ctx, const TaskFinishedEvent &event) {

    const QString &productPath = GetOutputProductPath(ctx, event);
    const QString &prodName = GetOutputProductName(ctx, event);
    if(prodName != "") {
        const QList<int> &parentIds = GetOutputProductParentProductIds(ctx, event);
        const ProductList &parentL2As = ctx.GetProducts({parentIds[0]});
        const Product &parentL2A = parentL2As[0];
        int ret = ctx.InsertProduct({ ProductType::MaskedL2AProductTypeId, event.processorId, parentL2A.satId, event.siteId,
                            event.jobId, productPath, parentL2A.created,
                            prodName, parentL2A.quicklookImage, parentL2A.geog,
                            parentL2A.orbitId, parentL2A.tiles, parentIds });
        Logger::debug(QStringLiteral("InsertProduct for %1 returned %2").arg(prodName).arg(ret));

        // Cleanup the currently processing products for the current job
        Logger::info(QStringLiteral("Cleaning up the file containing currently processing products for output product %1 and folder %2 and site id %3 and job id %4").
                     arg(prodName).arg(productPath).arg(event.siteId).arg(event.jobId));
        const QString &curProcPrdsFilePath = QDir::cleanPath(GetFinalProductFolder(ctx, event.jobId, event.siteId) +
                                                             QDir::separator() + "current_processing_l2a.txt");
        QStringList prdStrIds;
        for(int id: parentIds) { prdStrIds.append(QString::number(id)); }
        ProcessorHandlerHelper::CleanupCurrentProductIdsForJob(curProcPrdsFilePath, event.jobId, prdStrIds);
    } else {
        Logger::error(QStringLiteral("Cannot insert into database the product with name %1 and path %2").arg(prodName).arg(productPath));
    }

    return true;
}

void MaskedL2AHandler::CreateValidityMaskExtractorStep(EventProcessingContext &ctx,
                                                      const JobSubmittedEvent &evt,
                                                      const InputPrdInfo &prdInfo,
                                                      TaskToSubmit &task,
                                                      NewStepList &steps,
                                                       bool compress,
                                                       bool cog,
                                                       bool continueOnMissingInput)
{
    QString l2aMeta;
    try {
        std::unique_ptr<ProductHelper> helper = ProductHelperFactory::GetProductHelper(prdInfo.l2aPrd.fullPath);
        const QStringList &metaFiles = helper->GetProductMetadataFiles();
        if (metaFiles.size() == 0) {
            throw std::runtime_error(
                        QStringLiteral("Cannot determine the metadata file from the product %1").
                            arg(prdInfo.l2aPrd.fullPath).toStdString());
        }
        l2aMeta = metaFiles[0];
    } catch (const std::exception &e) {
        if (continueOnMissingInput) {
            Logger::error(QStringLiteral("Missing input product with name %1 and path %2. Will continue without it ...").arg(prdInfo.l2aPrd.name).arg(prdInfo.l2aPrd.fullPath));
            return;
        }
        throw std::runtime_error(e.what());
    }

    QStringList args = {"ValidityMaskExtractor",
                       "-xml", l2aMeta,
                       "-out"};
    const auto &targetFolder = GetFinalProductFolder(ctx, evt.jobId, evt.siteId);

    QString mskL2APrdName(prdInfo.l2aPrd.name);
    mskL2APrdName.replace("L2A", "L2AMSK");
    const QString &outProductPath = QString("%1/%2").arg(targetFolder, mskL2APrdName);
    for (int res : gOutRes) {
        QString fileName = mskL2APrdName;
        fileName.remove(".SAFE");
        if (res == -1) {
            fileName.append("_###RES###M");
        } else {
            fileName.append("_").append(QString::number(res)).append("M");
        }
        const QString &outFilePath = QString("%1/%2.tif").arg(outProductPath, fileName);
        args+= outFilePath;
    }

    args+= "-outres";
    for (int res : gOutRes) {
        args+= QString::number(res);
    }
    if (prdInfo.HasFMaskPrd() > 0) {
        args += "-extmask";
        args += prdInfo.fmaskPrd.fullPath;
        args += "-extmaskvalidval";
        args += QString::number(prdInfo.maskValidVal);
    }
    if (compress) {
        args += {"-compress", "1"};
    }
    if (cog) {
        args += {"-cog", "1"};
    }

    WriteOutputProductPath(task, outProductPath);
    WriteOutputProductSourceProductIds(task, {prdInfo.l2aPrd.productId, prdInfo.fmaskPrd.productId});

    steps.append(CreateTaskStep(task, "ValidityMaskExtractor", args));
}

void MaskedL2AHandler::SubmitEndOfLaiTask(EventProcessingContext &ctx,
                                                const JobSubmittedEvent &event,
                                                const QList<TaskToSubmit> &allTasksList)
{
    // add the end of lai job that will perform the cleanup
    QList<std::reference_wrapper<const TaskToSubmit>> endOfJobParents;
    for(const TaskToSubmit &task: allTasksList) {
        endOfJobParents.append(task);
    }
    // we add a task in order to wait for all product formatter to finish.
    // This will allow us to mark the job as finished and to remove the job folder
    TaskToSubmit endOfJobDummyTask{"end-of-job", {}};
    endOfJobDummyTask.parentTasks.append(endOfJobParents);
    SubmitTasks(ctx, event.jobId, {endOfJobDummyTask});
    ctx.SubmitSteps({CreateTaskStep(endOfJobDummyTask, "EndOfJob", QStringList())});

}

QList<MaskedL2AHandler::InputPrdInfo> MaskedL2AHandler::FilterInputProducts(EventProcessingContext &ctx, int siteId, int jobId,
                                                         const QList<InputPrdInfo> &prdsToProcess)
{
    const QString &curProcPrdsFile = QDir::cleanPath(GetFinalProductFolder(ctx, jobId, siteId) +
                                                     QDir::separator() + "current_processing_l2a.txt");

    std::unordered_map<QString, InputPrdInfo> mapFileToPrdsInfo;
    QStringList inPrdIds;
    std::for_each(prdsToProcess.begin(), prdsToProcess.end(), [&inPrdIds, &mapFileToPrdsInfo](const InputPrdInfo &prd) {
        const QString &strId = QString::number(prd.l2aPrd.productId);
        inPrdIds.append(strId);
        mapFileToPrdsInfo[strId] = prd;
    });

    const QStringList &prdsIds = ProcessorHandlerHelper::EnsureMonoDateProductUniqueProc(curProcPrdsFile, ctx, inPrdIds,
                                                            this->processorDescr.processorId, siteId, jobId);

    QList<InputPrdInfo> retPrds;
    std::for_each(prdsIds.begin(), prdsIds.end(), [&retPrds, &mapFileToPrdsInfo](const QString &prd) {
        std::unordered_map<QString, InputPrdInfo>::const_iterator found = mapFileToPrdsInfo.find (prd);
        if (found != mapFileToPrdsInfo.end()) {
            retPrds.append(found->second);
        }
    });
    return retPrds;
}
