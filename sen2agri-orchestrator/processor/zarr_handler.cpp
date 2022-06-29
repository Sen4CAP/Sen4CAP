#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "zarr_handler.hpp"
#include "processorhandlerhelper.h"
#include "json_conversions.hpp"
#include "logger.hpp"
#include "s4c_utils.hpp"
#include <unordered_map>

#include "products/producthelperfactory.h"
using namespace orchestrator::products;

#include <unistd.h>
#include <sys/types.h>

static ProductType ZARR_PRODUCT_TYPES[] = {ProductType::L3AProductTypeId,
                                           ProductType::L3BProductTypeId,
                                           ProductType::L4AProductTypeId,
                                           ProductType::L4BProductTypeId,
                                           ProductType::S4CS1L2AmpProductTypeId,
                                           ProductType::S4CS1L2CoheProductTypeId
                                          };

// For unordered map and QString as key
namespace std {
  template<> struct hash<QString> {
    std::size_t operator()(const QString& s) const noexcept {
      return (size_t) qHash(s);
    }
  };
}

ZarrHandler::ZarrHandler()
{
}

void ZarrHandler::CreateTasksAndSteps(EventProcessingContext &ctx, const JobSubmittedEvent &evt,
                                      const QList<Product> &prdInfos) {
    QList<TaskToSubmit> allTasksList;
    QList<std::reference_wrapper<const TaskToSubmit>> endOfJobParentsRefs;
    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    int cnt = 0;
    for (cnt = 0; cnt<prdInfos.size(); cnt++) {
        allTasksList.append(TaskToSubmit{ "zarr-converter", {} });
        endOfJobParentsRefs.append(allTasksList[cnt]);
        allTasksListRef.append(allTasksList[cnt]);
    }
    allTasksList.append({"end-of-job", {}});
    allTasksList[cnt].parentTasks.append(endOfJobParentsRefs);
    allTasksListRef.append(allTasksList[cnt]);

    SubmitTasks(ctx, evt.jobId, allTasksListRef);

    NewStepList allSteps;

    cnt = 0;
    for(const Product &prdInfo: prdInfos) {
        TaskToSubmit &curTask = allTasksList[cnt++];
        CreateZarrStep(prdInfo, curTask, allSteps);
    }
    // create the end of all steps marker
    allSteps.append(CreateTaskStep(allTasksList[allTasksList.size()-1], "EndOfJob", QStringList()));
    ctx.SubmitSteps(allSteps);
}

void ZarrHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                              const JobSubmittedEvent &evt)
{
    auto parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();

    // if a scheduled job, we are extracting all the not processed products
    ProductList prds;
    int ret = GetProductsFromSchedReq(ctx, evt, parameters, prds);
    // no products available from the scheduling ... mark also the job as failed
    // TODO: Maybe we should somehow delete completely the job
    if (ret == 0) {
        ctx.MarkJobFailed(evt.jobId);
        throw std::runtime_error(
                    QStringLiteral("Zarr Scheduled job with id %1 for site %2 marked as done as no products are available for now to process").
                                         arg(evt.jobId).arg(evt.siteId).toStdString());
    } else if (ret == -1) {
        // custom job
        for (ProductType prdType : ZARR_PRODUCT_TYPES) {
            const QStringList &prdNames = GetInputProductNames(parameters, prdType);
            prds += ctx.GetProducts(evt.siteId, prdNames);
        }
        if (prds.size() == 0) {
            ctx.MarkJobFailed(evt.jobId);
            throw std::runtime_error(
                        QStringLiteral("No products provided for custom job %1 on site %2 for Zarr convertion").
                                             arg(evt.jobId).arg(evt.siteId).toStdString());
        }
    }

    CreateTasksAndSteps(ctx, evt, prds);
}

void ZarrHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                              const TaskFinishedEvent &event)
{
    if (event.module == "end-of-job") {
        ctx.MarkJobFinished(event.jobId);
        // Now remove the job folder containing temporary files
        RemoveJobFolder(ctx, event.jobId, processorDescr.shortName);
    }
}

ProcessorJobDefinitionParams ZarrHandler::GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
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

    const ConfigurationParameterValueMap &mapCfg = ctx.GetConfigurationParameters(QString(ZARR_CFG_PREFIX),
                                                                           siteId, requestOverrideCfgValues);
    std::map<QString, QString> configParams;
    for (const auto &p : mapCfg) {
        configParams.emplace(p.key, p.value);
    }
    params.jsonParameters.append("{ \"scheduled_job\": \"1\", \"start_date\": \"" + seasonStartDate.toString("yyyyMMdd") + "\", " +
                                 "\"end_date\": \"" + qScheduledDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_start_date\": \"" + seasonStartDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_end_date\": \"" + seasonEndDate.toString("yyyyMMdd") + "\"}");

    params.isValid = true;

    return params;
}

void ZarrHandler::HandleProductAvailableImpl(EventProcessingContext &ctx,
                                const ProductAvailableEvent &event)
{
    // Get the product description from the database
    const ProductList &prds = ctx.GetProducts({event.productId});
    if(prds.size() == 0) {
        return;
    }
    const Product &prd = prds.back();

    if (prd.productTypeId != ProductType::S4CS1L2AmpProductTypeId &&
        prd.productTypeId != ProductType::S4CS1L2CoheProductTypeId) {
        return;
    }

    // Create a new JOB
    NewJob newJob;
    newJob.processorId = processorDescr.processorId;  //send the job to this processor
    newJob.siteId = prd.siteId;
    newJob.startType = JobStartType::Triggered;

    QJsonObject processorParamsObj;
    QJsonArray prodsJsonArray;
    prodsJsonArray.append(prd.name);

    const QString &prdKey = "input_products";
    processorParamsObj[prdKey] = prodsJsonArray;
    newJob.parametersJson = jsonToString(processorParamsObj);
    ctx.SubmitJob(newJob);
    Logger::info(QStringLiteral("ZarrHandler - HandleProductAvailable - Submitted trigger job "
                                "for product %1 (triggered by %2) and siteid = %3").arg(prd.fullPath)
                 .arg(prd.fullPath)
                 .arg(QString::number((int)prd.siteId)));
}

void ZarrHandler::CreateZarrStep(const Product &prdInfo,
                                 TaskToSubmit &task,
                                 NewStepList &steps)
{
    QStringList args = {"--product-path", prdInfo.fullPath};
    steps.append(CreateTaskStep(task, "ZarrConverter", args));
}

int ZarrHandler::GetProductsFromSchedReq(EventProcessingContext &ctx,
                                                          const JobSubmittedEvent &event, QJsonObject &parameters,
                                                          ProductList &outPrdsList) {
    int jobVal;
    QString strStartDate, strEndDate;
    if(ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "scheduled_job", jobVal) && (jobVal == 1) &&
        ProcessorHandlerHelper::GetParameterValueAsString(parameters, "start_date", strStartDate) &&
        ProcessorHandlerHelper::GetParameterValueAsString(parameters, "end_date", strEndDate)) {
        const auto &startDate = ProcessorHandlerHelper::GetLocalDateTime(strStartDate);
        const auto &endDate = ProcessorHandlerHelper::GetLocalDateTime(strEndDate);

        Logger::info(QStringLiteral("Zarr Converter Scheduled job received for siteId = %1, startDate=%2, endDate=%3").
                     arg(event.siteId).arg(startDate.toString("yyyyMMddTHHmmss")).arg(endDate.toString("yyyyMMddTHHmmss")));
        for (ProductType prdType : ZARR_PRODUCT_TYPES) {
            outPrdsList += ctx.GetProducts(event.siteId, (int)prdType, startDate, endDate);
        }
        return outPrdsList.size();
    }
    return -1;
}
