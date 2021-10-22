#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "trex_handler.hpp"
#include "processorhandlerhelper.h"
#include "json_conversions.hpp"
#include "logger.hpp"
#include "s4c_utils.hpp"
#include <unordered_map>

#include "products/producthelperfactory.h"
using namespace orchestrator::products;

#include <unistd.h>
#include <sys/types.h>

// For unordered map and QString as key
namespace std {
  template<> struct hash<QString> {
    std::size_t operator()(const QString& s) const noexcept {
      return (size_t) qHash(s);
    }
  };
}

TRexHandler::TRexHandler()
{
}

void TRexHandler::CreateTasksAndSteps(EventProcessingContext &ctx, const JobSubmittedEvent &evt) {
    QList<TaskToSubmit> allTasksList;
    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    int cnt = 0;
    allTasksList.append(TaskToSubmit{ "trex-updater", {} });
    allTasksListRef.append(allTasksList[cnt]);

    SubmitTasks(ctx, evt.jobId, allTasksListRef);

    NewStepList allSteps;

    TaskToSubmit &curTask = allTasksList[0];
    CreateTRexUpdaterStep(ctx, evt, curTask, allSteps);
    ctx.SubmitSteps(allSteps);
}

void TRexHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                              const JobSubmittedEvent &evt)
{
    CreateTasksAndSteps(ctx, evt);
}

void TRexHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                              const TaskFinishedEvent &event)
{
    if (event.module == "trex-updater") {
        ctx.MarkJobFinished(event.jobId);
        // Now remove the job folder containing temporary files
        RemoveJobFolder(ctx, event.jobId, processorDescr.shortName);
    }
}

ProcessorJobDefinitionParams TRexHandler::GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
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

    const ConfigurationParameterValueMap &mapCfg = ctx.GetConfigurationParameters(QString(TREX_CFG_PREFIX),
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

void TRexHandler::HandleProductAvailableImpl(EventProcessingContext &ctx,
                                const ProductAvailableEvent &event)
{
    // Get the product description from the database
    const ProductList &prds = ctx.GetProducts({event.productId});
    const Product &prd = prds.back();

    if (prd.productTypeId != ProductType::S4CLPISProductTypeId &&
            prd.productTypeId != ProductType::S4CL4AProductTypeId &&
            prd.productTypeId != ProductType::S4CL4CProductTypeId) {
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
    Logger::info(QStringLiteral("TrexHandler - HandleProductAvailable - Submitted trigger job "
                                "for product %1 (triggered by %2) and siteid = %3").arg(prd.fullPath)
                 .arg(prd.fullPath)
                 .arg(QString::number((int)prd.siteId)));
}

void TRexHandler::CreateTRexUpdaterStep(EventProcessingContext &ctx,
                                                      const JobSubmittedEvent &evt,
                                                      TaskToSubmit &task,
                                                      NewStepList &steps)
{
    const std::map<QString, QString> &configParameters = ctx.GetJobConfigurationParameters(evt.jobId, TREX_CFG_PREFIX);
    const QJsonObject &parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
    const QString &trexContainerName = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters,
                                                                                    "t-rex-container", TREX_CFG_PREFIX);
    const QString &trexOutFile = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters,
                                                                                    "t-rex-output-file", TREX_CFG_PREFIX);
    QStringList args = {"--restart-container", trexContainerName,
                       trexOutFile};
    steps.append(CreateTaskStep(task, "TRexUpdater", args));
}
