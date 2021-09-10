#include <QDBusPendingCallWatcher>

#include <optional.hpp>

#include "orchestrator.hpp"
#include "settings.hpp"
#include "configuration.hpp"

#include "make_unique.hpp"
#include "processor/cropmaskhandler.hpp"
#include "processor/croptypehandler.hpp"
#include "processor/compositehandler.hpp"
#include "processor/lairetrievalhandler.hpp"
#include "processor/lairetrievalhandler_l3c.hpp"
#include "processor/lairetrievalhandler_l3d.hpp"
#include "processor/phenondvihandler.hpp"
#include "processor/s4c_croptypehandler.hpp"
#include "processor/grasslandmowinghandler.hpp"
#include "processor/agricpracticeshandler.hpp"
#include "processor/s4c_markersdb1.hpp"
#include "processor/masked_l2a_handler.hpp"
#include "processor/s4s_permanent_crop_handler.hpp"
#include "processor/s4s_yieldhandler.hpp"
#include "json_conversions.hpp"
#include "schedulingcontext.h"
#include "logger.hpp"

std::map<int, std::unique_ptr<ProcessorHandler>> & GetHandlersMap(PersistenceManagerDBProvider &persistenceManager) {
    ProcessorDescriptionList processorsDescriptions = persistenceManager.GetProcessorDescriptions();
    static std::map<int, std::unique_ptr<ProcessorHandler>> handlersMap;
    for(ProcessorDescription procDescr: processorsDescriptions) {
        bool bAdded = true;
        if(procDescr.shortName == "l2a") {
            // TODO:
            //handlers.emplace(procDescr.processorId, std::make_unique<MACCSHandler>());
            bAdded = false;
        } else if(procDescr.shortName == "l3a") {
            handlersMap.emplace(procDescr.processorId, std::make_unique<CompositeHandler>());
        } else if(procDescr.shortName == "l3b_lai" || procDescr.shortName == "l3b") {
            handlersMap.emplace(procDescr.processorId, std::make_unique<LaiRetrievalHandler>());
        } else if(procDescr.shortName == "l3e_pheno" || procDescr.shortName == "l3e") {
            handlersMap.emplace(procDescr.processorId, std::make_unique<PhenoNdviHandler>());
        } else if(procDescr.shortName == "l4a") {
            handlersMap.emplace(procDescr.processorId, std::make_unique<CropMaskHandler>());
        } else if(procDescr.shortName == "l4b") {
            handlersMap.emplace(procDescr.processorId, std::make_unique<CropTypeHandler>());
        } else if(procDescr.shortName == "s4c_l4a") {
            handlersMap.emplace(procDescr.processorId, std::make_unique<S4CCropTypeHandler>());
        } else if(procDescr.shortName == "s4c_l4b") {
            handlersMap.emplace(procDescr.processorId, std::make_unique<GrasslandMowingHandler>());
        } else if(procDescr.shortName == "s4c_l4c") {
            handlersMap.emplace(procDescr.processorId, std::make_unique<AgricPracticesHandler>());
        } else if(procDescr.shortName == "s2a_l3c") {
            handlersMap.emplace(procDescr.processorId, std::make_unique<LaiRetrievalHandlerL3C>());
        } else if(procDescr.shortName == "s4s_perm_crop") {
            handlersMap.emplace(procDescr.processorId, std::make_unique<S4SPermanentCropHandler>());
        } else if(procDescr.shortName == "s2a_l3d") {
            handlersMap.emplace(procDescr.processorId, std::make_unique<LaiRetrievalHandlerL3D>());
        } else if(procDescr.shortName == "s4c_mdb1") {
            handlersMap.emplace(procDescr.processorId, std::make_unique<S4CMarkersDB1Handler>());
        } else if(procDescr.shortName == "l2a_msk") {
            handlersMap.emplace(procDescr.processorId, std::make_unique<MaskedL2AHandler>());
        } else if(procDescr.shortName == "s4s_yield_feat") {
            handlersMap.emplace(procDescr.processorId, std::make_unique<S4SYieldHandler>());
        } else {
            bAdded = false;
            Logger::error(QStringLiteral("Invalid processor configuration found in database: %1, "
                                         "igoring it as no handler is available for it!")
                          .arg(procDescr.shortName));
        }
        if(bAdded) {
            auto it = handlersMap.find(procDescr.processorId);
            ProcessorHandler &handler = *it->second;
            handler.SetProcessorDescription(procDescr);
        }
    }

    return handlersMap;
}


Orchestrator::Orchestrator(QObject *parent)
    : QObject(parent),
      persistenceManager(
          Settings::readSettings(getConfigurationFile(*QCoreApplication::instance()))),
      worker(GetHandlersMap(persistenceManager), persistenceManager)
{
    worker.RescanEvents();
}

void Orchestrator::NotifyEventsAvailable() { RescanEvents(); }

void Orchestrator::RescanEvents() { worker.RescanEvents(); }

JobDefinition Orchestrator::GetJobDefinition(const ProcessingRequest &request)
{
    ConfigurationParameterValueMap requestOverrideCfgValues;

    QJsonObject retObj;
    const auto &doc = QJsonDocument::fromJson(request.parametersJson.toUtf8());
    if (doc.isObject()) {
        const auto &inObj = doc.object();
        const auto &generalParamNode = inObj[QStringLiteral("general_params")];
        if (generalParamNode.isObject()) {
            const auto &generalParamObj = generalParamNode.toObject();
            for(QJsonObject::const_iterator iter = generalParamObj.begin(); iter != generalParamObj.end (); ++iter)
            {
                requestOverrideCfgValues[iter.key()] = {iter.key(), -1, iter.value().toString()};
            }

            // pass the general params to the submitted job later
            retObj[QStringLiteral("general_params")] = generalParamObj;
        }

        const auto &configParamNode = inObj[QStringLiteral("config_params")];
        if (configParamNode.isObject()) {
            const auto &configParamObj = configParamNode.toObject();
            for(QJsonObject::const_iterator iter = configParamObj.begin(); iter != configParamObj.end (); ++iter)
            {
                requestOverrideCfgValues[iter.key()] = {iter.key(), -1, iter.value().toString()};
            }

            // pass the config params to the submitted job later
            retObj[QStringLiteral("config_params")] = configParamObj;
        }
    }

    SchedulingContext ctx(persistenceManager);
    JobDefinition jobDef;
    jobDef.processorId = request.processorId;
    jobDef.siteId = request.siteId;
    try {
        ProcessorHandler &handler = worker.GetHandler(request.processorId);
        ProcessorJobDefinitionParams procDefParams = handler.GetProcessingDefinition(ctx, request.siteId, request.ttNextScheduledRunTime, requestOverrideCfgValues);
        jobDef.isValid = procDefParams.isValid;
        if(jobDef.isValid) {
            QJsonArray inputProductsArr;
            for (const auto &p : procDefParams.productList) {
                inputProductsArr.append(p.name);
            }
            QJsonObject processorParamsObj;
            // add the input products key to the processor params
            processorParamsObj[QStringLiteral("input_products")] = inputProductsArr;

            // now add any other parameters computed by the processor handler
            const auto &docProcParams = QJsonDocument::fromJson(procDefParams.jsonParameters.toUtf8());
            if (docProcParams.isObject()) {
                const auto &docProcParamsObj = docProcParams.object();
                for(QJsonObject::const_iterator iter = docProcParamsObj.begin(); iter != docProcParamsObj.end (); ++iter)
                {
                    processorParamsObj[iter.key()] = iter.value().toString();
                }
            }

            retObj[QStringLiteral("processor_params")] = processorParamsObj;
            jobDef.jobDefinitionJson = jsonToString(retObj);
        }
    }
    catch (...) {
        std::exception_ptr p = std::current_exception();
        Logger::info(QStringLiteral("Exception during creation of job definition: %1")
                     .arg((p ? p.__cxa_exception_type()->name() : "null")));
    }

    return jobDef;
}

void Orchestrator::SubmitJob(const JobDefinition &job)
{
    const auto &doc = QJsonDocument::fromJson(job.jobDefinitionJson.toUtf8());
    if (!doc.isObject()) {
        throw std::runtime_error(
            QStringLiteral("Unexpected step parameter JSON schema: root node should be an "
                           "object. The parameter JSON was: '%1'").arg(job.jobDefinitionJson).toStdString());
    }
    const auto &object = doc.object();
    // Get the general parameters node
    const auto &generalParamsNode = object[QStringLiteral("general_params")];
    if (!generalParamsNode.isObject()) {
        throw std::runtime_error(
            QStringLiteral("Unexpected step parameter JSON schema: node 'general_params' should be an "
                           "object. The parameter JSON was: '%1'").arg(job.jobDefinitionJson).toStdString());
    }
    const auto &generalParamsObj = generalParamsNode.toObject();
    QString taskName = generalParamsObj["task_name"].toString();
    QString taskDescr = generalParamsObj["task_description"].toString();
    QString taskStartType = generalParamsObj["task_type"].toString();

    NewJob newJob;
    newJob.processorId = job.processorId;
    newJob.name = taskName;
    newJob.description = taskDescr;

    newJob.siteId = job.siteId;
    if(taskStartType == "requested")
        newJob.startType = JobStartType::Requested;
    else if(taskStartType == "triggered")
        newJob.startType = JobStartType::Triggered;
    else
        newJob.startType = JobStartType::Scheduled;

    // this one is requested as we should have at least the input products
    const auto &processorParamsNode = object[QStringLiteral("processor_params")];
    if (!processorParamsNode.isObject()) {
        throw std::runtime_error(
            QStringLiteral("Unexpected step parameter JSON schema: node 'processor_params' should be an "
                           "object. The parameter JSON was: '%1'").arg(job.jobDefinitionJson).toStdString());
    }
    QJsonObject processorParamsObj(processorParamsNode.toObject());
    // Add also the site_name and processor_name to the parameteres
    QString processorName = persistenceManager.GetProcessorShortName(job.processorId);
    if(processorName.length() == 0)
        processorName = "UnknownProcessor";
    QString siteName = persistenceManager.GetSiteName(job.siteId);
    if(siteName.length() == 0)
        siteName = "UnknownSite";
    processorParamsObj["processor_short_name"] = processorName;
    processorParamsObj["site_name"] = siteName;
    newJob.parametersJson = jsonToString(processorParamsObj);

    // this one is optional as we might have any configuration params specified
    const auto &configParamNode = object[QStringLiteral("config_param")];
    if (configParamNode.isObject()) {
        const auto &configParamObj = configParamNode.toObject();

        for(QJsonObject::const_iterator iter = configParamObj.begin(); iter != configParamObj.end (); ++iter)
        {
            newJob.configuration.append(JobConfigurationUpdateAction(iter.key(), iter.value().toString()));
        }
    }

    persistenceManager.SubmitJob(newJob);
    NotifyEventsAvailable();
}

