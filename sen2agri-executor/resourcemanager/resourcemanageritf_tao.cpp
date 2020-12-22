#include <stdio.h>
#include <QDateTime>
#include <QString>

#include <string>
using namespace std;

#include "resourcemanageritf_tao.h"
#include "persistenceitfmodule.h"

#include <QNetworkRequest>
#include <QNetworkReply>
#include <QAuthenticator>
#include "logger.hpp"
#include "configurationmgr.h"

#define TAO_SERVICES_URL_KEY "executor.resource-manager.tao.services-url"
#define TAO_SERVICES_HTTP_METHOD "executor.resource-manager.tao.services-method"
#define TAO_SERVICES_USER "executor.resource-manager.tao.services-user"
#define TAO_SERVICES_PASSWORD "executor.resource-manager.tao.services-pass"

ResourceManagerItf_TAO::ResourceManagerItf_TAO()
{
    QString serviceUrl;
    if (!ConfigurationMgr::GetInstance()->GetValue(TAO_SERVICES_URL_KEY, serviceUrl)) {
        // Throw an exception here
        throw std::runtime_error(QStringLiteral("No resource manager service URL was defined in the config table.")
                                     .toStdString());
    }
    serviceUrl = serviceUrl.trimmed();
    if (!serviceUrl.endsWith("/")) {
        serviceUrl += "/";
    }
    QString serviceMethod;
    if (!ConfigurationMgr::GetInstance()->GetValue(TAO_SERVICES_HTTP_METHOD, serviceMethod)) {
        serviceMethod = "POST";
    }
    QString serviceUser;
    if (!ConfigurationMgr::GetInstance()->GetValue(TAO_SERVICES_USER, serviceUser)) {
        serviceUser = "";
    }
    QString servicePass;
    if (!ConfigurationMgr::GetInstance()->GetValue(TAO_SERVICES_USER, servicePass)) {
        servicePass = "";
    }

    ConfigurationMgr::GetInstance()->GetValue(SRV_IP_ADDR, callbackIpAddr);
    ConfigurationMgr::GetInstance()->GetValue(SRV_PORT_NO, callbackPort);

    taoOperationsSenderHandler.SetServiceUrl(serviceUrl);
    taoOperationsSenderHandler.SetServiceMethod(serviceMethod);
    taoOperationsSenderHandler.SetServiceUser(serviceUser);
    taoOperationsSenderHandler.SetServicePass(servicePass);

    connect (this, &ResourceManagerItf_TAO::startJob, &taoOperationsSenderHandler, &TaoJobOperationsSender::SendStartJob);
}

ResourceManagerItf_TAO::~ResourceManagerItf_TAO()
{
}

bool ResourceManagerItf_TAO::HandleJobOperations(RequestParamsJobOps *pReqParams)
{
    int jobId = pReqParams->GetJobId();
    switch (pReqParams->GetRequestType()) {
        case START_JOB_REQ:
            return HandleStartJob(jobId);
        case CANCEL_JOB_REQ:
            return HandleCancelJob(jobId);
        case PAUSE_JOB_REQ:
            return HandlePauseJob(jobId);
        case RESUME_JOB_REQ:
            return HandleResumeJob(jobId);
        default:
            break;
    }
    return false;
}

/**
 * @brief RessourceManagerItf_TAO::StartProcessor
 */
bool ResourceManagerItf_TAO::HandleStartSteps(RequestParamsSubmitSteps *)
{
    // TODO: not supported for now
    return false;
}

/**
 * @brief RessourceManagerItf_TAO::StopProcessor
 */
void ResourceManagerItf_TAO::HandleStopTasks(RequestParamsCancelTasks *)
{
    // TODO: not supported for now
}

void ResourceManagerItf_TAO::FillEndStepAddExecInfos(ProcessorExecutionInfos &)
{
    // TODO: not supported for now
}

bool ResourceManagerItf_TAO::HandleStartJob(int jobId)
{
    const QJsonObject &jobJsonObj = GenerateJobJson(jobId);

    QJsonDocument doc(jobJsonObj);
    const QString &jsonStr = doc.toJson(QJsonDocument::Compact);
    qDebug() << jsonStr;

    emit startJob(GetEndpointName(START_JOB_REQ), jsonStr, GenerateCallbackJson());

    return true;
}

bool ResourceManagerItf_TAO::HandleCancelJob(int )
{
    return false;
}

bool ResourceManagerItf_TAO::HandlePauseJob(int )
{
    return false;
}

bool ResourceManagerItf_TAO::HandleResumeJob(int )
{
    return false;
}

QString ResourceManagerItf_TAO::GetEndpointName(const RequestType &request) {
    switch(request) {
        case START_JOB_REQ:
            return "process";
        case CANCEL_JOB_REQ:
            return "stop";
        case PAUSE_JOB_REQ:
            return "pause";
        case RESUME_JOB_REQ:
            return "resume";
        default:
            break;
    }
    return "";
}

QString ResourceManagerItf_TAO::GenerateCallbackJson()
{

    QJsonObject jobJsonObj;
    jobJsonObj.insert("protocol", "tcp");
    jobJsonObj.insert("hostName", callbackIpAddr);
    jobJsonObj.insert("port", callbackPort);
    QJsonDocument doc(jobJsonObj);
    return doc.toJson(QJsonDocument::Compact);
}

QMap<QString, QString> ResourceManagerItf_TAO::GetJobPropertiesMap(int jobId) {
    QMap<QString, QString> mapProperties;
    mapProperties["temporaryFolder"] = BuildOutputTempPath(jobId);
    mapProperties["outputFolder"] = BuildOutputFolderPath(jobId);
    mapProperties["configurationFolder"] = "/usr/share/sen2agri";

    return mapProperties;
}

QJsonObject ResourceManagerItf_TAO::GenerateJobJson(int jobId)
{

    QJsonObject jobJsonObj;
    jobJsonObj.insert("jobId", QJsonValue::fromVariant(jobId));

    // Add the properties
    const QMap<QString, QString> &jobProps = GetJobPropertiesMap(jobId);
    for(auto e : jobProps.keys()) {
        jobJsonObj.insert(e, jobProps.value(e));
    }

    // Add the tasks
    jobJsonObj.insert("tasks", GenerateTasksJson(jobId, jobProps));

    return jobJsonObj;
}

QJsonArray ResourceManagerItf_TAO::GenerateTasksJson(int jobId, const QMap<QString, QString> &jobProps)
{
    const JobStepList &steps = PersistenceItfModule::GetInstance()->GetDBProvider()->GetJobSteps(jobId);
    QMap<int, JobStepList> tasksMap;
    for (const JobStep &step: steps) {
        tasksMap[step.taskId].append(step);
    }

    const std::map<QString, QString> &modulePaths = GetModulePaths(jobId);
    QJsonArray arrTasks;
    for(auto e : tasksMap.keys())
    {
        const JobStepList &taskSteps = tasksMap.value(e);

        // extract the preceding tasks ids
        QJsonArray precTaskIds;
        for (int precTaskId : taskSteps[0].precedingTaskIds) {
            precTaskIds.append(QJsonValue::fromVariant(precTaskId));
        }

        // create the steps array of the task
        QJsonArray stepsJsonArr;
        for (const JobStep &taskStep: taskSteps) {
            stepsJsonArr.append(GenerateStepJson(taskStep, modulePaths, jobProps));
        }

        QJsonObject taskJsonObj;
        taskJsonObj.insert("id", QJsonValue::fromVariant(e));
        taskJsonObj.insert("preceding_task_ids", precTaskIds);
        taskJsonObj.insert("name", QJsonValue::fromVariant(taskSteps[0].module));
        taskJsonObj.insert("steps", stepsJsonArr);

        arrTasks.append(taskJsonObj);
    }

    return arrTasks;
}

QJsonObject ResourceManagerItf_TAO::GenerateStepJson(const JobStep &taskStep, const std::map<QString, QString> &modulePaths,
                                                     const QMap<QString, QString> &jobProps)
{
    QString processorPath;
    auto modulePathsEnd = std::end(modulePaths);
    // first get the executable for this module
    auto it = modulePaths.find(taskStep.module);
    if (it == modulePathsEnd) {
        processorPath = "otbcli";   // default is an OTB application if not found in DB
    } else {
        processorPath = it->second;
    }

    // add the step command line arguments
    QJsonArray stepCmdParams;
    stepCmdParams.append(processorPath);
    const StepArgumentList &arguments = GetStepArguments(taskStep);
    for (const StepArgument &stepArg: arguments) {

        // Replace the possible occurences of the values in the properties map in the current argument
        QString arg = stepArg.value;
        for (auto mapPair : jobProps.toStdMap()) {
            arg.replace(mapPair.second, mapPair.first);
        }

        stepCmdParams.append(QJsonValue::fromVariant(arg));
    }

    QJsonObject stepJsonObj;
    stepJsonObj.insert("name", QJsonValue::fromVariant(taskStep.stepName));
    stepJsonObj.insert("arguments", stepCmdParams);

    return stepJsonObj;
}

std::map<QString, QString> ResourceManagerItf_TAO::GetModulePaths(int jobId)
{
    const auto &paramList = PersistenceItfModule::GetInstance()->GetDBProvider()->
                            GetJobConfigurationParameters(jobId, QStringLiteral("executor.module.path."));
    std::map<QString, QString> result;
    for (const auto &p : paramList) {
        result.emplace(p.key, p.value);
    }

    std::map<QString, QString> modulePaths;
    for (const auto &p : result) {
        modulePaths.emplace(p.first.mid(p.first.lastIndexOf('.') + 1), p.second);
    }
    return modulePaths;
}

StepArgumentList ResourceManagerItf_TAO::GetStepArguments(const JobStepToRun &step)
{
    const auto &parametersDoc = QJsonDocument::fromJson(step.parametersJson.toUtf8());
    if (!parametersDoc.isObject()) {
        throw std::runtime_error(
            QStringLiteral("Unexpected step parameter JSON schema: root node should be an "
                           "object. The parameter JSON was: '%1'")
                .arg(step.parametersJson)
                .toStdString());
    }

    const auto &argNode = parametersDoc.object()[QStringLiteral("arguments")];
    if (!argNode.isArray()) {
        throw std::runtime_error(
            QStringLiteral("Unexpected step parameter JSON schema: node 'arguments' should be an "
                           "array. The parameter JSON was: '%1'")
                .arg(step.parametersJson)
                .toStdString());
    }
    const auto &argArray = argNode.toArray();

    StepArgumentList arguments;
    arguments.reserve(argArray.count());
    for (const auto &arg : argArray) {
        if (!arg.isString()) {
            throw std::runtime_error(
                QStringLiteral("Unexpected step parameter JSON schema: arguments should be "
                               "strings. The parameter JSON object was: '%1'")
                    .arg(step.parametersJson)
                    .toStdString());
        }

        arguments.append(arg.toString());
    }

    return arguments;
}

QString ResourceManagerItf_TAO::BuildOutputTempPath(int jobId)
{
    // Ex: "temporaryFolder" : "/mnt/archive/orchestrator_temp/l3b/4875"
    QString orchestratorTempPath;

    const JobDefinition &jobDef = PersistenceItfModule::GetInstance()->GetDBProvider()->GetJobDefinition(jobId);
    const QString &procName = PersistenceItfModule::GetInstance()->GetDBProvider()->GetProcessorShortName(jobDef.processorId);
    // first try to get the scratch path specific for the processor, if defined
    if (!ConfigurationMgr::GetInstance()->GetValue(QString(ORCHESTRATOR_TMP_PATH) + "." + procName, orchestratorTempPath)) {
        // get the general scratch path
        ConfigurationMgr::GetInstance()->GetValue(ORCHESTRATOR_TMP_PATH, orchestratorTempPath);
    }
    orchestratorTempPath.replace("{job_id}", QString::number(jobId));
    QStringList tokens = orchestratorTempPath.split("/");
    while(tokens[tokens.length() - 1].contains("{")) {
        tokens.removeLast();
    }
    return tokens.join("/");
}

QString ResourceManagerItf_TAO::BuildOutputFolderPath(int jobId)
{
    const JobDefinition &jobDef = PersistenceItfModule::GetInstance()->GetDBProvider()->GetJobDefinition(jobId);
    const QString &procName = PersistenceItfModule::GetInstance()->GetDBProvider()->GetProcessorShortName(jobDef.processorId);
    const QString &siteName = PersistenceItfModule::GetInstance()->GetDBProvider()->GetSiteShortName(jobDef.siteId);

    // Ex:  "outputFolder" : "/mnt/archive/test_theia/l3b_lai/",
    return "/mnt/archive/" + siteName + "/" + procName + "/";
}

