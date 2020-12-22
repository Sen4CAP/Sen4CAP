#ifndef RessourceManagerItf_TAO_H
#define RessourceManagerItf_TAO_H

#include "abstractresourcemanageritf.h"
#include "model.hpp"
#include "taojoboperationssender.h"

#include <QNetworkAccessManager>
#include <QNetworkReply>


/**
 * @brief The RessourceManagerItf_TAO class
 * \note
 * This class represents the interface with the Ressource Manager (TAO).
 */
class ResourceManagerItf_TAO : public AbstractResourceManagerItf
{
    Q_OBJECT

public:
    ResourceManagerItf_TAO();
    virtual ~ResourceManagerItf_TAO();

protected:
    virtual bool HandleJobOperations(RequestParamsJobOps *pReqParams);
    virtual bool HandleStartSteps(RequestParamsSubmitSteps *pReqParams);
    virtual void HandleStopTasks(RequestParamsCancelTasks *pReqParams);
    virtual void FillEndStepAddExecInfos(ProcessorExecutionInfos &info);

private:
    bool HandleStartJob(int jobId);
    bool HandleCancelJob(int jobId);
    bool HandlePauseJob(int jobId);
    bool HandleResumeJob(int jobId);

    QString GenerateCallbackJson();

    QMap<QString, QString> GetJobPropertiesMap(int jobId);
    QJsonObject GenerateJobJson(int jobId);
    QJsonArray GenerateTasksJson(int jobId, const QMap<QString, QString> &jobProps);
    QJsonObject GenerateStepJson(const JobStep &taskStep, const std::map<QString, QString> &modulePaths,
                                 const QMap<QString, QString> &jobProps);
    std::map<QString, QString> GetModulePaths(int jobId);

    StepArgumentList GetStepArguments(const JobStepToRun &step);
    QString BuildOutputTempPath(int jobId);
    QString BuildOutputFolderPath(int jobId);

    // void SendRequest(RequestType reqType, const QString &jSon);
    QString GetEndpointName(const RequestType &request);

    TaoJobOperationsSender taoOperationsSenderHandler;
    QString callbackIpAddr;
    QString callbackPort;

signals:
    void startJob(QString endpointName, QString jobJson, QString callbackJson);

};

#endif // RessourceManagerItf_TAO_H
