#ifndef AbstractRessourceManagerItf_H
#define AbstractRessourceManagerItf_H

#include <QThread>
#include <QList>
#include <QMutex>
#include <QWaitCondition>

#include <string>
using namespace std;

#include "iprocessorwrappermsgslistener.h"
#include "requestparamssubmitsteps.h"
#include "requestparamscanceltasks.h"
#include "requestparamsjobops.h"
#include "requestparamsexecutioninfos.h"
#include "processorexecutioninfos.h"


/**
 * @brief The AbstractRessourceManagerItf class
 * \note
 * This class represents the base class for the interface with the Ressource Manager (SLURM, TAO etc.)
 */
class AbstractResourceManagerItf : public QThread, public IProcessorWrapperMsgsListener
{
    Q_OBJECT

public:
    AbstractResourceManagerItf();
    ~AbstractResourceManagerItf();

    virtual bool Start();
    virtual void Stop();

    virtual void PerformJobOperation(RequestParamsJobOps* pReqParams);
    virtual void StartExecutionSteps(RequestParamsSubmitSteps *pReqParams);
    virtual void CancelTasks(RequestParamsCancelTasks *pReqParams);

    virtual void OnStepFeedbackNewMsg(RequestParamsBase *pReq);

protected:
    virtual bool HandleJobOperations(RequestParamsJobOps *pReqParams) = 0;
    virtual bool HandleStartSteps(RequestParamsSubmitSteps *pReqParams) = 0;
    virtual void HandleStopTasks(RequestParamsCancelTasks *pReqParams) = 0;

    virtual void FillEndStepAddExecInfos(const QString &strJobName, ProcessorExecutionInfos &infos);

    QString BuildStepExecutionName(int nTaskId, QString &stepName, int idx);
    bool ParseStepExecutionName(const QString &jobName, int &nTaskId, QString &strStepName, int &nStepIdx);
    QString BuildParamsString(QStringList &listParams);

private:
    void run();
    bool m_bStop;
    bool m_bStarted;

    QList<RequestParamsBase*> m_msgQueue;
    QMutex m_syncMutex;
    QWaitCondition m_condition;

    void AddRequestToQueue(RequestParamsBase *pReq);
    void HandleStepFeedbackInfosMsg(RequestParamsExecutionInfos *pReqParams);

    // particular implementations for HandleProcessorInfosMsg
    void HandleStepExecutionStartedMsg(RequestParamsExecutionInfos *pReqParams);
    void HandleStepEndedMsg(RequestParamsExecutionInfos *pReqParams);
    void HandleStepInfosLogMsg(RequestParamsExecutionInfos *pReqParams);
};

#endif // AbstractRessourceManagerItf_H
