#include <stdio.h>
#include <QDateTime>
#include <QString>

#include <string>
using namespace std;

#include "abstractresourcemanageritf.h"
#include "commandinvoker.h"

#include "slurmsacctresultparser.h"
#include "processorwrapperfactory.h"
#include "logger.hpp"
#include "configurationmgr.h"
#include "persistenceitfmodule.h"
#include "orchestratorclient/orchestratorclientfactory.h"

AbstractResourceManagerItf::AbstractResourceManagerItf()
{
    m_bStop = false;
    m_bStarted = false;
}

AbstractResourceManagerItf::~AbstractResourceManagerItf()
{
    Stop();
}

bool AbstractResourceManagerItf::Start()
{
    if (m_bStarted) {
        return true;
    }
    m_bStop = false;
    start();

    return true;
}

void AbstractResourceManagerItf::Stop()
{
    m_bStop = true;
    m_condition.wakeAll();
    quit();
    wait();
}

void AbstractResourceManagerItf::run()
{
    RequestParamsBase *pReqParams;
    bool bAvailable = false;

    while (!m_bStop) {
        pReqParams = 0;
        m_syncMutex.lock();
        m_condition.wait(&m_syncMutex);
        if (!m_msgQueue.isEmpty()) {
            pReqParams = m_msgQueue.takeFirst();
            bAvailable = true;
        } else {
            // we can have a stop command that only awakes the thread
            bAvailable = false;
        }
        m_syncMutex.unlock();

        while (bAvailable && pReqParams) {
            bAvailable = false;
            RequestType nMsgType = pReqParams->GetRequestType();
            switch (nMsgType) {
                case START_JOB_REQ:
                case CANCEL_JOB_REQ:
                case PAUSE_JOB_REQ:
                case RESUME_JOB_REQ:
                    HandleJobOperations((RequestParamsJobOps *) pReqParams);
                    break;
                case STEP_EXECUTION_INFO_MSG:
                    HandleStepFeedbackInfosMsg((RequestParamsExecutionInfos *) pReqParams);
                    break;
                case START_STEP_REQ:
                    HandleStartSteps((RequestParamsSubmitSteps *) pReqParams);
                    break;
                case STOP_STEP_REQ:
                    HandleStopTasks((RequestParamsCancelTasks *) pReqParams);
                    break;
                default: // unknown msg type
                    break;
            }
            if (pReqParams)
                delete pReqParams;
            if (!m_msgQueue.isEmpty()) {
                pReqParams = m_msgQueue.takeFirst();
                bAvailable = true;
            }
        }
    }
}

/**
 * @brief AbstractRessourceManagerItf::StartExecutionSteps
 */
void AbstractResourceManagerItf::PerformJobOperation(RequestParamsJobOps* pReqParams)
{
    AddRequestToQueue(pReqParams);
}

/**
 * @brief AbstractRessourceManagerItf::StartExecutionSteps
 */
void AbstractResourceManagerItf::StartExecutionSteps(RequestParamsSubmitSteps *pReqParams)
{
    AddRequestToQueue(pReqParams);
}

/**
 * @brief AbstractRessourceManagerItf::CancelTasks
 */
void AbstractResourceManagerItf::CancelTasks(RequestParamsCancelTasks *pReqParams)
{
    AddRequestToQueue(pReqParams);
}

/**
 * @brief AbstractRessourceManagerItf::OnProcessorFinishedExecution
 * \note This function is called when a processor finished execution
 * It is called by the ExecutionInfosProtocolServer.
 */
void AbstractResourceManagerItf::OnStepFeedbackNewMsg(RequestParamsBase *pReq)
{
    AddRequestToQueue(pReq);
}

void AbstractResourceManagerItf::AddRequestToQueue(RequestParamsBase *pReq)
{
    m_syncMutex.lock();
    m_msgQueue.push_back(pReq);
    m_syncMutex.unlock();
    m_condition.wakeAll();
}

void AbstractResourceManagerItf::HandleStepFeedbackInfosMsg(RequestParamsExecutionInfos *pReqParams)
{
    if (pReqParams->IsExecutionStarted()) {
        HandleStepExecutionStartedMsg(pReqParams);
    } else if (pReqParams->IsExecutionEnded()) {
        HandleStepEndedMsg(pReqParams);
    } else if (pReqParams->IsLogMsg()) {
        HandleStepInfosLogMsg(pReqParams);
    } else {
        Logger::info("Invalid message received from processor wrapper!");
    }
}

void AbstractResourceManagerItf::HandleStepExecutionStartedMsg(
    RequestParamsExecutionInfos *pReqParams)
{
    // TODO: send the information to persistence manager
    // Send the statistic infos to the persistence interface module
    QString strJobName = pReqParams->GetJobName();
    int nTaskId = -1, nStepIdx = -1;
    QString strStepName;
    if (ParseStepExecutionName(strJobName, nTaskId, strStepName, nStepIdx)) {
        PersistenceItfModule::GetInstance()->MarkStepStarted(nTaskId, strStepName);
    } else {
        Logger::error(
            QString("Invalid job name for starting execution message %1").arg(strJobName));
    }
}

void AbstractResourceManagerItf::HandleStepEndedMsg(RequestParamsExecutionInfos *pReqParams)
{
    // Get the job name and the execution time
    QString strJobName = pReqParams->GetJobName();
    QString strStatusText = pReqParams->GetStatusText();
    int nExitCode = pReqParams->GetExitCode();
    QString executionDuration = pReqParams->GetExecutionTime();
    int nTaskId = -1, nStepIdx = -1;
    QString strStepName;

    Logger::debug(QString("HandleProcessorEndedMsg: Received message from job name %1 with status %2 and exit code %3")
                      .arg(strJobName)
                      .arg(strStatusText)
                      .arg(nExitCode));

    // check if it a correct job name and extract the information from it
    if (ParseStepExecutionName(strJobName, nTaskId, strStepName, nStepIdx)) {
        ProcessorExecutionInfos jobExecInfos;
        // request additional end step infos
        FillEndStepAddExecInfos(strJobName, jobExecInfos);
        jobExecInfos.strJobName = strJobName;
        jobExecInfos.strExecutionDuration = executionDuration;
        jobExecInfos.strJobStatus = ProcessorExecutionInfos::g_strFinished;
        jobExecInfos.strStdOutText = pReqParams->GetStdOutText();
        if (jobExecInfos.strStdOutText.size() == 0) {
            jobExecInfos.strStdOutText = "empty log";
        }
        jobExecInfos.strStdErrText = pReqParams->GetStdErrText();
        if (jobExecInfos.strStdErrText.size() == 0) {
            jobExecInfos.strStdErrText = "empty err log";
        }
        jobExecInfos.strExitCode = QString::number(nExitCode);
        // Send the statistic infos to the persistence interface module
        // TODO: Here according to the exitCode should be executed MarkStepFinished if nExitCode == 0 and MarkStepFailed otherwise.
        //       the problem is that MarkStepFailed is marking also job as Failed which is not always OK (we might
        //      have some steps that could fail without making the whole job failed => ex. processing 99 tiles OK but
        //       for one the processing is failing)
        if (PersistenceItfModule::GetInstance()->MarkStepFinished(nTaskId, strStepName,
                                                                  jobExecInfos)) {
            OrchestratorClientFactory::GetOrchestratorClient(
                        *PersistenceItfModule::GetInstance()->GetDBProvider())->NotifyEventsAvailable();
        }
    } else {
        Logger::error(
            QString("Invalid job name for starting execution message %1").arg(strJobName));
    }
}

void AbstractResourceManagerItf::FillEndStepAddExecInfos(const QString &, ProcessorExecutionInfos &)
{
}

void AbstractResourceManagerItf::HandleStepInfosLogMsg(RequestParamsExecutionInfos *pReqParams)
{
    // just send the information to the Logger
    Logger::info(
        QString("JOB_NAME: %1, MSG: %2").arg(pReqParams->GetJobName(), pReqParams->GetLogMsg()));
}

QString AbstractResourceManagerItf::BuildStepExecutionName(int nTaskId, QString &stepName, int idx)
{
    return QString("TSKID_%1_STEPNAME_%2_%3")
        .arg(QString::number(nTaskId), stepName, QString::number(idx));
}

bool AbstractResourceManagerItf::ParseStepExecutionName(const QString &jobName,
                                       int &nTaskId,
                                       QString &strStepName,
                                       int &nStepIdx)
{
    // The expected format is "TSKID_%1_STEPNAME_%2_%3"
    QStringList list = jobName.split('_');
    int listSize = list.size();
    if (listSize >= 5) {
        if (list.at(0) == "TSKID" && list.at(2) == "STEPNAME") {
            bool bTaskOk = false;
            bool bStepOk = false;
            nTaskId = list.at(1).toInt(&bTaskOk);
            nStepIdx = list.at(list.size() - 1).toInt(&bStepOk);
            if (bTaskOk && bStepOk) {
                strStepName = list.at(3);
                if (listSize > 5) {
                    // we might have the step name built from several items separated by _
                    // compute the number of additional components
                    int nStepNameComponentsCnt = list.size() - 5;

                    for (int i = 0; i < nStepNameComponentsCnt; i++) {
                        strStepName.append('_');
                        strStepName.append(list.at(4 + i));
                    }
                }
                return true;
            }
        }
    }
    return false;
}

QString AbstractResourceManagerItf::BuildParamsString(QStringList &listParams)
{
    QString retStr;
    for (QStringList::iterator it = listParams.begin(); it != listParams.end(); ++it) {
        retStr.append(*it);
        retStr.append(" ");
    }
    return retStr;
}
