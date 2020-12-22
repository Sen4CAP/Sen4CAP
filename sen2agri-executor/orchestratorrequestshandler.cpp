#include <QDebug>

#include "orchestratorrequestshandler.h"
#include "resourcemanager/resourcemanagerfactory.h"
#include "requestparamssubmitsteps.h"
#include "requestparamscanceltasks.h"
#include "requestparamsjobops.h"

void OrchestratorRequestsHandler::SubmitJob(int jobId)
{
    ResourceManagerFactory::GetResourceManager()->PerformJobOperation(
                new RequestParamsJobOps(START_JOB_REQ, jobId));
}

void OrchestratorRequestsHandler::CancelJob(int jobId)
{
    ResourceManagerFactory::GetResourceManager()->PerformJobOperation(
                new RequestParamsJobOps(CANCEL_JOB_REQ, jobId));
}

void OrchestratorRequestsHandler::PauseJob(int jobId)
{
    ResourceManagerFactory::GetResourceManager()->PerformJobOperation(
                new RequestParamsJobOps(PAUSE_JOB_REQ, jobId));
}

void OrchestratorRequestsHandler::ResumeJob(int jobId)
{
    ResourceManagerFactory::GetResourceManager()->PerformJobOperation(
                new RequestParamsJobOps(RESUME_JOB_REQ, jobId));
}

void OrchestratorRequestsHandler::SubmitSteps(const NewExecutorStepList &steps)
{
    QList<NewExecutorStep>::const_iterator stepIt;
    QList<StepArgument>::const_iterator stepArgIt;

    RequestParamsSubmitSteps *pReqParams = new RequestParamsSubmitSteps();

    for (stepIt = steps.begin(); stepIt != steps.end(); stepIt++) {
        ExecutionStep& execStep = pReqParams->AddExecutionStep((*stepIt).processorId, (*stepIt).taskId,
                                                               (*stepIt).stepName, (*stepIt).processorPath);
        for (stepArgIt = (*stepIt).arguments.begin(); stepArgIt != (*stepIt).arguments.end(); stepArgIt++) {
            execStep.AddArgument((*stepArgIt).value);
        }
    }
    ResourceManagerFactory::GetResourceManager()->StartExecutionSteps(pReqParams);
}

void OrchestratorRequestsHandler::CancelTasks(const TaskIdList &tasks)
{
    RequestParamsCancelTasks *pReq = new RequestParamsCancelTasks();
    QList<int> taskIds;
    QList<int>::const_iterator idIt;
    for (idIt = tasks.begin(); idIt != tasks.end(); idIt++) {
        taskIds.append(*idIt);
    }
    pReq->SetTaskIdsToCancel(taskIds);
    ResourceManagerFactory::GetResourceManager()->CancelTasks(pReq);
}

