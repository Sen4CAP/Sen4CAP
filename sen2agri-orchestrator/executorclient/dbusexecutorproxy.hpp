#pragma once

#include <memory>
#include "model.hpp"

#include "executorproxy.hpp"
#include "processorsexecutor_interface.h"

class DBusExecutorProxy : public ExecutorProxy
{
    OrgEsaSen2agriProcessorsExecutorInterface executorClient;

public:
    DBusExecutorProxy();
    virtual ~DBusExecutorProxy();

    virtual void SubmitJob(int jobId);
    virtual void CancelJob(int jobId);
    virtual void PauseJob(int jobId);
    virtual void ResumeJob(int jobId);

    virtual void SubmitSteps(NewExecutorStepList steps);
    virtual void CancelTasks(TaskIdList tasks);
};
