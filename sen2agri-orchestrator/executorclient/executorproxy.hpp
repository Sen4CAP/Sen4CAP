#pragma once

#include "model.hpp"

class ExecutorProxy
{
public:
    virtual ~ExecutorProxy();

    virtual void SubmitJob(int jobId) = 0;
    virtual void CancelJob(int jobId) = 0;
    virtual void PauseJob(int jobId) = 0;
    virtual void ResumeJob(int jobId) = 0;

    virtual void SubmitSteps(NewExecutorStepList steps) = 0;
    virtual void CancelTasks(TaskIdList tasks) = 0;
};

