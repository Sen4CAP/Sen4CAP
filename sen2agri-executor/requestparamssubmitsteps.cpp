#include "requestparamssubmitsteps.h"

ExecutionStep::ExecutionStep()
{
}

ExecutionStep::ExecutionStep(int nProcessorId, int nTaskId, const QString &strName,
                             const QString &strProcPath)
{
    m_nProcessorId = nProcessorId;
    m_nTaskId = nTaskId;
    m_strStepName = strName;
    m_strProcessorPath = strProcPath;
}

ExecutionStep::~ExecutionStep()
{
}

void ExecutionStep::AddArgument(const QString &strArg)
{
    m_listArgs.push_back(strArg);
}

int ExecutionStep::GetProcessorId()
{
    return m_nProcessorId;
}

int ExecutionStep::GetTaskId()
{
    return m_nTaskId;
}

QString& ExecutionStep::GetStepName()
{
    return m_strStepName;
}

QString& ExecutionStep::GetProcessorPath()
{
    return m_strProcessorPath;
}

QStringList& ExecutionStep::GetArgumentsList()
{
    return m_listArgs;
}

ExecutionStep& ExecutionStep::operator=(const ExecutionStep &rhs)
{
    // Check for self-assignment!
    if (this == &rhs)      // Same object?
      return *this;

    m_nTaskId = rhs.m_nTaskId;
    m_strStepName = rhs.m_strStepName;
    m_nProcessorId = rhs.m_nProcessorId;
    m_strProcessorPath = rhs.m_strProcessorPath;
    m_listArgs = rhs.m_listArgs;
    return *this;
}

RequestParamsSubmitSteps::RequestParamsSubmitSteps()
    : RequestParamsBase(START_STEP_REQ)
{
}

ExecutionStep& RequestParamsSubmitSteps::AddExecutionStep(int nProcessorId, int nTaskId,
                                                          const QString &strStepName,
                                                          const QString &strProcPath)
{
    ExecutionStep execStep(nProcessorId, nTaskId, strStepName, strProcPath);
    m_executionSteps.append(execStep);
    return m_executionSteps.last();
}

QList<ExecutionStep> &RequestParamsSubmitSteps::GetExecutionSteps()
{
    return m_executionSteps;
}
