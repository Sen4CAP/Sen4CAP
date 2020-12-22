#include "requestparamsjobops.h"

RequestParamsJobOps::RequestParamsJobOps(RequestType reqType)
    : RequestParamsBase(reqType), jobId(-1)
{
}

RequestParamsJobOps::RequestParamsJobOps(RequestType reqType, int jobId)
    : RequestParamsBase(reqType), jobId(jobId)
{
}

void RequestParamsJobOps::SetJobId(int jobId)
{
    this->jobId = jobId;
}

int RequestParamsJobOps::GetJobId()
{
    return this->jobId;
}
