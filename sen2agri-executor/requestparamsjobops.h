#ifndef REQUESTPARAMSJOBOPS_H
#define REQUESTPARAMSJOBOPS_H

#include "requestparamsbase.h"

class RequestParamsJobOps : public RequestParamsBase
{
public:
    RequestParamsJobOps() = delete;
    RequestParamsJobOps(RequestType reqType);
    RequestParamsJobOps(RequestType reqType, int jobId);

    void SetJobId(int jobId);
    int GetJobId();

private:
    int jobId;
};

#endif // REQUESTPARAMSJOBOPS_H
