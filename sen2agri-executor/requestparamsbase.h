#ifndef REQUESTPARAMSBASE_H
#define REQUESTPARAMSBASE_H


typedef enum RequestType
{
    STEP_EXECUTION_INFO_MSG = 1,
    START_STEP_REQ = 2,
    STOP_STEP_REQ = 3,
    START_JOB_REQ = 4,
    CANCEL_JOB_REQ = 5,
    PAUSE_JOB_REQ = 6,
    RESUME_JOB_REQ = 7,
    REQUEST_UNKNOWN = 256
} RequestType;


class RequestParamsBase
{
public:
    RequestParamsBase();
    RequestParamsBase(RequestType reqType);
    RequestType GetRequestType();

protected:
    RequestType m_ReqType;
};

#endif // REQUESTPARAMSBASE_H
