#ifndef IPROCESSORWRAPPERMSGSLISTENER_H
#define IPROCESSORWRAPPERMSGSLISTENER_H

#include "requestparamsbase.h"

class IProcessorWrapperMsgsListener
{
public:
    IProcessorWrapperMsgsListener() {}
    virtual ~IProcessorWrapperMsgsListener(){}

    virtual void OnStepFeedbackNewMsg(RequestParamsBase *pReq) = 0;
};

#endif // IPROCESSORWRAPPERMSGSLISTENER_H
