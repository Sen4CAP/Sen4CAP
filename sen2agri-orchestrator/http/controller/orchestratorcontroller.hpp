#pragma once

#include "abstracthttpcontroller.h"
#include "orchestrator.hpp"

class OrchestratorController :  public QObject, public AbstractHttpController
{
    Q_OBJECT
    Q_DISABLE_COPY(OrchestratorController)

public:
    explicit OrchestratorController(Orchestrator *pOrchestrator, QObject *parent = 0);

    virtual void service(AbstractHttpRequest &request, AbstractHttpResponse &response);

private:
    void NotifyEventsAvailable(AbstractHttpRequest &request, AbstractHttpResponse &response);
    void GetJobDefinition(AbstractHttpRequest &request, AbstractHttpResponse &response);
    void SubmitJob(AbstractHttpRequest &request, AbstractHttpResponse &response);

    Orchestrator *m_pOrchestrator;
};
