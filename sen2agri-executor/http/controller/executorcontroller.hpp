#pragma once

#include "abstracthttpcontroller.h"
#include "orchestratorrequestshandler.h"
#include <QJsonArray>

class ExecutorController :  public QObject, public AbstractHttpController
{
    Q_OBJECT
    Q_DISABLE_COPY(ExecutorController)

public:
    explicit ExecutorController(OrchestratorRequestsHandler *pResHandler, QObject *parent = 0);

    virtual void service(AbstractHttpRequest &request, AbstractHttpResponse &response);

private:
    void SubmitJob(AbstractHttpRequest &request, AbstractHttpResponse &response);
    void CancelJob(AbstractHttpRequest &request, AbstractHttpResponse &response);
    void PauseJob(AbstractHttpRequest &request, AbstractHttpResponse &response);
    void ResumeJob(AbstractHttpRequest &request, AbstractHttpResponse &response);
    void SubmitSteps(AbstractHttpRequest &request, AbstractHttpResponse &response);
    void CancelTasks(AbstractHttpRequest &request, AbstractHttpResponse &response);

    int GetJobId(AbstractHttpRequest &request);

    OrchestratorRequestsHandler *m_pResHandler;
};
