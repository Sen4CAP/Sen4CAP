#include <QJsonDocument>

#include "orchestratorcontroller.hpp"
#include "stopwatch.hpp"
#include "logger.hpp"

OrchestratorController::OrchestratorController(Orchestrator *pOrchestrator, QObject *parent)
    : QObject(parent), m_pOrchestrator(pOrchestrator)
{
}

void OrchestratorController::service(AbstractHttpRequest &request, AbstractHttpResponse &response)
{
    Stopwatch sw(__func__);
    Q_UNUSED(sw);

    try {
        const auto &path = request.getPath();
        const auto &method = request.getMethod();
        const auto &action = path.mid(path.indexOf('/', 1) + 1);

        if (method == "GET") {
            response.setStatus(400, "Bad Request");
        } else if (method == "POST") {
            if (action == "GetJobDefinition") {
                GetJobDefinition(request, response);
            } else if (action == "NotifyEventsAvailable") {
                NotifyEventsAvailable(request, response);
            } else  if (action == "SubmitJob") {
                 SubmitJob(request, response);
            } else {
                response.setStatus(400, "Bad Request");
            }
        }
    } catch (const std::exception &e) {
        response.setStatus(500, "Internal Server Error");

        Logger::error(e.what());
    }
}

void OrchestratorController::NotifyEventsAvailable(AbstractHttpRequest &, AbstractHttpResponse &)
{
     m_pOrchestrator->NotifyEventsAvailable();
}
void OrchestratorController::GetJobDefinition(AbstractHttpRequest &request, AbstractHttpResponse &response)
{
    const ProcessingRequest &procReq = ProcessingRequest::fromJson(request.getBody());
    const JobDefinition &jobDef = m_pOrchestrator->GetJobDefinition(procReq);

    response.write(jobDef.toJson().toUtf8());
}

void OrchestratorController::SubmitJob(AbstractHttpRequest &request, AbstractHttpResponse &)
{
    const JobDefinition &jobDef = JobDefinition::fromJson(request.getBody());
    m_pOrchestrator->SubmitJob(jobDef);
}
