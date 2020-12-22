#include <QJsonDocument>

#include "executorcontroller.hpp"
#include "stopwatch.hpp"
#include "logger.hpp"

ExecutorController::ExecutorController(OrchestratorRequestsHandler *pResHandler, QObject *parent)
    : QObject(parent), m_pResHandler(pResHandler)
{
}

void ExecutorController::service(AbstractHttpRequest &request, AbstractHttpResponse &response)
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
            if (action == "SubmitJob") {
                SubmitJob(request, response);
            } else if (action == "CancelJob") {
                CancelJob(request, response);
            } else if (action == "PauseJob") {
                PauseJob(request, response);
            } else if (action == "ResumeJob") {
                ResumeJob(request, response);
            } else if (action == "SubmitSteps") {
                SubmitSteps(request, response);
            } else  if (action == "CancelTasks") {
                 CancelTasks(request, response);
            } else {
                response.setStatus(400, "Bad Request");
            }
        }
    } catch (const std::exception &e) {
        response.setStatus(500, "Internal Server Error");

        Logger::error(e.what());
    }
}

void ExecutorController::SubmitJob(AbstractHttpRequest &request, AbstractHttpResponse &)
{
    int jobId = GetJobId(request);
    if (jobId != -1) {
        m_pResHandler->SubmitJob(jobId);
    }

}

void ExecutorController::CancelJob(AbstractHttpRequest &request, AbstractHttpResponse &)
{
    int jobId = GetJobId(request);
    if (jobId != -1) {
        m_pResHandler->CancelJob(jobId);
    }
}

void ExecutorController::PauseJob(AbstractHttpRequest &request, AbstractHttpResponse &)
{
    int jobId = GetJobId(request);
    if (jobId != -1) {
        m_pResHandler->PauseJob(jobId);
    }
}

void ExecutorController::ResumeJob(AbstractHttpRequest &request, AbstractHttpResponse &)
{
    int jobId = GetJobId(request);
    if (jobId != -1) {
        m_pResHandler->ResumeJob(jobId);
    }
}

void ExecutorController::SubmitSteps(AbstractHttpRequest &request, AbstractHttpResponse &)
{
    const auto &doc = QJsonDocument::fromJson(request.getBody());
    const auto &arr = doc.array();
    NewExecutorStepList steps;
    for (auto obj: arr) {
        steps.append(NewExecutorStep::fromJson(obj.toString()));
    }
    m_pResHandler->SubmitSteps(steps);
}

void ExecutorController::CancelTasks(AbstractHttpRequest &request, AbstractHttpResponse &)
{
    const auto &doc = QJsonDocument::fromJson(request.getBody());
    const auto &arr = doc.array();
    QList<int> taskIds;
    for (auto obj: arr) {
        taskIds.append(obj.toInt());
    }
    m_pResHandler->CancelTasks(taskIds);
}

int ExecutorController::GetJobId(AbstractHttpRequest &request)
{
    const auto &doc = QJsonDocument::fromJson(request.getBody());
    const QJsonObject &obj = doc.object();
    if (obj.contains("jobId")) {
        return obj["jobId"].toInt();
    }
    return -1;
}
