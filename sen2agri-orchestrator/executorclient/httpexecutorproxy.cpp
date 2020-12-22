#include "httpexecutorproxy.hpp"
#include <qeventloop.h>
#include <QNetworkReply>
#include <QJsonDocument>
#include <QJsonObject>
#include "logger.hpp"
#include "persistencemanager.hpp"
#include "configuration.hpp"

HttpExecutorProxy::HttpExecutorProxy(PersistenceManagerDBProvider &persistenceMng, QObject *parent)
    : QObject(parent),
      networkAccessManager(parent),
      persistenceManager(persistenceMng)
{
    QString orchestratorPrefix("executor.http-server.");
    const auto &params =
        persistenceManager.GetConfigurationParameters(orchestratorPrefix);

    int port = 0;
    QString ipAddr;
    const QString &listenPortKey = orchestratorPrefix + "listen-port";
    const QString &listenIpKey = orchestratorPrefix + "listen-ip";
    for (const auto &p : params) {
        if (!p.siteId) {
            if (p.key == listenPortKey) {
                port = p.value.toInt();
            } else if (p.key == listenIpKey) {
                ipAddr = p.value;
            }
        }
    }
    if (ipAddr.trimmed().size() == 0) {
        throw std::runtime_error(QStringLiteral("Configuration key  %1 does not exist or is invalid in database, exiting.")
                                     .arg(listenIpKey)
                                     .toStdString());
    }

    serviceUrl = "http://" + ipAddr;
    if (port > 0) {
        serviceUrl += (":" + QString::number(port));
    }
    serviceUrl += "/executor/";
}


HttpExecutorProxy::~HttpExecutorProxy()
{
}

void HttpExecutorProxy::SubmitJob(int jobId)
{
    PostRequest(QStringLiteral("SubmitJob"), GetJobIdJson(jobId));
}

void HttpExecutorProxy::CancelJob(int jobId)
{
    PostRequest(QStringLiteral("CancelJob"), GetJobIdJson(jobId));
}

void HttpExecutorProxy::PauseJob(int jobId)
{
    PostRequest(QStringLiteral("PauseJob"), GetJobIdJson(jobId));
}

void HttpExecutorProxy::ResumeJob(int jobId)
{
    PostRequest(QStringLiteral("ResumeJob"), GetJobIdJson(jobId));
}

void HttpExecutorProxy::SubmitSteps(NewExecutorStepList steps)
{
    PostRequest(QStringLiteral("SubmitSteps"), QListToJSon(steps).toUtf8());
}

void HttpExecutorProxy::CancelTasks(TaskIdList tasks)
{
    QJsonArray array;
    for (auto & item : tasks) { array.append(item);}
    PostRequest(QStringLiteral("CancelTasks"), QJsonDocument(array).toJson());
}

void HttpExecutorProxy::PostRequest(const QString &fnc, const QByteArray &payload)
{
    QNetworkRequest request(serviceUrl + fnc);
    request.setHeader(QNetworkRequest::ContentTypeHeader, QStringLiteral("application/json"));
    auto reply = networkAccessManager.post(request, payload);

    connect(reply, SIGNAL(finished()), this, SLOT(replyFinished()));
}

void HttpExecutorProxy::replyFinished() {
    auto reply = qobject_cast<QNetworkReply *>(sender());
    Q_ASSERT(reply);

    reply->deleteLater();

    if (reply->error() != QNetworkReply::NoError) {
        qDebug() << "Network error: " << reply->error();
        Logger::error(QStringLiteral("Network error while invoking executor function %1")
                                 .arg(reply->error()));
    } // else {
//        const auto &obj = reply->readAll();
//        qDebug() << "Received response: " << obj;
//        Logger::info(QStringLiteral("Received response from executor function %1")
//                                 .arg(QString(obj.toStdString().c_str())));
//
//    }
}

QByteArray HttpExecutorProxy::GetJobIdJson(int jobId) {
    QJsonObject obj;
    obj["jobId"] = jobId;
    return QJsonDocument(obj).toJson();
}
