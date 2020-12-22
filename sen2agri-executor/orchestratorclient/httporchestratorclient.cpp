#include "httporchestratorclient.h"

#include <QNetworkReply>

HttpOrchestratorClient::HttpOrchestratorClient(PersistenceManagerDBProvider &persistenceMng, QObject *parent)
    : QObject(parent),
      persistenceManager(persistenceMng)
{
    QString orchestratorPrefix("orchestrator.http-server.");
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
    serviceUrl += "/orchestrator/";
}

void HttpOrchestratorClient::NotifyEventsAvailable()
{
    PostRequest(QStringLiteral("NotifyEventsAvailable"), QByteArray());
}

QByteArray HttpOrchestratorClient::PostRequest(const QString &fnc, const QByteArray &payload)
{
    QNetworkRequest request(serviceUrl + fnc);
    request.setHeader(QNetworkRequest::ContentTypeHeader, QStringLiteral("application/json"));
    auto reply = networkAccessManager.post(request, payload);

    QEventLoop eventLoop;
    connect(reply, SIGNAL(finished()), &eventLoop, SLOT(quit()));
    eventLoop.exec();
    if (reply->error() != QNetworkReply::NoError)
    {
        qDebug() << "Network error: " << reply->error();
        throw std::runtime_error(QStringLiteral("Network error while invoking orchestrator function %1: %2 ")
                                 .arg(fnc)
                                 .arg(reply->error())
                                 .toStdString());
    }
    else
    {
        reply->deleteLater();
        return reply->readAll();
    }
}
