#ifndef HTTPORCHESTRATORPROXY_H
#define HTTPORCHESTRATORPROXY_H

#include <QNetworkAccessManager>

#include "model.hpp"
#include "orchestratorproxy.hpp"
#include "persistencemanager.hpp"


class HttpOrchestratorProxy : public QObject, public OrchestratorProxy
{
    Q_OBJECT

    QNetworkAccessManager networkAccessManager;
    QString serviceUrl;
    PersistenceManagerDBProvider &persistenceManager;

public:
    HttpOrchestratorProxy(PersistenceManagerDBProvider &persistenceMng, QObject *parent = 0);
    virtual ~HttpOrchestratorProxy();
    virtual JobDefinition GetJobDefinition(const ProcessingRequest &procRequest);
    virtual void SubmitJob(const JobDefinition &job);

private slots:
    QByteArray PostRequest(const QString &fnc, const QByteArray &payload);
};

#endif // HTTPORCHESTRATORPROXY_H
