#pragma once

#include "orchestratorclient.h"

#include <QNetworkAccessManager>
#include "persistencemanager.hpp"

class HttpOrchestratorClient : public QObject, public OrchestratorClient
{
    Q_OBJECT

    QNetworkAccessManager networkAccessManager;
    QString serviceUrl;
    PersistenceManagerDBProvider &persistenceManager;

public:
    HttpOrchestratorClient(PersistenceManagerDBProvider &persistenceMng, QObject *parent = 0);

    virtual void NotifyEventsAvailable();

private slots:
    QByteArray PostRequest(const QString &fnc, const QByteArray &payload);
};

