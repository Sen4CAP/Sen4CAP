#pragma once

#include <QDBusConnection>
#include "orchestratorrequestshandler.h"

class DBusExecutorAdaptor : public QObject
{
    Q_OBJECT
    QDBusConnection m_dbusConnection;
public:
    DBusExecutorAdaptor(OrchestratorRequestsHandler *orchestratorReqHandler, QObject *parent = 0);
};
