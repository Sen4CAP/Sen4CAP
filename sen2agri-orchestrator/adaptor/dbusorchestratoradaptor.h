#ifndef DBUSORCHESTRATORADAPTOR_H
#define DBUSORCHESTRATORADAPTOR_H

#include <QDBusConnection>
#include "orchestrator_adaptor.h"
#include "orchestrator.hpp"

class DBusOrchestratorAdaptor : public QObject
{
    Q_OBJECT
    QDBusConnection m_dbusConnection;
public:
    DBusOrchestratorAdaptor(Orchestrator *orchestrator, QObject *parent = 0);
};

#endif // DBUSORCHESTRATORADAPTOR_H
