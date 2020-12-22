#include "dbusexecutoradaptor.h"

#include "processorsexecutor_adaptor.h"

#define SERVICE_NAME "org.esa.sen2agri.processorsExecutor"

DBusExecutorAdaptor::DBusExecutorAdaptor(OrchestratorRequestsHandler *orchestratorReqHandler, QObject *parent)
    : QObject(parent), m_dbusConnection(QDBusConnection::systemBus())
{
    new ProcessorsExecutorAdaptor(orchestratorReqHandler);
    m_dbusConnection = QDBusConnection::systemBus();

    if (!m_dbusConnection.registerObject("/org/esa/sen2agri/processorsExecutor",
                                   orchestratorReqHandler)) {
        QString str = QString("Error registering the object with D-Bus: %1, exiting.")
                          .arg(m_dbusConnection.lastError().message());

        throw std::runtime_error(str.toStdString());
    }

    if (!m_dbusConnection.registerService(SERVICE_NAME)) {
        QString str = QString("Error registering the object with D-Bus: %1, exiting.")
                          .arg(m_dbusConnection.lastError().message());

        throw std::runtime_error(str.toStdString());
    }
}
