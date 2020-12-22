#include "dbusorchestratoradaptor.h"

DBusOrchestratorAdaptor::DBusOrchestratorAdaptor(Orchestrator *orchestrator, QObject *parent)
    : QObject(parent), m_dbusConnection(QDBusConnection::systemBus())
{
    new OrchestratorAdaptor(orchestrator);
    m_dbusConnection = QDBusConnection::systemBus();
    if (!m_dbusConnection.registerObject(QStringLiteral("/org/esa/sen2agri/orchestrator"),
                                   orchestrator)) {
        throw std::runtime_error(
            QStringLiteral("Error registering the object with D-Bus: %1, exiting.")
                .arg(m_dbusConnection.lastError().message())
                .toStdString());
    }

    if (!m_dbusConnection.registerService(QStringLiteral("org.esa.sen2agri.orchestrator"))) {
        throw std::runtime_error(
            QStringLiteral("Error registering the service with D-Bus: %1, exiting.")
                .arg(m_dbusConnection.lastError().message())
                .toStdString());
    }
}
