#include "dbusorchestratorclient.h"

DBusOrchestratorClient::DBusOrchestratorClient()
    : orchestrator(OrgEsaSen2agriOrchestratorInterface::staticInterfaceName(),
                   QStringLiteral("/org/esa/sen2agri/orchestrator"),
                   QDBusConnection::systemBus())
{

}

void DBusOrchestratorClient::NotifyEventsAvailable()
{
    orchestrator.NotifyEventsAvailable();
}
