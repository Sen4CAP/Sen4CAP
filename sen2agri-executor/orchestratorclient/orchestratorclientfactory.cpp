#include "orchestratorclientfactory.h"
#include "dbusorchestratorclient.h"
#include "httporchestratorclient.h"

std::unique_ptr<OrchestratorClient> OrchestratorClientFactory::m_orchestratorClient;
std::once_flag OrchestratorClientFactory::m_onceFlag;

OrchestratorClientFactory::OrchestratorClientFactory()
{

}

OrchestratorClient* OrchestratorClientFactory::GetOrchestratorClient(PersistenceManagerDBProvider &persistenceManager) {
    if (!m_orchestratorClient) {
        QString interProcCommType;
        const auto &params =
            persistenceManager.GetConfigurationParameters("general.inter-proc-com-type");
        for (const auto &p : params) {
            if (!p.siteId) {
                interProcCommType = p.value;
            }
        }

        std::call_once(m_onceFlag, [interProcCommType, &persistenceManager] {
            if (interProcCommType == "http") {
                m_orchestratorClient.reset(new HttpOrchestratorClient(persistenceManager));
            } else {
                m_orchestratorClient.reset(new DBusOrchestratorClient());
            }
        });
    }
    return m_orchestratorClient.get();
}
