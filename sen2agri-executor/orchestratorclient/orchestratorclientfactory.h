#ifndef ORCHESTRATORCLIENTFACTORY_H
#define ORCHESTRATORCLIENTFACTORY_H

#include <memory>
#include <mutex>

#include "orchestratorclient.h"
#include "persistencemanager.hpp"

class OrchestratorClientFactory
{
public:
    OrchestratorClientFactory();

    static OrchestratorClient *GetOrchestratorClient(PersistenceManagerDBProvider &persistenceManager);
private:
    static std::unique_ptr<OrchestratorClient> m_orchestratorClient;
    static std::once_flag m_onceFlag;
};

#endif // ORCHESTRATORCLIENTFACTORY_H
