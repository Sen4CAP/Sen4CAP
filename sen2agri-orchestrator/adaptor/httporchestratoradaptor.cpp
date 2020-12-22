#include "httporchestratoradaptor.h"

HttpOrchestratorAdaptor::HttpOrchestratorAdaptor(PersistenceManagerDBProvider &persistenceMng, Orchestrator *orchestrator)
    : persistenceManager(persistenceMng),
      httpServer(persistenceManager, "orchestrator.http-server"),
      orchestratorController(orchestrator)
{
    httpServer.addController("/orchestrator/", &orchestratorController);
    httpServer.start();
}
