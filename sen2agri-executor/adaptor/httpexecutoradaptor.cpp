#include "httpexecutoradaptor.h"

HttpExecutorAdaptor::HttpExecutorAdaptor(PersistenceManagerDBProvider &persistenceMng,
                                           OrchestratorRequestsHandler *orchestratorReqHandler)
    : persistenceManager(persistenceMng),
      httpServer(persistenceManager, "executor.http-server"),
      executorController(orchestratorReqHandler)
{
    httpServer.addController("/executor/", &executorController);
    httpServer.start();
}
