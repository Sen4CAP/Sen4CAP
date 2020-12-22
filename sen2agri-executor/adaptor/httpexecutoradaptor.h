#pragma once

#include "httpserver.h"
#include "http/controller/executorcontroller.hpp"
#include "persistencemanager.hpp"

class HttpExecutorAdaptor
{
    PersistenceManagerDBProvider &persistenceManager;
    HttpServer httpServer;
    ExecutorController executorController;

public:
    HttpExecutorAdaptor(PersistenceManagerDBProvider &persistenceMng, OrchestratorRequestsHandler *orchestrator);
};
