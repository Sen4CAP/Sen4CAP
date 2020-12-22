#ifndef HTTPORCHESTRATORADAPTOR_H
#define HTTPORCHESTRATORADAPTOR_H

#include "persistencemanager.hpp"
#include "httpserver.h"
#include "http/controller/orchestratorcontroller.hpp"

class HttpOrchestratorAdaptor
{
    PersistenceManagerDBProvider &persistenceManager;
    HttpServer httpServer;
    OrchestratorController orchestratorController;

public:
    HttpOrchestratorAdaptor(PersistenceManagerDBProvider &persistenceMng, Orchestrator *orchestrator);
};

#endif // HTTPORCHESTRATORADAPTOR_H
