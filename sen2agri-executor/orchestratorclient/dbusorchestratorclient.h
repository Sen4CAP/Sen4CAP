#pragma once

#include "orchestrator_interface.h"
#include "orchestratorclient.h"

class DBusOrchestratorClient  : public OrchestratorClient
{
    OrgEsaSen2agriOrchestratorInterface orchestrator;

public:
    DBusOrchestratorClient();

    virtual void NotifyEventsAvailable();
};

