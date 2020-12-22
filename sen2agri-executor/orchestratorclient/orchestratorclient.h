#pragma once


class OrchestratorClient
{
public:
    OrchestratorClient();

    virtual void NotifyEventsAvailable() = 0;
};
