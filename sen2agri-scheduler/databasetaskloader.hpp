#pragma once

#include "taskloader.hpp"
#include "persistencemanager.hpp"

class DatabaseTaskLoader : public TaskLoader
{
    PersistenceManagerDBProvider &persistenceManager;

public:
    DatabaseTaskLoader(PersistenceManagerDBProvider &persistenceMng);
    virtual ~DatabaseTaskLoader();
    virtual std::vector<ScheduledTask> LoadFromDatabase( );
    virtual void UpdateStatusinDatabase( const std::vector<ScheduledTask>& );
};
