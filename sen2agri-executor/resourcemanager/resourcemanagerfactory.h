#ifndef RESOURCEMANAGERFACTORY_H
#define RESOURCEMANAGERFACTORY_H

#include <memory>
#include <mutex>
#include "abstractresourcemanageritf.h"

class ResourceManagerFactory
{
public:
    static AbstractResourceManagerItf *GetResourceManager();

private:
    ResourceManagerFactory();

    static std::unique_ptr<AbstractResourceManagerItf> m_resMngInstance;
    static std::once_flag m_onceFlag;
};

#endif // RESOURCEMANAGERFACTORY_H
