#include "resourcemanagerfactory.h"
#include "persistenceitfmodule.h"
#include "resourcemanageritf_slurm.h"
#include "resourcemanageritf_tao.h"
#include "configurationmgr.h"

std::unique_ptr<AbstractResourceManagerItf> ResourceManagerFactory::m_resMngInstance;
std::once_flag ResourceManagerFactory::m_onceFlag;

ResourceManagerFactory::ResourceManagerFactory()
{
}

AbstractResourceManagerItf* ResourceManagerFactory::GetResourceManager()
{
    if (!m_resMngInstance) {
        QString resMngName;
        ConfigurationMgr::GetInstance()->GetValue("executor.resource-manager.name", resMngName);

        std::call_once(m_onceFlag, [resMngName] {
            if (resMngName == "tao") {
                m_resMngInstance.reset(new ResourceManagerItf_TAO());
            } else {
                m_resMngInstance.reset(new ResourceManagerItf_SLURM());
            }
        });
    }
    return m_resMngInstance.get();
}
