#include "executorproxyfactory.h"
#include "persistencemanager.hpp"
#include "configuration.hpp"
#include "executorclient/httpexecutorproxy.hpp"
#include "executorclient/dbusexecutorproxy.hpp"

ExecutorProxyFactory::ExecutorProxyFactory()
{
}

std::unique_ptr<ExecutorProxy> ExecutorProxyFactory::GetExecutorClient(QObject *pParent, PersistenceManagerDBProvider &persistenceManager) {
    QString interProcCommType;
    const auto &params =
        persistenceManager.GetConfigurationParameters("general.inter-proc-com-type");
    for (const auto &p : params) {
        if (!p.siteId) {
            interProcCommType = p.value;
        }
    }
    if (interProcCommType == "http") {
        std::unique_ptr<ExecutorProxy> proxy(new HttpExecutorProxy(persistenceManager, pParent));
        return proxy;
    }
    std::unique_ptr<ExecutorProxy> proxy(new DBusExecutorProxy());
    return proxy;
}

ExecutorProxyFactory::~ExecutorProxyFactory()
{
}
