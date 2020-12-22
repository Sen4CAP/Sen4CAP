#ifndef EXECUTORPROXYFACTORY_H
#define EXECUTORPROXYFACTORY_H

#include "executorproxy.hpp"
#include <memory>
#include "persistencemanager.hpp"

class ExecutorProxyFactory
{

public:
    ~ExecutorProxyFactory();

    static std::unique_ptr<ExecutorProxy> GetExecutorClient(QObject *pParent, PersistenceManagerDBProvider &persistenceManager);
private :
    ExecutorProxyFactory();
};

#endif // EXECUTORPROXYFACTORY_H
