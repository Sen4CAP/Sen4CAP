#include <QCoreApplication>

#include "orchestrator_interface.h"
#include "httporchestratorproxy.hpp"
#include "dbusorchestratorproxy.hpp"
#include "databasetaskloader.hpp"
#include "schedulerapp.hpp"
#include "configuration.hpp"

int main(int argc, char *argv[])
{
    QCoreApplication a(argc, argv);

    registerMetaTypes();

    PersistenceManagerDBProvider persistenceManager(
                Settings::readSettings(
                    getConfigurationFile(*QCoreApplication::instance())));

    QString interProcCommType;
    const auto &params =
        persistenceManager.GetConfigurationParameters("general.inter-proc-com-type");
    for (const auto &p : params) {
        if (!p.siteId) {
            interProcCommType = p.value;
        }
    }
    OrchestratorProxy *orchestrator;
    if (interProcCommType == "http") {
        orchestrator = new HttpOrchestratorProxy(persistenceManager);
    } else  {
        orchestrator = new DBusOrchestratorProxy();
    }
    DatabaseTaskLoader loader(persistenceManager);
    SchedulerApp sapp(&loader, orchestrator);
    sapp.StartRunning();

    return a.exec();
}
