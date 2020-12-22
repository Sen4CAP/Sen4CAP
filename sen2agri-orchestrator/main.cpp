#include <stdexcept>

#include <QCoreApplication>
#include <QDBusConnection>

#include "make_unique.hpp"
#include "model.hpp"
#include "logger.hpp"

#include "orchestrator.hpp"
#include "adaptor/dbusorchestratoradaptor.h"
#include "adaptor/httporchestratoradaptor.h"

#include "configuration.hpp"

#define RESCAN_EVENTS_TIMEOUT   10000

int main(int argc, char *argv[])
{
    try {
        Logger::installMessageHandler();

        QCoreApplication app(argc, argv);

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

        Orchestrator orchestrator;
        Timer timer(&orchestrator);
        timer.start(RESCAN_EVENTS_TIMEOUT);

        if (interProcCommType == "http") {
            new HttpOrchestratorAdaptor(persistenceManager, &orchestrator);
        } else {
            new DBusOrchestratorAdaptor(&orchestrator);
        }

        return app.exec();
    } catch (const std::exception &e) {
        Logger::fatal(e.what());

        return EXIT_FAILURE;
    }
}
