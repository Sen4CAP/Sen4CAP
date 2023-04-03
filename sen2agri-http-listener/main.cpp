#include <QCoreApplication>

#include <httpserver/httplistener.h>

#include "logger.hpp"
#include "requestmapper.hpp"
#include "dbus_future_utils.hpp"
#include "persistencemanager.hpp"
#include "settings.hpp"
#include "configuration.hpp"

int main(int argc, char *argv[])
{
    try {
        Logger::installMessageHandler();

        QCoreApplication app(argc, argv);

        registerMetaTypes();

        PersistenceManagerDBProvider persistenceManager(
            Settings::readSettings(getConfigurationFile(app)));

        const auto &params =
            persistenceManager.GetConfigurationParameters(QStringLiteral("http-listener."));

        std::experimental::optional<int> port;

        for (const auto &p : params) {
            if (!p.siteId) {
                if (p.key == "http-listener.listen-port") {
                    port = p.value.toInt();
                }
            }
        }

        if (!port) {
            throw std::runtime_error("Please configure the \"http-listener.listen-port\" parameter "
                                     "with the listening port");
        }

        RequestMapper mapper(persistenceManager);

        QSettings listenerSettings;
        listenerSettings.setValue(QStringLiteral("port"), *port);
        HttpListener listener(&listenerSettings, &mapper);

        Q_UNUSED(listener);

        return app.exec();
    } catch (const std::exception &e) {
        Logger::fatal(e.what());

        return EXIT_FAILURE;
    }
}
