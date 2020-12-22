#include "httpserver.h"
#include "qtwebapphttplistener.h"
#include "qtwebapphttprequestmapper.hpp"

HttpServer::HttpServer(PersistenceManagerDBProvider &persistenceManager, const QString &httpSrvCfgPrefix,
                       QObject *)
: persistenceManager(persistenceManager), m_httpSrvCfgPrefix(httpSrvCfgPrefix), m_pListener(nullptr)
{
    m_pRequestMapper = new QtWebAppRequestMapper();
    if (!m_httpSrvCfgPrefix.endsWith(".")) {
        m_httpSrvCfgPrefix += ".";
    }
    const auto &params =
        persistenceManager.GetConfigurationParameters(m_httpSrvCfgPrefix);

    std::experimental::optional<int> port;
    QString listenPortKey = m_httpSrvCfgPrefix + "listen-port";
    for (const auto &p : params) {
        if (!p.siteId) {
            if (p.key == listenPortKey) {
                port = p.value.toInt();
            }
        }
    }

    if (!port) {
        throw std::runtime_error("Please configure the \"" + listenPortKey.toStdString() + "\" parameter "
                                 "with the listening port");
    }
    m_listenerSettings.setValue(QStringLiteral("port"), *port);
}

HttpServer::~HttpServer() {
    if (m_pListener) {
        delete m_pListener;
    }
}

void HttpServer::start()
{
    m_pListener = new QtWebAppHttpListener(&m_listenerSettings, (QtWebAppRequestMapper*)m_pRequestMapper);
}


void HttpServer::addController(const QString &pathPrefix,
                             AbstractHttpController *pController)
{
    m_pRequestMapper->addController(pathPrefix, pController);
}
