#ifndef HTTPSERVER_H
#define HTTPSERVER_H

#include "abstracthttpcontroller.h"
#include "abstracthttplistener.h"
#include "requestmapperbase.h"

#include "persistencemanager.hpp"

class HttpServer: public QObject
{
    Q_OBJECT
    Q_DISABLE_COPY(HttpServer)

    PersistenceManagerDBProvider &persistenceManager;

public:
    HttpServer(PersistenceManagerDBProvider &persistenceManager, const QString &httpSrvCfgPrefix,
                  QObject *parent = 0);
    ~HttpServer();

    void start();
    void addController(const QString &pathPrefix, AbstractHttpController *pController);

protected :
    QMap<QString, AbstractHttpController*> m_mapControllers;
    QString m_httpSrvCfgPrefix;
    AbstractHttpListener *m_pListener;
    RequestMapperBase *m_pRequestMapper;
    QSettings m_listenerSettings;

};

#endif // HTTPSERVER_H
