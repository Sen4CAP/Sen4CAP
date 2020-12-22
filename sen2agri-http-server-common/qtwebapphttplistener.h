#ifndef QTWEBAPPHTTPLISTENER_H
#define QTWEBAPPHTTPLISTENER_H

#include <httpserver/httplistener.h>
#include "abstracthttplistener.h"

using namespace stefanfrings;

class QtWebAppHttpListener : public HttpListener, public AbstractHttpListener
{
    Q_OBJECT
    Q_DISABLE_COPY(QtWebAppHttpListener)

public:
    QtWebAppHttpListener(const QSettings* settings, HttpRequestHandler* requestHandler, QObject* parent=nullptr);

    void listen();
    void close();
};

#endif // QTWEBAPPHTTPLISTENER_H
