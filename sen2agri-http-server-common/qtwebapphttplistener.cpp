#include "qtwebapphttplistener.h"

QtWebAppHttpListener::QtWebAppHttpListener(const QSettings* settings, HttpRequestHandler* requestHandler, QObject* parent)
    : HttpListener(settings, requestHandler, parent)
{
}

void QtWebAppHttpListener::listen()
{
    ((HttpListener*)this)->listen();
}

void QtWebAppHttpListener::close()
{
    ((HttpListener*)this)->close();
}
