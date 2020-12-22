#pragma once

#include "requestmapperbase.h"
#include <httpserver/httprequesthandler.h>
#include "qtwebapphttprequest.h"
#include "qtwebapphttpresponse.h"

using namespace stefanfrings;

class QtWebAppRequestMapper : public HttpRequestHandler, public RequestMapperBase
{
    Q_OBJECT
    Q_DISABLE_COPY(QtWebAppRequestMapper)

public:
    QtWebAppRequestMapper(QObject *parent = 0);

    void service(HttpRequest &request, HttpResponse &response);

};
