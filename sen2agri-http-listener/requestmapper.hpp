#pragma once

#include <httpserver/httprequesthandler.h>

#include "persistencemanager.hpp"

using namespace stefanfrings;

class RequestMapper : public HttpRequestHandler
{
    Q_OBJECT
    Q_DISABLE_COPY(RequestMapper)

    PersistenceManagerDBProvider &persistenceManager;

public:
    RequestMapper(PersistenceManagerDBProvider &persistenceManager,
                  QObject *parent = 0);

    void service(HttpRequest &request, HttpResponse &response);
};
