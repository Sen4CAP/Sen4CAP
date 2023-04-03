#include <QByteArray>

#include "stopwatch.hpp"
#include "requestmapper.hpp"
#include "controller/statisticscontroller.hpp"

RequestMapper::RequestMapper(PersistenceManagerDBProvider &persistenceManager,
                             QObject *parent)
    : HttpRequestHandler(parent),
      persistenceManager(persistenceManager)
{
}

void RequestMapper::service(HttpRequest &request, HttpResponse &response)
{
    START_STOPWATCH("RequestMapper::service");

    const auto &path = request.getPath();
    if (path.startsWith("/statistics/")) {
        StatisticsController(persistenceManager).service(request, response);
    }
}
