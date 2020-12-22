#include <QByteArray>

#include "stopwatch.hpp"
#include "qtwebapphttprequestmapper.hpp"
#include "qtwebapphttprequest.h"
#include "qtwebapphttpresponse.h"

QtWebAppRequestMapper::QtWebAppRequestMapper(QObject *)
{
}

void QtWebAppRequestMapper::service(HttpRequest &request, HttpResponse &response)
{
    START_STOPWATCH("QtWebAppRequestMapper::service");

    const auto &path = request.getPath();

    QtWebAppHttpRequest req(request);
    QtWebAppHttpResponse resp(response);

    for(auto pathPrefix : m_mapControllers.keys())
    {
        if (path.startsWith(pathPrefix.toUtf8())) {
            m_mapControllers.value(pathPrefix)->service(req, resp);
            return;
        }
    }
    response.setStatus(400, "Bad Request");
}
