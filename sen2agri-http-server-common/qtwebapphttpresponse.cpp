#include "qtwebapphttpresponse.h"

QtWebAppHttpResponse::QtWebAppHttpResponse(HttpResponse &httpResponse)
    : m_httpResponse(httpResponse)
{

}

HttpResponse & QtWebAppHttpResponse::getResponse() {
    return m_httpResponse;
}

void QtWebAppHttpResponse::setHeader(const QByteArray name, const QByteArray value)
{
    m_httpResponse.setHeader(name, value);
}

void QtWebAppHttpResponse::setHeader(const QByteArray name, const int value)
{
    m_httpResponse.setHeader(name, value);
}

QMap<QByteArray,QByteArray>& QtWebAppHttpResponse::getHeaders()
{
    return m_httpResponse.getHeaders();
}

void QtWebAppHttpResponse::setStatus(const int statusCode, const QByteArray description)
{
    m_httpResponse.setStatus(statusCode, description);
}

int QtWebAppHttpResponse::getStatusCode() const
{
    return m_httpResponse.getStatusCode();
}

void QtWebAppHttpResponse::write(const QByteArray data, const bool lastPart)
{
    m_httpResponse.write(data, lastPart);
}

bool QtWebAppHttpResponse::hasSentLastPart() const
{
    return m_httpResponse.hasSentLastPart();
}

void QtWebAppHttpResponse::redirect(const QByteArray& url)
{
    m_httpResponse.redirect(url);
}

void QtWebAppHttpResponse::flush()
{
    m_httpResponse.flush();
}

bool QtWebAppHttpResponse::isConnected() const
{
    return m_httpResponse.isConnected();
}

