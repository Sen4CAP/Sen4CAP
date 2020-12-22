#include "qtwebapphttprequest.h"

QtWebAppHttpRequest::QtWebAppHttpRequest(HttpRequest &httpRequest)
    : m_httpRequest(httpRequest)
{

}

QByteArray QtWebAppHttpRequest::getMethod() const
{
    return m_httpRequest.getMethod();
}

QByteArray QtWebAppHttpRequest::getPath() const
{
    return m_httpRequest.getPath();
}

const QByteArray& QtWebAppHttpRequest::getRawPath() const
{
    return m_httpRequest.getRawPath();
}

QByteArray QtWebAppHttpRequest::getVersion() const
{
    return m_httpRequest.getVersion();
}

QByteArray QtWebAppHttpRequest::getHeader(const QByteArray& name) const
{
    return m_httpRequest.getHeader(name);
}

QList<QByteArray> QtWebAppHttpRequest::getHeaders(const QByteArray& name) const
{
    return m_httpRequest.getHeaders(name);
}

QMultiMap<QByteArray,QByteArray> QtWebAppHttpRequest::getHeaderMap() const
{
    return m_httpRequest.getHeaderMap();
}

QByteArray QtWebAppHttpRequest::getParameter(const QByteArray& name) const
{
    return m_httpRequest.getParameter(name);
}

QList<QByteArray> QtWebAppHttpRequest::getParameters(const QByteArray& name) const
{
    return m_httpRequest.getParameters(name);
}

QMultiMap<QByteArray,QByteArray> QtWebAppHttpRequest::getParameterMap() const
{
    return m_httpRequest.getParameterMap();
}

QByteArray QtWebAppHttpRequest::getBody() const
{
    return m_httpRequest.getBody();
}

QTemporaryFile* QtWebAppHttpRequest::getUploadedFile(const QByteArray fieldName) const
{
    return m_httpRequest.getUploadedFile(fieldName);
}

QByteArray QtWebAppHttpRequest::getCookie(const QByteArray& name) const
{
    return m_httpRequest.getCookie(name);
}

QMap<QByteArray,QByteArray>& QtWebAppHttpRequest::getCookieMap()
{
    return m_httpRequest.getCookieMap();
}

QHostAddress QtWebAppHttpRequest::getPeerAddress() const
{
    return m_httpRequest.getPeerAddress();
}
