#ifndef QTWEBAPPHTTPREQUEST_H
#define QTWEBAPPHTTPREQUEST_H

#include <httpserver/httprequest.h>
#include "abstracthttprequest.h"

using namespace stefanfrings;

class QtWebAppHttpRequest : public AbstractHttpRequest
{
public:
    QtWebAppHttpRequest(HttpRequest &httpRequest);

    /** Get the method of the HTTP request  (e.g. "GET") */
    virtual QByteArray getMethod() const;

    /** Get the decoded path of the HTPP request (e.g. "/index.html") */
    virtual QByteArray getPath() const;

    /** Get the raw path of the HTTP request (e.g. "/file%20with%20spaces.html") */
    virtual const QByteArray& getRawPath() const;

    /** Get the version of the HTPP request (e.g. "HTTP/1.1") */
    virtual QByteArray getVersion() const;

    /**
      Get the value of a HTTP request header.
      @param name Name of the header, not case-senitive.
      @return If the header occurs multiple times, only the last
      one is returned.
    */
    virtual QByteArray getHeader(const QByteArray& name) const;

    /**
      Get the values of a HTTP request header.
      @param name Name of the header, not case-senitive.
    */
    virtual QList<QByteArray> getHeaders(const QByteArray& name) const;

    /**
     * Get all HTTP request headers. Note that the header names
     * are returned in lower-case.
     */
    virtual QMultiMap<QByteArray,QByteArray> getHeaderMap() const;

    /**
      Get the value of a HTTP request parameter.
      @param name Name of the parameter, case-sensitive.
      @return If the parameter occurs multiple times, only the last
      one is returned.
    */
    virtual QByteArray getParameter(const QByteArray& name) const;

    /**
      Get the values of a HTTP request parameter.
      @param name Name of the parameter, case-sensitive.
    */
    virtual QList<QByteArray> getParameters(const QByteArray& name) const;

    /** Get all HTTP request parameters. */
    virtual QMultiMap<QByteArray,QByteArray> getParameterMap() const;

    /** Get the HTTP request body.  */
    virtual QByteArray getBody() const;

    /**
      Get an uploaded file. The file is already open. It will
      be closed and deleted by the destructor of this HttpRequest
      object (after processing the request).
      <p>
      For uploaded files, the method getParameters() returns
      the original fileName as provided by the calling web browser.
    */
    virtual QTemporaryFile* getUploadedFile(const QByteArray fieldName) const ;

    /**
      Get the value of a cookie.
      @param name Name of the cookie
    */
    virtual QByteArray getCookie(const QByteArray& name) const ;

    /** Get all cookies. */
    virtual QMap<QByteArray,QByteArray>& getCookieMap() ;

    /**
      Get the address of the connected client.
      Note that multiple clients may have the same IP address, if they
      share an internet connection (which is very common).
     */
    virtual QHostAddress getPeerAddress() const;

private :
    HttpRequest &m_httpRequest;

};

#endif // QTWEBAPPHTTPREQUEST_H
