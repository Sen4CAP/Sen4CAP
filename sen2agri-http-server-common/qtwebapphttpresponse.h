#ifndef QTWEBAPPHTTPRESPONSE_H
#define QTWEBAPPHTTPRESPONSE_H

#include <httpserver/httpresponse.h>
#include "abstracthttpresponse.h"

using namespace stefanfrings;


class QtWebAppHttpResponse : public AbstractHttpResponse
{
public:
    QtWebAppHttpResponse(HttpResponse &httpResponse);

    HttpResponse & getResponse();

    /**
      Set a HTTP response header.
      You must call this method before the first write().
      @param name name of the header
      @param value value of the header
    */
    virtual void setHeader(const QByteArray name, const QByteArray value);

    /**
      Set a HTTP response header.
      You must call this method before the first write().
      @param name name of the header
      @param value value of the header
    */
    virtual void setHeader(const QByteArray name, const int value);

    /** Get the map of HTTP response headers */
    virtual QMap<QByteArray,QByteArray>& getHeaders();

    /**
      Set status code and description. The default is 200,OK.
      You must call this method before the first write().
    */
    virtual void setStatus(const int statusCode, const QByteArray description=QByteArray());

    /** Return the status code. */
    virtual int getStatusCode() const;

    /**
      Write body data to the socket.
      <p>
      The HTTP status line, headers and cookies are sent automatically before the body.
      <p>
      If the response contains only a single chunk (indicated by lastPart=true),
      then a Content-Length header is automatically set.
      <p>
      Chunked mode is automatically selected if there is no Content-Length header
      and also no Connection:close header.
      @param data Data bytes of the body
      @param lastPart Indicates that this is the last chunk of data and flushes the output buffer.
    */
    virtual void write(const QByteArray data, const bool lastPart=false);

    /**
      Indicates whether the body has been sent completely (write() has been called with lastPart=true).
    */
    virtual bool hasSentLastPart() const;

    /**
      Send a redirect response to the browser.
      Cannot be combined with write().
      @param url Destination URL
    */
    virtual void redirect(const QByteArray& url);

    /**
     * Flush the output buffer (of the underlying socket).
     * You normally don't need to call this method because flush is
     * automatically called after HttpRequestHandler::service() returns.
     */
    virtual void flush();

    /**
     * May be used to check whether the connection to the web client has been lost.
     * This might be useful to cancel the generation of large or slow responses.
     */
    virtual bool isConnected() const;


private :
    HttpResponse &m_httpResponse;
};

#endif // QTWEBAPPHTTPRESPONSE_H
