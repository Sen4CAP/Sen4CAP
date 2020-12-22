#ifndef ABSTRACTHTTPRESPONSE_H
#define ABSTRACTHTTPRESPONSE_H

#include <QMap>
#include <QString>

class AbstractHttpResponse
{
public:
    AbstractHttpResponse() {}

    /**
      Set a HTTP response header.
      You must call this method before the first write().
      @param name name of the header
      @param value value of the header
    */
    virtual void setHeader(const QByteArray name, const QByteArray value) = 0;

    /**
      Set a HTTP response header.
      You must call this method before the first write().
      @param name name of the header
      @param value value of the header
    */
    virtual void setHeader(const QByteArray name, const int value) = 0;

    /** Get the map of HTTP response headers */
    virtual QMap<QByteArray,QByteArray>& getHeaders() = 0;

    /**
      Set status code and description. The default is 200,OK.
      You must call this method before the first write().
    */
    virtual void setStatus(const int statusCode, const QByteArray description=QByteArray()) = 0;

    /** Return the status code. */
    virtual int getStatusCode() const = 0;

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
    virtual void write(const QByteArray data, const bool lastPart=false) = 0;

    /**
      Indicates whether the body has been sent completely (write() has been called with lastPart=true).
    */
    virtual bool hasSentLastPart() const = 0;

    /**
      Send a redirect response to the browser.
      Cannot be combined with write().
      @param url Destination URL
    */
    virtual void redirect(const QByteArray& url) = 0;

    /**
     * Flush the output buffer (of the underlying socket).
     * You normally don't need to call this method because flush is
     * automatically called after HttpRequestHandler::service() returns.
     */
    virtual void flush() = 0;

    /**
     * May be used to check whether the connection to the web client has been lost.
     * This might be useful to cancel the generation of large or slow responses.
     */
    virtual bool isConnected() const = 0;

};

#endif // ABSTRACTHTTPRESPONSE_H
