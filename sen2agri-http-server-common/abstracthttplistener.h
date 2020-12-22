#ifndef ABSTRACTHTTPLISTENER_H
#define ABSTRACTHTTPLISTENER_H


class AbstractHttpListener
{
public:
    AbstractHttpListener();
    virtual ~AbstractHttpListener();

    virtual void listen() = 0;
    virtual void close() = 0;
};

#endif // ABSTRACTHTTPLISTENER_H
