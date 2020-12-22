#ifndef ABSTRACTHTTPCONTROLLER_H
#define ABSTRACTHTTPCONTROLLER_H

#include "abstracthttprequest.h"
#include "abstracthttpresponse.h"

class AbstractHttpController
{
public:
    AbstractHttpController();

    virtual void service(AbstractHttpRequest &request, AbstractHttpResponse &response) = 0;
};

#endif // ABSTRACTHTTPCONTROLLER_H
