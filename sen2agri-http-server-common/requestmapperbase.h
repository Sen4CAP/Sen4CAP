#ifndef REQUESTMAPPERBASE_H
#define REQUESTMAPPERBASE_H

#include "abstracthttpcontroller.h"

class RequestMapperBase
{
public:
    RequestMapperBase();

    void addController(const QString &pathPrefix, AbstractHttpController *pController);

protected :
    QMap<QString, AbstractHttpController*> m_mapControllers;

};

#endif // REQUESTMAPPERBASE_H
