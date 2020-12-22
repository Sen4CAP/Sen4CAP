#include "requestmapperbase.h"

RequestMapperBase::RequestMapperBase()
{

}

void RequestMapperBase::addController(const QString &pathPrefix,
                             AbstractHttpController *pController)
{
    if(pController && !m_mapControllers.contains(pathPrefix)){
        m_mapControllers.insert(pathPrefix, pController);
    }
}
