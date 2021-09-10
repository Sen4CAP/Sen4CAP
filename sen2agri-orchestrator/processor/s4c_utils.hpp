#ifndef S4C_UTILS_H
#define S4C_UTILS_H

#include "model.hpp"
#include "eventprocessingcontext.hpp"

class S4CUtils
{
public:
    static ProductList GetLpisProduct(ExecutionContextBase *pCtx, int siteId);
    static QString GetSiteYearFromDisk(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                       const QString &siteShortName, const QString &cfgFilesSubPath,
                                       const QString &cfgKeyPrefix, const QString &cfgKey);

private:
    S4CUtils();
};

#endif // S4C_UTILS_H
