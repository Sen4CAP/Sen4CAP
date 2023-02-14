#include "executioncontextbase.hpp"

ExecutionContextBase::ExecutionContextBase(PersistenceManagerDBProvider &persistenceManager)
    :persistenceManager(persistenceManager)
{
}

std::map<QString, QString> ExecutionContextBase::GetConfigurationParameters(const QString &prefix, int siteId)
{
    const auto &paramList = persistenceManager.GetConfigurationParameters(prefix);

    std::map<QString, QString> result;
    for (const auto &p : paramList) {
        if (siteId == -1 || p.siteId.value_or(-1) == -1 || p.siteId == siteId) {
            result.emplace(p.key, p.value);
        }
    }

    return result;
}

ProductList ExecutionContextBase::GetProducts(int siteId, int productTypeId, const QDateTime &startDate, const QDateTime &endDate)
{
    return persistenceManager.GetProducts(siteId, productTypeId, startDate, endDate);
}

L1CProductList ExecutionContextBase::GetL1CProducts(int siteId, const SatellitesList &satelliteIds, const StatusesList &statusIds,
                                                    const QDateTime &startDate, const QDateTime &endDate)
{
    return persistenceManager.GetL1CProducts(siteId, satelliteIds, statusIds, startDate, endDate);
}

QString ExecutionContextBase::GetProcessorShortName(int processorId)
{
    return persistenceManager.GetProcessorShortName(processorId);
}

QString ExecutionContextBase::GetSiteShortName(int siteId) {
    return persistenceManager.GetSiteShortName(siteId);
}

QString ExecutionContextBase::GetSiteName(int siteId)
{
    return persistenceManager.GetSiteName(siteId);
}

SeasonList ExecutionContextBase::GetSiteSeasons(int siteId)
{
    return persistenceManager.GetSiteSeasons(siteId);
}

ProductList ExecutionContextBase::GetL1DerivedProducts(int siteId, ProductType productTypeId, const ProductIdsList &dwnHistIds)
{
    return persistenceManager.GetL1DerivedProducts(siteId, static_cast<int>(productTypeId), dwnHistIds);
}

ProductIdToDwnHistIdMap ExecutionContextBase::GetDownloaderHistoryIds(const ProductIdsList &prdIds)
{
    return persistenceManager.GetDownloaderHistoryIds(prdIds);
}

ProductList ExecutionContextBase::GetProducts(const ProductIdsList &productIds)
{
    return persistenceManager.GetProducts(productIds);
}

ProductList ExecutionContextBase::GetProducts(int siteId, const QStringList &productNames)
{
    return persistenceManager.GetProducts(siteId, productNames);
}

QMap<QString, QString> ExecutionContextBase::GetProductsFullPaths(int siteId, const QStringList &productNames)
{
    const ProductList &infos = persistenceManager.GetProducts(siteId, productNames);
    QMap<QString, QString> fullPaths;
    for(const Product &info : infos) {
        fullPaths[info.name] = info.fullPath;
    }
    return fullPaths;
}

ProductList ExecutionContextBase::GetParentProductsInProvenance(int siteId, const QList<ProductType> &sourcePrdTypes, const ProductType &derivedProductType,
                                               const QDateTime &startDate, const QDateTime &endDate)
{
    return persistenceManager.GetParentProductsByProvenancePresence(siteId, sourcePrdTypes, derivedProductType, startDate, endDate, true);
}

ProductList ExecutionContextBase::GetParentProductsNotInProvenance(int siteId, const QList<ProductType> &sourcePrdTypes, const ProductType &derivedProductType,
                                               const QDateTime &startDate, const QDateTime &endDate)
{
    return persistenceManager.GetParentProductsByProvenancePresence(siteId, sourcePrdTypes, derivedProductType, startDate, endDate, false);
}

QMap<int, ProductList> ExecutionContextBase::GetParentProductsInProvenanceByIds(const QList<int> &productIds, const QList<ProductType> &sourcePrdTypes)
{
    return persistenceManager.GetParentProductsInProvenanceByIds(productIds, sourcePrdTypes);
}

JobIdsList ExecutionContextBase::GetActiveJobIds(int processorId, int siteId)
{
    return persistenceManager.GetActiveJobsIds(processorId, siteId);
}

bool ExecutionContextBase::IsProcessingDone(ProductType prdType, int siteId, const QDateTime &startDate, const QDateTime &endDate, const QList<int> &satIds)
{
    return persistenceManager.IsProcessingDone(prdType, siteId, startDate, endDate, satIds);
}
