#ifndef EXECUTIONCONTEXTBASE_H
#define EXECUTIONCONTEXTBASE_H

#include "persistencemanager.hpp"
#include "model.hpp"

class ExecutionContextBase
{
public:
    ExecutionContextBase(PersistenceManagerDBProvider &persistenceManager);
    std::map<QString, QString> GetConfigurationParameters(const QString &prefix, int siteId);
    ProductList GetProducts(int siteId, int productTypeId, const QDateTime &startDate, const QDateTime &endDate);
    L1CProductList GetL1CProducts(int siteId, const SatellitesList &satelliteIds, const StatusesList &statusIds, const QDateTime &startDate, const QDateTime &endDate);
    QString GetSiteName(int siteId);
    QString GetSiteShortName(int siteId);
    QString GetProcessorShortName(int processorId);

    SeasonList GetSiteSeasons(int siteId);
    ProductList GetL1DerivedProducts(int siteId, ProductType productTypeId, const ProductIdsList &dwnHistIds);
    ProductIdToDwnHistIdMap GetDownloaderHistoryIds(const ProductIdsList &prdIds);
    ProductList GetProducts(const ProductIdsList &productIds);
    ProductList GetProducts(int siteId, const QStringList &productNames);
    QMap<QString, QString> GetProductsFullPaths(int siteId, const QStringList &productNames);
    ProductList GetParentProductsInProvenance(int siteId, const QList<ProductType> &sourcePrdTypes, const ProductType &derivedProductType,
                                                   const QDateTime &startDate, const QDateTime &endDate);
    ProductList GetParentProductsNotInProvenance(int siteId, const QList<ProductType> &sourcePrdTypes, const ProductType &derivedProductType,
                                                   const QDateTime &startDate, const QDateTime &endDate);
    QMap<int, ProductList> GetParentProductsInProvenanceByIds(const QList<int> &productIds, const QList<ProductType> &sourcePrdTypes);

    JobIdsList GetActiveJobIds(int processorId, int siteId);
    bool IsProcessingDone(ProductType prdType, int siteId, const QDateTime &startDate, const QDateTime &endDate, const QList<int> &satIds = {});


protected:
    PersistenceManagerDBProvider &persistenceManager;
};

#endif // EXECUTIONCONTEXTBASE_H
