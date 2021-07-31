#ifndef PRODUCTHELPER_H
#define PRODUCTHELPER_H

#include "model.hpp"
#include "QFileInfo"
#include "productdetails.h"

namespace orchestrator
{
namespace products
{
#define ADD_PRD_INFO_PARENTS_CNT "PARENT_PRD_CNT"
#define ADD_PRD_INFO_PARENT_PREFIX "PARENT"

class ProductHelper
{
public:
    ProductHelper();
    ProductHelper(const QString &product);
    ProductHelper(const ProductDetails &prdWrp);

    virtual void SetProduct(const QString &product);
    virtual void SetProduct(const ProductDetails &product);

    virtual bool IsValid() { return m_bValid; }
    virtual bool HasSatellite()  { return false; }
    virtual bool HasMasks()  { return false; }
    virtual bool IsRaster()  { return false; }
    virtual bool HasTiles()  { return false; }


    virtual QStringList GetProductMetadataFiles() { return QStringList(); }
    virtual QStringList GetProductFiles(const QString & fileNameSubstrFilter = "");
    virtual QStringList GetProductMasks(const QString &fileNameSubstrFilter = "");
    virtual QStringList GetTileIdsFromProduct() { return m_prdDetails.GetProduct().tiles; }

    virtual QMap<QString, QString> GetProductFilesByTile(const QString &tileIdFilter = "", bool isQI = false);

    virtual Satellite GetSatellite() { return (Satellite)m_prdDetails.GetProduct().satId; }
    virtual QDateTime GetAcqDate() { return m_prdDetails.GetProduct().created; }
    virtual ProductType GetProductType() { return m_prdDetails.GetProduct().productTypeId; }
    virtual bool HasValidStructure() { return m_bValid; }

    static Satellite GetPrimarySatelliteId(const QList<Satellite> &satIds);
    static QString GetProductTypeShortName(ProductType prdType);

protected:
    ProductDetails m_prdDetails;
    bool m_bValid;

};
} // end of namespace products
} // // end of namespace orchestrator
#endif // PRODUCTHELPER_H
