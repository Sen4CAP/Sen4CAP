#ifndef MASKEDL2APRODUCTHELPER_H
#define MASKEDL2APRODUCTHELPER_H

#include "l2aproducthelper.h"

namespace orchestrator
{
namespace products
{

class MaskedL2AProductHelper : public ProductHelper
{
public:
    MaskedL2AProductHelper();
    MaskedL2AProductHelper(const ProductDetails &product);
    MaskedL2AProductHelper(const QString &product);

    static bool IsIntendedFor(const QString &product);
    static bool IsIntendedFor(const ProductType &prdType);

    virtual void SetProduct(const QString &product);
    virtual void SetProduct(const ProductDetails &product);

    virtual bool HasSatellite()  { return false; }
    virtual bool HasMasks()  { return true; }
    virtual bool IsRaster()  { return true; }
    virtual bool HasTiles()  { return true; }

    virtual QStringList GetProductMetadataFiles() { return m_l2aPrdHelper.GetProductMetadataFiles(); }
    virtual QStringList GetProductFiles(const QString & fileNameSubstrFilter = "") { return m_l2aPrdHelper.GetProductFiles(fileNameSubstrFilter); }
    virtual QStringList GetProductMasks(const QString &pattern = "");

private:
    L2AProductHelper m_l2aPrdHelper;

};

} // end of namespace products
} // // end of namespace orchestrator

#endif // MASKEDL2APRODUCTHELPER_H
