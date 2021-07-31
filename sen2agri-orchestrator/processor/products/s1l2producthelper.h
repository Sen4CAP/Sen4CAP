#ifndef S1L2PRODUCTHELPER_H
#define S1L2PRODUCTHELPER_H

#include "producthelper.h"

namespace orchestrator
{
namespace products
{

class S1L2ProductHelper : public ProductHelper
{
public:
    S1L2ProductHelper();
    S1L2ProductHelper(const ProductDetails &product);
    S1L2ProductHelper(const QString &product);

    virtual void SetProduct(const QString &product);
    virtual bool HasSatellite() { return true;}
    virtual bool IsRaster()  { return true; }
    virtual QStringList GetProductFiles(const QString &fileNameSubstrFilter="");

    static bool IsIntendedFor(const QString &product);
    static bool IsIntendedFor(const ProductType &prdType);

private:
};

} // end of namespace products
} // // end of namespace orchestrator

#endif // S1L2PRODUCTHELPER_H
