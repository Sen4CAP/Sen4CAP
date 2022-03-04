#ifndef L3BPRODUCTHELPER_H
#define L3BPRODUCTHELPER_H

#include "generichighlevelproducthelper.h"

namespace orchestrator
{
namespace products
{
class L3BProductHelper : public GenericHighLevelProductHelper
{
public:
    L3BProductHelper();
    L3BProductHelper(const ProductDetails &product);
    L3BProductHelper(const QString &product);
    virtual QStringList GetProductMasks(const QString &pattern);
};

} // end of namespace products
} // // end of namespace orchestrator

#endif // L3BPRODUCTHELPER_H
