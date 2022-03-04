#include "l3bproducthelper.h"

using namespace orchestrator::products;

L3BProductHelper::L3BProductHelper()
{

}

L3BProductHelper::L3BProductHelper(const ProductDetails &product) :
    GenericHighLevelProductHelper(product)
{
    SetProduct(product.GetProduct().fullPath);
}

L3BProductHelper::L3BProductHelper(const QString &product) :
    GenericHighLevelProductHelper(product)
{
}

QStringList L3BProductHelper::GetProductMasks(const QString &)
{
    // For L3B we return only the validity masks rasters
    return GetProductFilesByTile("MMONODFLG", true).values();
}



