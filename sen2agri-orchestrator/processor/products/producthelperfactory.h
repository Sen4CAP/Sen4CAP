#ifndef PRODUCTHELPERFACTORY_H
#define PRODUCTHELPERFACTORY_H

#include "model.hpp"
#include "producthelper.h"
#include <memory>

namespace orchestrator
{
namespace products
{

class ProductHelperFactory
{
public:
    static std::unique_ptr<ProductHelper> GetProductHelper(const QString &prdPath);
    static std::unique_ptr<ProductHelper> GetProductHelper(const ProductDetails &prdWrp);
    static std::unique_ptr<ProductHelper> GetProductHelper(const ProductType &prdType);

private:
    ProductHelperFactory();
};

} // end of namespace products
} // // end of namespace orchestrator

#endif // PRODUCTHELPERFACTORY_H
