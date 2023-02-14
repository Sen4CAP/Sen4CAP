#ifndef PRODUCTDETAILSBUILDER_H
#define PRODUCTDETAILSBUILDER_H

#include "processor/products/productdetails.h"
#include "eventprocessingcontext.hpp"

class ProductDetailsBuilder
{
public:
    ProductDetailsBuilder();

    static QList<ProductDetails> CreateDetails(const ProductList &prds, EventProcessingContext &ctx);

private:
    static bool CompareProductDates(const ProductDetails &prd1, const ProductDetails &prd2);
};

#endif // PRODUCTDETAILSBUILDER_H
