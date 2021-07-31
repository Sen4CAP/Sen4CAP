#ifndef PRODUCTDETAILSBUILDER_H
#define PRODUCTDETAILSBUILDER_H

#include "processor/products/productdetails.h"
#include "eventprocessingcontext.hpp"

class ProductDetailsBuilder
{
public:
    ProductDetailsBuilder();

    static ProductDetails CreateDetails(const Product &prd, EventProcessingContext &ctx);
};

#endif // PRODUCTDETAILSBUILDER_H
