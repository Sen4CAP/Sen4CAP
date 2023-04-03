#include "productdetails.h"

ProductDetails::ProductDetails()
    : prd(), additionalComposingProducts()
{
}

ProductDetails::ProductDetails(const Product &prd)
    : prd(prd), additionalComposingProducts()
{
}

bool ProductDetails::operator==(const ProductDetails& rhs) {
    if(prd.productId != rhs.prd.productId) {
        return false;
    }
    return true;
}
