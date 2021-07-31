#ifndef PRODUCTDETAILS_H
#define PRODUCTDETAILS_H

#include "model.hpp"

class ProductDetails {

public:
    ProductDetails();
    ProductDetails(const Product &prd);
    ProductDetails(const ProductDetails &other);

    void SetProduct(const Product &prd) {this->prd = prd;}
    void SetAdditionalComposingProducts(const ProductList &prds) { additionalComposingProducts = prds;}

    inline Product GetProduct() const { return prd; }
    inline Product& GetProductRef() { return prd; }
    inline ProductList GetAdditionalCompositingProducts() const { return additionalComposingProducts; }

    bool operator==(const ProductDetails& rhs);

private:
    Product prd;
    ProductList additionalComposingProducts;    // if a product is a wrapper over one or several products
};

typedef QList<ProductDetails> ProductDetailsList;


#endif // PRODUCTDETAILS_H
