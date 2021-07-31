#include "productdetailsbuilder.h"

ProductDetailsBuilder::ProductDetailsBuilder()
{

}

ProductDetails ProductDetailsBuilder::CreateDetails(const Product &prd, EventProcessingContext &ctx)
{
    ProductDetails prdDetails(prd);
    switch (prd.productTypeId) {
        case ProductType::MaskedL2AProductTypeId:
        {
            const ProductList &l2aPrds = ctx.GetParentProductsInProvenanceById(prd.productId, {ProductType::L2AProductTypeId});
            if (l2aPrds.size() != 1) {
                throw std::runtime_error(
                    QStringLiteral("Parent L2A products for masked L2A product % is not 1 (is %2)").arg(prd.fullPath).arg(l2aPrds.size()).
                            toStdString());
            }
            prdDetails.SetAdditionalComposingProducts(l2aPrds);
            break;
        }
        default:
            break;
    }

    return prdDetails;
}
