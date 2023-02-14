#include "productdetailsbuilder.h"

ProductDetailsBuilder::ProductDetailsBuilder()
{

}

QList<ProductDetails> ProductDetailsBuilder::CreateDetails(const ProductList &prds, EventProcessingContext &ctx)
{
    QList<ProductDetails> prdDetailsList;
    QList<int> maskedL2APrdIds;
    std::for_each(prds.begin(), prds.end(), [&maskedL2APrdIds](Product prd) {
        if (prd.productTypeId == ProductType::MaskedL2AProductTypeId) {
            maskedL2APrdIds.append(prd.productId);
        }
    });
    if (maskedL2APrdIds.size() > 0 && maskedL2APrdIds.size() != prds.size()) {
        throw std::runtime_error(
            QStringLiteral("The products of different types is not supported by this function").toStdString());
    }
    if (maskedL2APrdIds.size() > 0) {
        const QMap<int, ProductList> &l2aPrdsMap = ctx.GetParentProductsInProvenanceByIds(maskedL2APrdIds, {ProductType::L2AProductTypeId});
        int i = 0;
        for (int mskL2APrdId: maskedL2APrdIds) {
            QMap<int, ProductList>::const_iterator it = l2aPrdsMap.constFind(mskL2APrdId);
            if (it != l2aPrdsMap.end() && it.key() == mskL2APrdId) {
                const ProductList &l2aPrds = it.value();
                if (l2aPrds.size() != 1) {
                    throw std::runtime_error(
                        QStringLiteral("Parent L2A products for masked L2A product %1 is not 1 (is %2)").arg(mskL2APrdId).arg(l2aPrds.size()).
                                toStdString());
                }
                ProductDetails prdDetails(prds[i]);
                prdDetails.SetAdditionalComposingProducts(l2aPrds);
                prdDetailsList.push_back(prdDetails);
            } else {
                throw std::runtime_error(
                    QStringLiteral("Parent L2A products for masked L2A product %1 does not exist in the database").arg(mskL2APrdId).
                            toStdString());
            }
            i++;
       }
    } else {
        std::for_each(prds.begin(), prds.end(), [&prdDetailsList](Product prd) {
            prdDetailsList.push_back(ProductDetails(prd));
        });
    }

    // sort the input products according to their dates
    qSort(prdDetailsList.begin(), prdDetailsList.end(), CompareProductDates);

    return prdDetailsList;
}

bool ProductDetailsBuilder::CompareProductDates(const ProductDetails &prd1, const ProductDetails &prd2)
{
    return (prd1.GetProduct().created < prd2.GetProduct().created);
}


