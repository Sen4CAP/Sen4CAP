#include "producthelperfactory.h"
#include "l2aproducthelper.h"
#include "tilestimeseries.hpp"
#include "s1l2producthelper.h"
#include "maskedl2aproducthelper.h"
#include "generichighlevelproducthelper.h"

#include "productdetails.h"

#include <qfileinfo.h>
#include "make_unique.hpp"
#include <memory>

using namespace orchestrator::products;

ProductHelperFactory::ProductHelperFactory()
{

}

std::unique_ptr<ProductHelper> ProductHelperFactory::GetProductHelper(const ProductDetails &prdWrp)
{
    if (L2AProductHelper::IsIntendedFor(prdWrp.GetProduct().productTypeId)) {
        std::unique_ptr<ProductHelper> l2aHelper(new L2AProductHelper(prdWrp));
        return l2aHelper;
    }

    if (MaskedL2AProductHelper::IsIntendedFor(prdWrp.GetProduct().productTypeId)) {
        std::unique_ptr<ProductHelper> maskedl2aHelper(new MaskedL2AProductHelper(prdWrp));
        return maskedl2aHelper;
    }

    if (S1L2ProductHelper::IsIntendedFor(prdWrp.GetProduct().productTypeId)) {
        std::unique_ptr<ProductHelper> s1l2Helper(new S1L2ProductHelper(prdWrp));
        return s1l2Helper;
    }

    if (GenericHighLevelProductHelper::IsIntendedFor(prdWrp.GetProduct().productTypeId)) {
        std::unique_ptr<ProductHelper> genericHelper(new GenericHighLevelProductHelper(prdWrp));
        return genericHelper;
    }

    throw std::runtime_error(
        QStringLiteral("Unable to find an a handler for product %1.").arg(prdWrp.GetProduct().fullPath).toStdString());
}

std::unique_ptr<ProductHelper> ProductHelperFactory::GetProductHelper(const QString &prdPath)
{
    if (L2AProductHelper::IsIntendedFor(prdPath)) {
        std::unique_ptr<ProductHelper> l2aHelper(new L2AProductHelper(prdPath));
        return l2aHelper;
    }

    if (MaskedL2AProductHelper::IsIntendedFor(prdPath)) {
        std::unique_ptr<ProductHelper> maskedl2aHelper(new MaskedL2AProductHelper(prdPath));
        return maskedl2aHelper;
    }

    if (S1L2ProductHelper::IsIntendedFor(prdPath)) {
        std::unique_ptr<ProductHelper> s1l2Helper(new S1L2ProductHelper(prdPath));
        return s1l2Helper;
    }

    if (GenericHighLevelProductHelper::IsIntendedFor(prdPath)) {
        std::unique_ptr<ProductHelper> genericHelper(new GenericHighLevelProductHelper(prdPath));
        return genericHelper;
    }

    throw std::runtime_error(
        QStringLiteral("Unable to find an a handler for product %1.").arg(prdPath).toStdString());
}

// Returns an unitialized product helper for the provided product type
std::unique_ptr<ProductHelper> ProductHelperFactory::GetProductHelper(const ProductType &prdType) {
    if (L2AProductHelper::IsIntendedFor(prdType)) {
        std::unique_ptr<ProductHelper> l2aHelper(new L2AProductHelper());
        return l2aHelper;
    }

    if (MaskedL2AProductHelper::IsIntendedFor(prdType)) {
        std::unique_ptr<ProductHelper> maskedl2aHelper(new MaskedL2AProductHelper());
        return maskedl2aHelper;
    }

    if (S1L2ProductHelper::IsIntendedFor(prdType)) {
        std::unique_ptr<ProductHelper> s1l2Helper(new S1L2ProductHelper());
        return s1l2Helper;
    }

    if (GenericHighLevelProductHelper::IsIntendedFor(prdType)) {
        std::unique_ptr<ProductHelper> genericHelper(new GenericHighLevelProductHelper());
        return genericHelper;
    }

    throw std::runtime_error(
        QStringLiteral("Unable to find an a handler for product type %1.").arg(QString::number((int)prdType)).toStdString());
}

