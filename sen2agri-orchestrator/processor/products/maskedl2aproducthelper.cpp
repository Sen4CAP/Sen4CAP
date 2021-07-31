#include "maskedl2aproducthelper.h"

using namespace orchestrator::products;

MaskedL2AProductHelper::MaskedL2AProductHelper()
{

}

MaskedL2AProductHelper::MaskedL2AProductHelper(const ProductDetails &product) :
    ProductHelper(product)
{
    SetProduct(product);
}

MaskedL2AProductHelper::MaskedL2AProductHelper(const QString &product) :
    ProductHelper(product)
{
    throw std::runtime_error(QStringLiteral("Masked L2A product does not support simple product path %1.").
                             arg(product).toStdString());
}

bool MaskedL2AProductHelper::IsIntendedFor(const QString &product)
{
    QFileInfo info(product);
    const QString &name = info.baseName();
    if (info.isDir() && (name.contains("_L2AMSK_", Qt::CaseSensitive) ||
            name.contains("_MSIL2AMSK_", Qt::CaseSensitive))) {
        return true;
    }
    return false;
}

bool MaskedL2AProductHelper::IsIntendedFor(const ProductType &type)
{
    return (type == ProductType::MaskedL2AProductTypeId);
}

void MaskedL2AProductHelper::SetProduct(const QString &product)
{
    throw std::runtime_error(QStringLiteral("Masked L2A product does not support simple product path %1.").
                             arg(product).toStdString());
}

void MaskedL2AProductHelper::SetProduct(const ProductDetails &product)
{
    ProductHelper::SetProduct(product);
    const ProductList &l2aPrds = product.GetAdditionalCompositingProducts();
    if (l2aPrds.size() != 1) {
        throw std::runtime_error(QStringLiteral("Masked L2A product %1 should have exactly 1 L2A product but it has %2.").
                                 arg(product.GetProduct().fullPath).arg(l2aPrds.size()).toStdString());
    }
    ProductDetails l2aPrdDetails(l2aPrds[0]);
    m_l2aPrdHelper.SetProduct(l2aPrdDetails);
}

QStringList MaskedL2AProductHelper::GetProductMasks(const QString &pattern)
{
    QFileInfo info(m_prdDetails.GetProduct().fullPath);
    const QString &prdDir = info.absoluteFilePath();
    if (pattern.size() == 0) {
        return {prdDir};
    }
    return m_l2aPrdHelper.GetRasterFilesFromDir(prdDir, pattern, "*", "", {".tif"});
}
