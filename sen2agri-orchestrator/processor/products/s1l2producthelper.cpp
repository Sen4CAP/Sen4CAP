#include "s1l2producthelper.h"

#include <QRegularExpression>

using namespace orchestrator::products;

// QString S1L2ProductHelper::S1L2NewFormatFolderPattern = "(SEN4CAP_L2A_S\\d{1,3}_V)(\\d{8}T\\d{6})_(\\d{8}T\\d{6})_V[HV]_(\\d{3})";

#define S1_REGEX                    R"(SEN4CAP_L2A_.*_V(\d{8}T\d{6})_(\d{8}T\d{6})_(VH|VV)_(\d{3})_(?:.+)?(AMP|COHE)(\.tif)?)"
#define S1_REGEX_DATE_IDX           1
#define S1_REGEX_DATE2_IDX          2
#define S1_REGEX_TYPE_IDX           6

S1L2ProductHelper::S1L2ProductHelper()
{

}

S1L2ProductHelper::S1L2ProductHelper(const ProductDetails &product) :
    ProductHelper(product)
{
    SetProduct(product.GetProduct().fullPath);
}

S1L2ProductHelper::S1L2ProductHelper(const QString &product) :
    ProductHelper(product)
{
    SetProduct(product);
}

QStringList S1L2ProductHelper::GetProductFiles(const QString &)
{
    return QStringList(m_prdDetails.GetProduct().fullPath);
}

void S1L2ProductHelper::SetProduct(const QString &product)
{
    ProductHelper::SetProduct(product);

    QFileInfo qfileInfo(product);
    const QString &name = qfileInfo.baseName();
    // normally, it should
    QRegExp rx(S1_REGEX);
    if (rx.indexIn(name) == -1) {
        return;
    }

    const QStringList &list = rx.capturedTexts();
    if (S1_REGEX_TYPE_IDX >= list.size()) {
        return;
    }

    // extract min/max dates
    const QString &minDateStr = list.at(S1_REGEX_DATE_IDX);
    QDateTime startDate = QDateTime::fromString(minDateStr, "yyyyMMddThhmmss");
    const QString &maxDateStr = list.at(S1_REGEX_DATE2_IDX);
    QDateTime endDate = QDateTime::fromString(maxDateStr, "yyyyMMddThhmmss");
    if (endDate < startDate) {
        // switch the dates if they are inversed
        startDate = endDate;
        endDate = QDateTime::fromString(minDateStr, "yyyyMMdd");
    }

    // get the product type
    const QString &type = list.at(S1_REGEX_TYPE_IDX);
    this->m_prdDetails.GetProductRef().productTypeId = (type == "AMP") ? ProductType::S4CS1L2AmpProductTypeId :
                                            ProductType::S4CS1L2CoheProductTypeId;

    this->m_prdDetails.GetProductRef().satId = (int)Satellite::Sentinel1;
    this->m_prdDetails.GetProductRef().created = endDate;

    m_bValid = true;
}

bool S1L2ProductHelper::IsIntendedFor(const QString &product)
{
    QFileInfo info(product);
    QRegExp rx(S1_REGEX);
    const QString &name = info.baseName();
    if (rx.indexIn(name) == -1) {
        return false;
    }
    return true;
}

bool S1L2ProductHelper::IsIntendedFor(const ProductType &prdType)
{
    return (prdType == ProductType::S4CS1L2AmpProductTypeId ||
            prdType == ProductType::S4CS1L2CoheProductTypeId);
}
