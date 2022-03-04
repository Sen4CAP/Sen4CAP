#include "s1l2producthelper.h"

#include <QRegularExpression>

using namespace orchestrator::products;

// QString S1L2ProductHelper::S1L2NewFormatFolderPattern = "(SEN4CAP_L2A_S\\d{1,3}_V)(\\d{8}T\\d{6})_(\\d{8}T\\d{6})_V[HV]_(\\d{3})";

// SEN4CAP_L2A_S7_V20200404T165041_20200329T165122_VH_146_AMP.tif
// SEN4CAP_L2A_S7_V20200404T165041_20200329T165122_VH_146_COHE.tif
#define S1_REGEX                    R"(SEN4CAP_(L2A)_.*_V(\d{8}T\d{6})_(\d{8}T\d{6})_(VH|VV)_(\d{3})_(?:.+)?(AMP|COHE)(\.tif)?)"
#define S1_REGEX_VER_IDX            1
#define S1_REGEX_DATE_IDX           2
#define S1_REGEX_DATE2_IDX          3
#define S1_REGEX_TYPE_IDX           6
#define S1_REGEX_ORBIT_IDX          5

// S1A_L2_BCK_20200114T070238_VV_096_28RBR.tif
// S1A_L2_COH_20200114T070238_20200108T070156_VV_096_28RBR.tif
#define S1_V2_REGEX                    R"(S1[A-D]_(L2)_(BCK|COH)_(\d{8}T\d{6})(_(\d{8}T\d{6}))?_(VH|VV)_(\d{3})_(\d{2}\w{3})(\.tif)?)"
#define S1_V2_REGEX_VER_IDX            1
#define S1_V2_REGEX_DATE_IDX           3
#define S1_V2_REGEX_DATE2_IDX          5
#define S1_V2_REGEX_TYPE_IDX           2
#define S1_V2_REGEX_ORBIT_IDX          7
#define S1_V2_REGEX_TILET_IDX          8

const QList<FileNameInfosExtractor> S1L2ProductHelper::fnInfoExtractors = {
    FileNameInfosExtractor(S1_REGEX, S1_REGEX_VER_IDX, S1_REGEX_DATE_IDX, S1_REGEX_DATE2_IDX,
                           S1_REGEX_TYPE_IDX, S1_REGEX_ORBIT_IDX),
    FileNameInfosExtractor(S1_V2_REGEX, S1_V2_REGEX_VER_IDX, S1_V2_REGEX_DATE_IDX, S1_V2_REGEX_DATE2_IDX,
                           S1_V2_REGEX_TYPE_IDX, S1_V2_REGEX_ORBIT_IDX, S1_V2_REGEX_TILET_IDX)
};

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

//void S1L2ProductHelper::test()
//{
//    FileNameInfosExtractor fnExtractor1;
//    if (GetFileNameInfosExtractor("SEN4CAP_L2A_S7_V20200404T165041_20200329T165122_VH_146_AMP.tif", fnExtractor1)) {
//        const FileNameInfosType &fnInfos = fnExtractor1.ExtractInfos("SEN4CAP_L2A_S7_V20200404T165041_20200329T165122_VH_146_AMP.tif");
//        QString tileId = fnInfos.tileId;
//    }
//    FileNameInfosExtractor fnExtractor2;
//    if (GetFileNameInfosExtractor("SEN4CAP_L2A_S7_V20200404T165041_20200329T165122_VH_146_COHE.tif", fnExtractor2)) {
//        const FileNameInfosType &fnInfos = fnExtractor2.ExtractInfos("SEN4CAP_L2A_S7_V20200404T165041_20200329T165122_VH_146_COHE.tif");
//        QString tileId = fnInfos.tileId;
//    }
//    FileNameInfosExtractor fnExtractor22;
//    if (GetFileNameInfosExtractor("SEN4CAP_L2A_S53_V20190309T050916_20190303T050951_VV_022_S1_L2A_COHE.tif", fnExtractor22)) {
//        const FileNameInfosType &fnInfos = fnExtractor22.ExtractInfos("SEN4CAP_L2A_S53_V20190309T050916_20190303T050951_VV_022_S1_L2A_COHE.tif");
//        QString tileId = fnInfos.tileId;
//    }
//    FileNameInfosExtractor fnExtractor3;
//    if (GetFileNameInfosExtractor("S1A_L2_BCK_20200114T070238_VV_096_28RBR.tif", fnExtractor3)) {
//        const FileNameInfosType &fnInfos = fnExtractor3.ExtractInfos("S1A_L2_BCK_20200114T070238_VV_096_28RBR.tif");
//        QString tileId = fnInfos.tileId;
//    }
//    FileNameInfosExtractor fnExtractor4;
//    if (GetFileNameInfosExtractor("S1A_L2_COH_20200114T070238_20200108T070156_VV_096_28RBR.tif", fnExtractor4)) {
//        const FileNameInfosType &fnInfos = fnExtractor4.ExtractInfos("S1A_L2_COH_20200114T070238_20200108T070156_VV_096_28RBR.tif");
//        QString tileId = fnInfos.tileId;
//    }
//}

QStringList S1L2ProductHelper::GetProductFiles(const QString &)
{
    return QStringList(m_prdDetails.GetProduct().fullPath);
}

void S1L2ProductHelper::SetProduct(const QString &productPath)
{
    ProductHelper::SetProduct(productPath);

    FileNameInfosExtractor fnExtractor;
    if (!GetFileNameInfosExtractor(productPath, fnExtractor)) {
        return;
    }
    const FileNameInfosType &fnInfos = fnExtractor.ExtractInfos(productPath);

    // get the product type
    this->m_prdDetails.GetProductRef().productTypeId = (fnInfos.prdType == "AMP") ? ProductType::S4CS1L2AmpProductTypeId :
                                            ProductType::S4CS1L2CoheProductTypeId;
    this->m_prdDetails.GetProductRef().satId = (int)Satellite::Sentinel1;
    this->m_prdDetails.GetProductRef().created = fnInfos.endDate;
    // set the tile to tileId (if present in the name) or to orbitId otherwise
    this->m_prdDetails.GetProductRef().tiles = QStringList({fnInfos.tileId});
    this->m_prdDetails.GetProductRef().name = QFileInfo(productPath).baseName();

    m_bValid = true;
}

bool S1L2ProductHelper::IsIntendedFor(const QString &productPath)
{
    for(const FileNameInfosExtractor fnExt: fnInfoExtractors) {
        if (fnExt.IsIntended(productPath)) {
            return true;
        }
    }

    return false;
}

bool S1L2ProductHelper::IsIntendedFor(const ProductType &prdType)
{
    return (prdType == ProductType::S4CS1L2AmpProductTypeId ||
            prdType == ProductType::S4CS1L2CoheProductTypeId);
}

QMap<QString, QString> S1L2ProductHelper::GetProductFilesByTile(const QString &substrFilter, bool isQi)
{
    if( m_prdDetails.GetProduct().name.contains(substrFilter) && !isQi && this->m_prdDetails.GetProductRef().tiles.size() > 0) {
        return {{this->m_prdDetails.GetProductRef().tiles[0], m_prdDetails.GetProduct().fullPath}};
    }
    return {};
}

bool S1L2ProductHelper::GetFileNameInfosExtractor(const QString &productPath, FileNameInfosExtractor &extractor) {
    for(const FileNameInfosExtractor fnExt: fnInfoExtractors) {
        if (fnExt.IsIntended(productPath)) {
            extractor = fnExt;
            return true;
        }
    }
    return false;
}


bool FileNameInfosExtractor::IsIntended(const QString &productPath) const {
    QFileInfo qfileInfo(productPath);
    const QString &name = qfileInfo.baseName();

    QRegExp rx(regex);
    if (rx.indexIn(name) == -1) {
        return false;
    }
    return true;
}

FileNameInfosType FileNameInfosExtractor::ExtractInfos(const QString &productPath) {
    FileNameInfos fnInfos;
    QRegExp rx(regex);
    if (rx.indexIn(productPath) == -1) {
        return fnInfos;
    }
    const QStringList &list = rx.capturedTexts();
    fnInfos.prdType = list.at(regexTypeIdx);

    bool hasSecondDateField = true;
    // Normalize V1 and V2 fields
    if (fnInfos.prdType == "BCK") {
        fnInfos.prdType = "AMP";
        hasSecondDateField = false;
    } else if (fnInfos.prdType == "COH") {
        fnInfos.prdType = "COHE";
    }

    // extract min/max dates
    const QString &minDateStr = list.at(regexDateIdx);
    fnInfos.startDate = QDateTime::fromString(minDateStr, "yyyyMMddThhmmss");
    if (hasSecondDateField) {
        const QString &maxDateStr = list.at(regexDate2Idx);
        fnInfos.endDate = QDateTime::fromString(maxDateStr, "yyyyMMddThhmmss");
        if (fnInfos.endDate < fnInfos.startDate) {
            // switch the dates if they are inversed
            fnInfos.startDate = fnInfos.endDate;
            fnInfos.endDate = QDateTime::fromString(minDateStr, "yyyyMMddThhmmss");
        }
    } else {
        fnInfos.endDate = fnInfos.startDate;
    }
    fnInfos.orbitId = list.at(regexOrbitIdx);
    if (regexTileIdx != -1) {
        fnInfos.tileId = list.at(regexTileIdx);
    } else {
        fnInfos.tileId = fnInfos.orbitId;
    }

    return fnInfos;
}
