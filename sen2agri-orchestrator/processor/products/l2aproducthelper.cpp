#include "l2aproducthelper.h"

#include "QDirIterator"

using namespace orchestrator::products;

#define SECONDS_IN_DAY 86400

QList<ProductNamePatternInfos> L2AProductHelper::productNamesPatternInfos = {
    {Satellite::Sentinel2, QRegularExpression(R"(S2[A-D]_MSIL2A_(\d{8}T\d{6})_N\d+_R\d+_T(\d{2}\w{3})_\d{8}T\d{6}(?:.SAFE)?)"), false, QRegularExpression(""), 2, 1, false, "yyyyMMddThhmmss"},
    {Satellite::Landsat8, QRegularExpression(R"(LC08_[A-Z0-9]+_(\d{6})_(\d{8})_\d{8}_\d{2}_[A-Z0-9]{2})"), false, QRegularExpression(""), 1, 2, false, "yyyyMMdd"},
    {Satellite::Sentinel2, QRegularExpression(R"(S2[A-D]_OPER_PRD_MSIL2A_.+_V(\d{8}T\d{6})_.+(?:.SAFE)?)"), false, QRegularExpression(""), -1, 1, false, "yyyyMMddThhmmss"},
    {Satellite::Landsat8,QRegularExpression( R"(LC8(\d{6})(\d{7})[A-Z]{3}\d{2}_L2A)"), false, QRegularExpression(""), 1, 2, true, ""},
};

//      - product type
//      - pattern containing index of the date in the file name of the L2A product assuming _ separation
// NOTE: The key of the map is the string as it appears in the file name of the product (S2, L8, SPOT4 etc.) and
//       not the name of the satellite as it appears inside the file metadata of product (that can be SENTINEL-2A, LANDSAT_8 etc.)
/* static */
QList<ProductNamePatternInfos> L2AProductHelper::tileMetaFilePatternInfos =
{
    // MAJA product name ex. SENTINEL2A_20180124-110332-457_L2A_T30TYP_C_V1-0_MTD_ALL.xml
    {Satellite::Sentinel2, QRegularExpression(R"(SENTINEL2[A-D]_(\d{8}-\d{6})-(\d{3})_L2A_T(\d{2}[A-Za-z]{3})_.*_MTD_ALL\.xml)"), false, QRegularExpression(""), 3, 1, false, "yyyyMMdd-hhmmss"},
    // Sen2Cor product name ex. S2A_MSIL2A_20200205T100211_N0214_R122_T33UVQ_20200205T114355.SAFE
    {Satellite::Sentinel2, QRegularExpression(R"(MTD_MSIL2A\.xml)"), true, QRegularExpression(R"(S2[A-D]_MSIL2A_(\d{8}T\d{6})_.*T(\d{2}[A-Z]{3})_.*\.SAFE)"), 2, 1, false, "yyyyMMddThhmmss"},
    {Satellite::Landsat8, QRegularExpression(R"(LANDSAT8-.*_(\d{8})-.*_L2A_((\d{3})-(\d{3}))_.*_MTD_ALL.xml)"), false, QRegularExpression(""), 2, 1, false, "yyyyMMdd"},
    // L8 (MACCS and MAJA) product name ex. L8_TEST_L8C_L2VALD_196030_20191003.HDR
    {Satellite::Landsat8, QRegularExpression(R"(L8_.*_L8C_L2VALD_(\d{6})_(\d{8}).HDR)"), false, QRegularExpression(""), 1, 2, false, "yyyyMMdd"},
    // MACCS product name ex. S2A_OPER_SSC_L2VALD_29SMR____20151209.HDR
    {Satellite::Sentinel2, QRegularExpression(R"(S2[A-D]_OPER_SSC_L2VALD_(\d{2}[A-Z]{3})_.*_(\d{8}).HDR)"), false, QRegularExpression(""), 1, 2, false, "yyyyMMdd"}
//    {Satellite::Spot4, QRegularExpression(R"(SPOT4_.*_.*_(\d{6})_.*\.xml)"), false, QRegularExpression(""), -1, 1, false, "yyyyMMdd"}, //SPOT4_*_*_<DATE>_*_*.xml
//    {Satellite::Spot5, QRegularExpression(R"(SPOT5_.*_.*_(\d{6})_.*\.xml)"), false, QRegularExpression(""), -1, 1, false, "yyyyMMdd"}, //SPOT5_*_*_<DATE>_*_*.xml
};

L2AProductHelper::L2AProductHelper()
{
}


L2AProductHelper::L2AProductHelper(const ProductDetails &product) :
    ProductHelper(product)
{
}

L2AProductHelper::L2AProductHelper(const QString &product) :
    ProductHelper(product)
{
    SetProduct(product);
}

bool L2AProductHelper::IsIntendedFor(const QString &product)
{
    QFileInfo info(product);
    const QString &name = GetNameFromPath(product, info);
    const QList<ProductNamePatternInfos> &pList = info.isFile() ? tileMetaFilePatternInfos : productNamesPatternInfos;
    for(const ProductNamePatternInfos &patternInfo: pList) {
        const QRegularExpressionMatch &match = patternInfo.pattern.match(name);
        if (match.hasMatch()) {
            if (patternInfo.extractInfosFromParentFolder) {
                const QString &parentName = info.absoluteDir().dirName();
                const QRegularExpressionMatch &matchParent = patternInfo.parentFolderPattern.match(parentName);
                if (!matchParent.hasMatch()) {
                    continue;
                }
            }
            return true;
        }
    }
    return false;
}

bool L2AProductHelper::IsIntendedFor(const ProductType &prdType)
{
    return (prdType == ProductType::L2AProductTypeId);
}

void L2AProductHelper::SetProduct(const ProductDetails &product)
{
    ProductHelper::SetProduct(product);
}

void L2AProductHelper::SetProduct(const QString &product)
{
    ProductHelper::SetProduct(product);
    m_metadataFiles.clear();
    m_matchedPattern = ProductNamePatternInfos();

    m_bValid = ExtractInfosFromPath(product);

    QFileInfo qfileInfo(product);
    if (qfileInfo.isFile()) {
        m_metadataFiles.append(m_prdDetails.GetProduct().fullPath);
    } else {

    }
    m_prdDetails.GetProductRef().productTypeId = ProductType::L2AProductTypeId;
}

QStringList L2AProductHelper::GetProductMetadataFiles() {
    // laizy loading of metadata files and if was not possible to extract them from the beginning
    if (m_metadataFiles.size() == 0) {
        m_metadataFiles = FindProductMetadataFiles();
    }
    return m_metadataFiles;
}

QStringList L2AProductHelper::FindProductMetadataFiles()
{
    // TODO: Not sure if we should support this mode.
    // If so, additional things should be added
    QFileInfo qfileInfo(m_prdDetails.GetProduct().fullPath);
    if(qfileInfo.isFile()) {
        return {m_prdDetails.GetProduct().fullPath};
    }

    // TODO: This part was imported from the original implementation but should be changed
    //      based on the defined patterns for metafiles
    QStringList result;
    for (const auto &file : QDir(m_prdDetails.GetProduct().fullPath).entryList({ "S2*_OPER_SSC_L2VALD_*.HDR",
                                                    "L8_*_L8C_L2VALD_*.HDR", "MTD_MSIL2A.xml",
                                                    "SPOT*.xml"},
                                                    QDir::Files)) {
        result.append(QDir::cleanPath(m_prdDetails.GetProduct().fullPath + QDir::separator() + file));
    }
    // Check for MAJA product
    if (result.isEmpty()) {
        QDirIterator it(m_prdDetails.GetProduct().fullPath, QDir::Dirs | QDir::NoDot | QDir::NoDotDot, QDirIterator::Subdirectories);
        while(it.hasNext()) {
            const QString &subDirName = it.fileName();
            if (subDirName.startsWith("SENTINEL2") || subDirName.startsWith("LANDSAT8")) {
                const QString &metaFileName = subDirName + "_MTD_ALL.xml";
                const QString &metaFilePath = QDir(it.filePath()).filePath(metaFileName);
                if(QFileInfo(metaFilePath).exists()) {
                    result.append(metaFilePath);
                    break;
                }
            }
            it.next();
        }
    }

    if (result.isEmpty()) {
        throw std::runtime_error(
            QStringLiteral("Unable to find an HDR or xml file in path %1. Unable to determine the product "
                "metadata file.").arg(m_prdDetails.GetProduct().fullPath).toStdString());
    }
    return result;
}

QStringList L2AProductHelper::GetTileIdsFromProduct()
{
    // laizy loading of metadata files and tile ids if was not possible to extract them from the beginning
    if (m_prdDetails.GetProduct().tiles.size() == 0) {
        if (m_metadataFiles.size() == 0) {
            m_metadataFiles = FindProductMetadataFiles();
            for(const QString &metaPath: m_metadataFiles) {
                ExtractInfosFromPath(metaPath);
            }
        }

    }
    return m_prdDetails.GetProduct().tiles;
}

QStringList L2AProductHelper::GetProductFiles(const QString & fileNameSubstrFilter)
{
    QStringList retList;
    if (!m_bValid) {
        return retList;
    }
    const QStringList &metaFiles = GetProductMetadataFiles();
    if (metaFiles.size() == 0) {
        return retList;
    }
    for (const QString &metaFile: metaFiles) {
        if (m_matchedPattern.satelliteIdType == Satellite::Sentinel2) {
            retList.append(GetS2ProductFiles(fileNameSubstrFilter, metaFile));
        } else if (m_matchedPattern.satelliteIdType == Satellite::Landsat8) {
            retList.append(GetL8ProductFiles(fileNameSubstrFilter, metaFile));
        }
    }
    return retList;
}

bool L2AProductHelper::ExtractInfosFromPath(const QString &path)
{
    QFileInfo info(path);
    const QString &name = GetNameFromPath(path, info);
    bool matchOk = false;
    bool isFile = info.isFile();
    if (!isFile) {
        m_prdDetails.GetProductRef().name = name;
    }
    const QList<ProductNamePatternInfos> &patternInfos = (isFile ? tileMetaFilePatternInfos : productNamesPatternInfos);
    for (const ProductNamePatternInfos &infos : patternInfos) {
        const QRegularExpressionMatch &match = infos.pattern.match(name);
        if(match.hasMatch()) {
            if (infos.extractInfosFromParentFolder) {
                const QString &parentName = info.absoluteDir().dirName();
                const QRegularExpressionMatch &matchParent = infos.parentFolderPattern.match(parentName);
                if (!matchParent.hasMatch()) {
                    continue;
                }
                matchOk = ExtractInfosFromMatch(infos, matchParent);
            } else {
                matchOk = ExtractInfosFromMatch(infos, match);
            }
            if (m_matchedPattern.satelliteIdType == Satellite::Invalid) {
                m_matchedPattern = infos;
            }
            break;
        }
    }
    return matchOk;
}

bool L2AProductHelper::ExtractInfosFromMatch(const ProductNamePatternInfos &infos, const QRegularExpressionMatch &match)
{
    m_prdDetails.GetProductRef().satId = (int)infos.satelliteIdType;
    if (infos.dateIdxInName >= 0) {
        const QString &extractedStr = match.captured(infos.dateIdxInName);
        QDateTime dateTime;
        if (infos.isDoy && extractedStr.size() == 7) {
            QDate date(extractedStr.left(4).toInt(), 1,1);
            date = date.addDays(extractedStr.right(3).toInt());
            dateTime = QDateTime (date);
        } else {
            dateTime = QDateTime::fromString(extractedStr, infos.dateFormat);
        }
        if(!m_prdDetails.GetProductRef().created.isValid() || m_prdDetails.GetProductRef().created > dateTime) {
            m_prdDetails.GetProductRef().created = dateTime;
        }
    }
    if (infos.tileIdxInName >= 0) {
        QString extractedStr = match.captured(infos.tileIdxInName);
        extractedStr.remove('-');
        if (!m_prdDetails.GetProductRef().tiles.contains(extractedStr)) {
            m_prdDetails.GetProductRef().tiles.append(extractedStr);
        }
    }

    return true;
}

QString L2AProductHelper::GetNameFromPath(const QString &path, const QFileInfo &pathFileInfo)
{
    QString name;
    if(pathFileInfo.isFile()) {
        name = pathFileInfo.fileName();
    } else if (pathFileInfo.isDir()) {
        if (path.trimmed().endsWith("/")) {
            name = pathFileInfo.dir().dirName();
        } else {
            name = pathFileInfo.fileName();
        }
    }
    return name;
}

QStringList L2AProductHelper::GetS2ProductFiles(const QString &fileNameSubstrFilter, const QString &metaFile)
{
    QStringList retList;
    QFileInfo info(metaFile);
    const QString &fileName = info.fileName();
    const QString &parentDir = info.dir().absolutePath();
    if (fileName == "MTD_MSIL2A.xml") {
        // Sen2Cor L2A product
        const QString &granulesPath = QDir::cleanPath(parentDir + QDir::separator() + "GRANULE");
        QDirIterator it(granulesPath, QDir::Dirs | QDir::NoDot | QDir::NoDotDot, QDirIterator::Subdirectories);
        while(it.hasNext()) {
            const QString &subDirName = it.fileName();
            if (subDirName.size() == 0) {
                it.next();
                continue;
            }
            const QString &granulePath = QDir::cleanPath(granulesPath + QDir::separator() + subDirName);
            const QString &imgPath = QDir::cleanPath(granulePath + QDir::separator() + "IMG_DATA");
            const QString &rasters10mPath = QDir::cleanPath(imgPath + QDir::separator() + "R10m");
            QString newFilter = fileNameSubstrFilter;
            if (newFilter.size() == 0) {
                newFilter = "_B*_*0m";
            }
            // First get the rasters in the R10m directory
            const QStringList &rasters10M = GetRasterFilesFromDir(rasters10mPath, newFilter);
            retList.append(rasters10M);
            if (fileNameSubstrFilter.size() > 0) {
                if (rasters10M.size() > 0) {
                    return retList;
                } else {
                    // try to get from 20m resolution
                    const QString &rasters20mPath = QDir::cleanPath(imgPath + QDir::separator() + "R20m");
                    const QStringList &rasters20M = GetRasterFilesFromDir(rasters20mPath, newFilter);
                    retList.append(rasters20M);
                    // no need to iterate other dirs
                    return retList;
                }
            } else {
                if (rasters10M.size() == 0) {
                    // we should throw an error here or simply return empty list
                    throw std::runtime_error(
                        QStringLiteral("Unsupported L2A Sen2Cor product without 10m res rasters in path %1").arg(m_prdDetails.GetProduct().fullPath).toStdString());
                }
                // get also the raster for 8A from 20 m
                const QString &rasters20mPath = QDir::cleanPath(imgPath + QDir::separator() + "R20m");
                // get all 20m res files and filter them one by one (to avoid multiple listing of files from disk, which might be a slow operation)
                const QStringList &rasters20M = GetRasterFilesFromDir(rasters20mPath);
                QStringList filters({"_B05", "_B06", "_B07", "_B8A", "_B11", "_B12"});
                for (const QString &rasterFile: rasters20M) {
                    for(const QString &filter: filters) {
                        if (QFileInfo(rasterFile).fileName().contains(filter)) {
                            retList.append(rasterFile);
                        }
                    }
                }
            }
            // no need to iterate other dirs
            return retList;
        }
    } else if(fileName.startsWith("SENTINEL2")) {
        // MAJA format - the files are in the same directory with the metadata file
        QString newFilter = "_FRE_" + fileNameSubstrFilter;
        newFilter.remove("0");
        const QStringList &rasters = GetRasterFilesFromDir(parentDir, newFilter, "*", "", {".tif"});
        retList.append(rasters);
    } else if(fileName.startsWith("S2")) {
        // MACCS format
        // TODO: Should we still support this format?
        QString resolutionFilter = "_R1";
        if (fileNameSubstrFilter.size() > 0) {
            if (!QStringList({"B02", "B03", "B04", "B08"}).contains(fileNameSubstrFilter)) {
                resolutionFilter = "_R2";
            }
        }
        const QString &baseName = info.baseName();
        const QString &rastersDir = QDir::cleanPath(parentDir + QDir::separator() + baseName + ".DBL.DIR");
        const QStringList &rasters = GetRasterFilesFromDir(rastersDir, "_FRE" + resolutionFilter);
        retList.append(rasters);
        if (fileNameSubstrFilter.size() == 0) {
            const QStringList &rasters20M = GetRasterFilesFromDir(rastersDir, "_FRE_R2");
            retList.append(rasters20M);
        }
    }
    return retList;
}

QStringList L2AProductHelper::GetL8ProductFiles(const QString & fileNameSubstrFilter, const QString &metaFile)
{
    QStringList retList;
    QFileInfo info(metaFile);
    const QString &fileName = info.fileName();
    const QString &parentDir = info.dir().absolutePath();

    // TODO: Here we should map the S2 bands to L8 bands and filter for FRE

    if(fileName.startsWith("L8")) {
        const QString &baseName = info.baseName();
        const QString &rastersDir = QDir::cleanPath(parentDir + QDir::separator() + baseName + ".DBL.DIR");
        const QStringList &rasters = GetRasterFilesFromDir(rastersDir, "_FRE.DBL");
        retList.append(rasters);

    } else if (fileName.startsWith("LANDSAT8")) {
        QString newFilter = "_FRE_" + fileNameSubstrFilter;
        newFilter.remove("0");
        const QStringList &rasters = GetRasterFilesFromDir(parentDir, newFilter);
        retList.append(rasters);
    }

    return retList;
}

QStringList L2AProductHelper::GetRasterFilesFromDir(const QString &dirPath, const QString &fileNameSubstrFilter,
                                                    const QString &prefix, const QString &suffix, const QStringList &extensions)
{
    QStringList result;
    QStringList filters;
    for (const QString &extension: extensions) {
        if(fileNameSubstrFilter.size() > 0) {
            filters << prefix + fileNameSubstrFilter + suffix + extension << prefix + fileNameSubstrFilter + suffix + extension.toUpper();
        } else {
            // prefix and suffix ignored if no filter
            filters << "*" + extension << "*" + extension.toUpper();
        }
    }
    for (const auto &file : QDir(dirPath).entryList(filters, QDir::Files)) {
        result.append(QDir::cleanPath(dirPath + QDir::separator() + file));
    }
    return result;
}
