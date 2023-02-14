#include "generichighlevelproducthelper.h"
#include "logger.hpp"

#include <QDirIterator>
#include <QTextStream>

using namespace orchestrator::products;

#define EMPTY_TILE_ID           "00000"

QMap<QString, ProductType> GenericHighLevelProductHelper::m_mapHighLevelProductTypeInfos = {
    {"_L3A_", ProductType::L3AProductTypeId},
    {"_L3B_", ProductType::L3BProductTypeId},
    {"_L3C_", ProductType::L3CProductTypeId},
    {"_L3D_", ProductType::L3DProductTypeId},
    {"_L3E_", ProductType::L3EProductTypeId},
    {"_L4A_", ProductType::L4AProductTypeId},
    {"_L4B_", ProductType::L4BProductTypeId},
    {"_S4C_L4A_", ProductType::S4CL4AProductTypeId},
    {"_S4C_L4B_", ProductType::S4CL4BProductTypeId},
    {"_S4C_L4C_", ProductType::S4CL4CProductTypeId},
    {"S4S_L4A_", ProductType::S4SCropTypeMappingProductTypeId},
    {"_S4S_YIELDFEAT_", ProductType::S4SYieldFeatProductTypeId}
};

GenericHighLevelProductHelper::GenericHighLevelProductHelper()
{

}

GenericHighLevelProductHelper::GenericHighLevelProductHelper(const ProductDetails &product) :
    ProductHelper(product)
{
    SetProduct(product.GetProduct().fullPath);
}

GenericHighLevelProductHelper::GenericHighLevelProductHelper(const QString &product) :
    ProductHelper(product)
{
    SetProduct(product);
}

void GenericHighLevelProductHelper::SetProduct(const QString &product)
{
    QString prodName = product.trimmed();
    // remove any / at the end of the product name
    if(prodName.endsWith('/')) {
        prodName.chop( 1 );
    }
    ProductHelper::SetProduct(prodName);
    m_tileDirs.clear();

    // invalid product
    if (prodName.size() == 0) {
        return;
    }
    QFileInfo info(prodName);
    const QString &name = info.baseName();

    QMap<QString, ProductType>::const_iterator i;
    for (const auto &key: m_mapHighLevelProductTypeInfos.keys()) {
        if(name.contains(key)) {
            m_prdDetails.GetProductRef().productTypeId = m_mapHighLevelProductTypeInfos[key];
            break;
        }
    }

    QDateTime startDate, endDate;
    if (!GetProductAcqDatesFromName(name, startDate, endDate)) {
        return;
    }
    if (endDate < startDate) {
        QDateTime temp = startDate;
        startDate = endDate;
        endDate = temp;
    }
    m_prdDetails.GetProductRef().created = endDate;
    m_prdDetails.GetProductRef().name = name;
    m_startDate = startDate;
    m_endDate = endDate;

    m_bValid = true;
}

bool GenericHighLevelProductHelper::IsIntendedFor(const QString &product)
{
    if (product.size() == 0) {
        return false;
    }

    QFileInfo info(product);
    const QString &name = info.baseName();
    for (const auto &key: m_mapHighLevelProductTypeInfos.keys()) {
        if(name.contains(key)) {
            return true;
        }
    }
    return false;
}

bool GenericHighLevelProductHelper::IsIntendedFor(const ProductType &prdType)
{
    return m_mapHighLevelProductTypeInfos.values().contains(prdType);
}

bool GenericHighLevelProductHelper::HasSatellite()
{
    return false;
}

bool GenericHighLevelProductHelper::HasMasks()
{
    switch (m_prdDetails.GetProduct().productTypeId) {
        case ProductType::L3AProductTypeId:
        case ProductType::L3BProductTypeId:
        case ProductType::L3CProductTypeId:
        case ProductType::L3DProductTypeId:
        case ProductType::L3EProductTypeId:
        case ProductType::L4AProductTypeId:
        case ProductType::L4BProductTypeId:
            return true;
        default:
            return false;
    }
}

bool GenericHighLevelProductHelper::IsRaster()
{
    switch (m_prdDetails.GetProduct().productTypeId) {
        case ProductType::L3AProductTypeId:
        case ProductType::L3BProductTypeId:
        case ProductType::L3CProductTypeId:
        case ProductType::L3DProductTypeId:
        case ProductType::L3EProductTypeId:
        case ProductType::L4AProductTypeId:
        case ProductType::L4BProductTypeId:
            return true;
        default:
            return false;
    }
}

bool GenericHighLevelProductHelper::HasTiles()
{
    switch (m_prdDetails.GetProduct().productTypeId) {
        case ProductType::L3AProductTypeId:
        case ProductType::L3BProductTypeId:
        case ProductType::L3CProductTypeId:
        case ProductType::L3DProductTypeId:
        case ProductType::L3EProductTypeId:
        case ProductType::L4AProductTypeId:
        case ProductType::L4BProductTypeId:
            return true;
        default:
            return false;
    }
}

QStringList GenericHighLevelProductHelper::GetProductFiles(const QString &pattern)
{
    return GetProductFilesByTile(pattern, false).values();
}

QStringList GenericHighLevelProductHelper::GetProductMasks(const QString &pattern)
{
    return GetProductFilesByTile(pattern, true).values();
}

QMap<QString, QString> GenericHighLevelProductHelper::GetProductFilesByTile(const QString &pattern, bool isQi)
{
    QMap<QString, QString> retMap;
    ExtractTilesDirs();
    for(const auto &tileId : m_tileDirs.keys())
    {
        const QString &tileDir = m_tileDirs.value(tileId);
        const QString &file = GetProductTileFile(tileDir, pattern, isQi);
        if (file.size() > 0) {
            retMap.insert(tileId, file);
        }
    }
    return retMap;
}

bool GenericHighLevelProductHelper::HasValidStructure()
{
    // invalid product
    if (!m_bValid) {
        return false;
    }
    // not a known structure, we assume is valid
    if (!HasTiles()) {
        return true;
    }

    QDirIterator it(m_prdDetails.GetProduct().fullPath, QStringList() << "*.xml", QDir::Files);
    if (!it.hasNext()) {
        return false;
    }

    QDirIterator itTiles(m_prdDetails.GetProduct().fullPath + "/TILES/", QStringList() << "*", QDir::Dirs);
    if (!itTiles.hasNext()) {
        return false;
    }
    bool bAtLeastOneValidTile = false;
    while(itTiles.hasNext()) {
        const QString &tileDir = itTiles.next();
        // get the dir name
        const QString &tileDirName = QFileInfo(tileDir).fileName();
        if(tileDirName == "." || tileDirName == "..") {
            continue;
        }
        const QString &tileId = GetTileId(tileDir);
        if(tileId != EMPTY_TILE_ID) {
            m_tileDirs[tileId] = tileDir;
        }

        bool bValidTile = true;
        // we should have some TIF files in this folder
        QDirIterator itImgData(tileDir + "/IMG_DATA/", QStringList() << "*.TIF", QDir::Files);
        if (!itImgData.hasNext()) {
            bValidTile = false;
        }
        // check if we have some files here (we cannot assume they are TIF or other format)
        QDirIterator itQiData(tileDir + "/QI_DATA/", QStringList() << "*.*", QDir::Files);
        if (!itQiData.hasNext()) {
            bValidTile = false;
        }
        if(bValidTile) {
            bAtLeastOneValidTile = true;
            break;
        }
    }
    return bAtLeastOneValidTile;
}

void GenericHighLevelProductHelper::ExtractTilesDirs()
{
    // if already extracted, do nothing
    if(!HasTiles() || m_tileDirs.size() > 0) {
        return;
    }

    QString tilesDir = m_prdDetails.GetProduct().fullPath + "/TILES/";
    QDirIterator it(tilesDir, QStringList() << "*", QDir::Dirs);
    while (it.hasNext()) {
        const QString &subDir = it.next();
       // get the dir name
        const QString &tileDirName = QFileInfo(subDir).fileName();
        if(tileDirName == "." || tileDirName == "..") {
            continue;
        }
        const QString &tileId = GetTileId(subDir);
        if(tileId != EMPTY_TILE_ID) {
            m_tileDirs[tileId] = subDir;
        }
    }
}

bool GenericHighLevelProductHelper::GetProductAcqDatesFromName(const QString &productName, QDateTime &minDate, QDateTime &maxDate) {
    QString prodName = productName.trimmed();
    // remove any / at the end of the product name
    if(prodName.endsWith('/')) {
        prodName.chop( 1 );
    }
    // if the name is actually the full path, then keep only the last part to avoid interpreting
    // incorrectly other elements in path
    const QStringList &els = prodName.split( "/" );
    prodName = els.value( els.length() - 1 );

    const QStringList &pieces = prodName.split("_");
    for (int i = 0; i < pieces.size(); i++) {
        const QString &piece = pieces[i];
        if(piece.length() == 0) // is it possible?
            continue;
        bool bIsInterval = (piece[0] == 'V');
        bool bIsAcquisition = (piece[0] == 'A');
        if(bIsInterval || bIsAcquisition) {
            QString timeFormat("yyyyMMdd");
            // Remove the A or V from the name
            QString trimmedPiece = piece.right(piece.size()-1);
            // check if the date is in formate yyyyMMddTHHmmss (ISO 8601)
            if(trimmedPiece.length() == 15 && trimmedPiece[8] == 'T') {
                // use this format, more precise
                timeFormat = "yyyyMMddTHHmmss";
            }
            minDate = QDateTime::fromString(trimmedPiece, timeFormat);
            maxDate = minDate;
            if(bIsInterval && (i+1) < pieces.size()) {
                const QDateTime tmpDate = QDateTime::fromString(pieces[i+1], timeFormat);
                // Do not make assumption min is first - choose the min between them
                if (tmpDate <= minDate) {
                    minDate = tmpDate;
                }
                if (tmpDate >= maxDate) {
                    maxDate = tmpDate;
                }
            }
            return true;
        }
    }

    Logger::error(QStringLiteral("Cannot extract acquisition dates from product %1").arg(productName));
    return false;
}

QString GenericHighLevelProductHelper::GetTileId(const QString &tileFilePath)
{
    // First remove the extension
    QFileInfo info(tileFilePath);
    const QString &fileNameWithoutExtension = info.completeBaseName();
    // Split the name by "_" and search the part having _Txxxxx (_T followed by 5 characters)
    const QStringList &pieces = fileNameWithoutExtension.split("_");
    for (const QString &piece : pieces) {
        int pieceLen = piece.length();
        if (((pieceLen == 6) || (pieceLen == 7)) && (piece.at(0) == 'T')) {
            // return the tile without the 'T'
            return piece.right(pieceLen-1);
        }
    }

    return QString(EMPTY_TILE_ID);
}

QStringList GenericHighLevelProductHelper::GetTileIdsFromProduct() {
    ExtractTilesDirs();
    return m_tileDirs.keys();
}

QString GenericHighLevelProductHelper::GetProductTileFile(const QString &tileDir, const QString &fileNameSubstrFilter, bool isQiData) {
    // get the dir name
    QString tileFolder = tileDir + (isQiData ? "/QI_DATA/" : "/IMG_DATA/");
    QString filesFilter = fileNameSubstrFilter.size() > 0 ? QString("*_%1_*.TIF").arg(fileNameSubstrFilter) :
                                                            QString("*.TIF");
    QDirIterator it(tileFolder, QStringList() << filesFilter, QDir::Files);
    QStringList listFoundFiles;
    while (it.hasNext()) {
        listFoundFiles.append(it.next());
    }
    if(listFoundFiles.size() == 0) {
        return "";
    } else if (listFoundFiles.size() == 1) {
        return listFoundFiles[0];
    } else {
        // we have several files
        QString tileDirName = QFileInfo(tileDir).fileName();
        QStringList pieces = tileDirName.split("_");
        QString fileName;
        int curPiece = 0;
        // We have a tilename something like S2AGRI_L3A_V2013xxx_2013yyyy
        // the file that we are looking for is something like S2AGRI_L3A_CM_V2013xxx_2013yyyy.zzz
        // if there are multiple files with different timestamps, we get the one that has
        // the same timestamp as the tile/product
        int numPieces = pieces.size();
        for (int i = 0; i<numPieces; i++) {
            const QString &piece = pieces[i];
            fileName += piece;

            // we do not add the _ if it is the last piece
            if(curPiece != numPieces - 1)
                fileName += "_";

            // add the identifier after the product TYPE
            if(curPiece == 1)
                fileName += fileNameSubstrFilter + "_";
            curPiece++;
        }
        QString filePath = tileDir + (isQiData ? "/QI_DATA/" : "/IMG_DATA/") + fileName + ".TIF";
        if(listFoundFiles.contains(filePath)) {
            return filePath;
        }
        // otherwise return the last in the list as it might possibly have the latest date
        return listFoundFiles[listFoundFiles.size()-1];
    }
}

QString GenericHighLevelProductHelper::GetIppFile() {
    QString auxDir = m_prdDetails.GetProduct().fullPath + "/AUX_DATA/";
    QDirIterator it(auxDir, QStringList() << "*_IPP_*.xml", QDir::Files);
    // get the last strata shape file found
    while (it.hasNext()) {
        return it.next();
    }
    return "";
}

QStringList GenericHighLevelProductHelper::GetSourceProducts(const QString &tileFilter) {
    QStringList retList;

    const QString ippXmlFile = GetIppFile();
    if(ippXmlFile.length() == 0) {
        // incomplete product
        return retList;
    }
    const QString startTag((tileFilter.length() == 0) ? "<XML_0>" : "<XML_");
    const QString endTag  ((tileFilter.length() == 0) ? "</XML_0>" : "</XML_");
    bool bFullTag = (tileFilter.length() == 0);
    QFile inputFile(ippXmlFile);
    if (inputFile.open(QIODevice::ReadOnly))
    {
       QTextStream in(&inputFile);
       while (!in.atEnd()) {
           const QString &line = in.readLine();
           int startTagIdx = line.indexOf(startTag);
           if(startTagIdx >= 0) {
               int endTagIdx = line.indexOf(endTag);
               if(endTagIdx > startTagIdx) {
                   if ((tileFilter.length() == 0) || (line.indexOf(tileFilter) >= 0)) {
                       int startIdx;
                       if (bFullTag) {
                            startIdx = startTagIdx + startTag.length();
                       } else {
                           // we need to search for the closing of xml i.e the ">" character
                           int closingBracketIdx = line.indexOf('>', startTagIdx);
                           if(closingBracketIdx > startTagIdx) {
                               startIdx = closingBracketIdx+1;
                           } else {
                               continue;
                           }
                       }
                       // check for the new values of startIdx
                       if(endTagIdx > startIdx) {
                            retList.append(line.mid(startIdx, endTagIdx - startIdx));
                       }
                   }
               }
           }
       }
       inputFile.close();
    }

    return retList;
}


bool GenericHighLevelProductHelper::HasSource(const QString &srcPrd) {
    const QString &ippXmlFile = GetIppFile();
    if(ippXmlFile.length() == 0) {
        // incomplete product
        return false;
    }
    const QStringList &lines = GetTextFileLines(ippXmlFile, srcPrd, 1);
    return (lines.size() == 1);
}

QStringList GenericHighLevelProductHelper::GetTextFileLines(const QString &filePath, const QString &lineFilterStr, int maxLines) {
    QFile inputFile(filePath);
    QStringList lines;
    if (inputFile.open(QIODevice::ReadOnly))
    {
        QTextStream in(&inputFile);
        while (!in.atEnd())
        {
            const QString &line = in.readLine();
            if (lineFilterStr.size() == 0 || line.contains(lineFilterStr)) {
                lines.append(line);
                if (maxLines > 0 && lines.size() >= maxLines) {
                    break;
                }
            }
        }
        inputFile.close();
    }
    return lines;
}

