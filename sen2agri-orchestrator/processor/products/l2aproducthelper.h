#ifndef L2APRODUCTHELPER_H
#define L2APRODUCTHELPER_H

#include "producthelper.h"

#include "QFileInfo"
#include "QRegularExpression"

namespace orchestrator
{
namespace products
{

typedef struct ProductNamePatternInfos {

    ProductNamePatternInfos() {
        satelliteIdType = Satellite::Invalid;
        extractInfosFromParentFolder = false;
        tileIdxInName = dateIdxInName = -1;
        isDoy = false;
    }
    ProductNamePatternInfos(const Satellite &sat, const QRegularExpression &pat, bool extrFromParent,
                            const QRegularExpression &parentPat, int tileIdx, int dateIdx, bool doy, const
                            QString &dateFrmt)
        : satelliteIdType(sat), pattern(pat), extractInfosFromParentFolder(extrFromParent),
          parentFolderPattern(parentPat), tileIdxInName(tileIdx), dateIdxInName(dateIdx),
          isDoy(doy), dateFormat(dateFrmt) {
    }

    Satellite satelliteIdType;
    //Regext for the product name
    QRegularExpression pattern;
    // If set, this means that the regex for extracting infos (date, tile) is applied on parent dir name
    bool extractInfosFromParentFolder;
    QRegularExpression parentFolderPattern;

    // the idx of the tile in the tile name regex
    int tileIdxInName;
    // position of the date group in the date regex
    int dateIdxInName;
    bool isDoy;
    // patter for the date to be extracted into a QDateTime object
    QString dateFormat;

} ProductNamePatternInfos;

class L2AProductHelper : public ProductHelper
{
public:
    L2AProductHelper();
    L2AProductHelper(const ProductDetails &product);
    L2AProductHelper(const QString &product);

    virtual void SetProduct(const ProductDetails &product);
    virtual void SetProduct(const QString &product);

    virtual bool HasSatellite()  { return true; }
    virtual bool HasMasks()  { return false; }
    virtual bool IsRaster()  { return true; }
    virtual bool HasTiles()  { return true; }

    virtual QStringList GetProductFiles(const QString & fileNameSubstrFilter = "");
    virtual QStringList GetTileIdsFromProduct();
    virtual QStringList GetProductMetadataFiles();

    static bool IsIntendedFor(const QString &product);
    static bool IsIntendedFor(const ProductType &productType);

    QStringList GetRasterFilesFromDir(const QString &dirPath, const QString &fileNameSubstrFilter = "",
                                      const QString &prefix = "*", const QString &suffix = "*", const QStringList &extensions = {".jp2", ".tif"});

private:
    bool ExtractInfosFromPath(const QString &path);
    bool ExtractInfosFromMatch(const ProductNamePatternInfos &infos, const QRegularExpressionMatch &match);
    QStringList FindProductMetadataFiles();
    static QString GetNameFromPath(const QString &path, const QFileInfo &pathFileInfo);

    QStringList GetS2ProductFiles(const QString & fileNameSubstrFilter, const QString &metaFile);
    QStringList GetL8ProductFiles(const QString & fileNameSubstrFilter, const QString &metaFile);

    static QList<ProductNamePatternInfos> productNamesPatternInfos;
    static QList<ProductNamePatternInfos> tileMetaFilePatternInfos;

    QStringList m_metadataFiles;

    // infos about the matched
    ProductNamePatternInfos m_matchedPattern;
};

} // end of namespace products
} // // end of namespace orchestrator

#endif // L2APRODUCTHELPER_H
