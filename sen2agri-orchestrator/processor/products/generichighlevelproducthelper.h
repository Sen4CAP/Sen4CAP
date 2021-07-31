#ifndef GENERICHIGHLEVELPRODUCTHELPER_H
#define GENERICHIGHLEVELPRODUCTHELPER_H

#include "producthelper.h"

namespace orchestrator
{
namespace products
{
class GenericHighLevelProductHelper : public ProductHelper
{
public:
    GenericHighLevelProductHelper();
    GenericHighLevelProductHelper(const ProductDetails &product);
    GenericHighLevelProductHelper(const QString &product);
    virtual void SetProduct(const QString &product);

    virtual bool HasSatellite();
    virtual bool HasMasks();
    virtual bool IsRaster();
    virtual bool HasTiles();

    virtual QStringList GetProductFiles(const QString &pattern);
    virtual QStringList GetProductMasks(const QString &pattern);

    virtual QStringList GetTileIdsFromProduct();

    virtual bool HasValidStructure();

    static bool IsIntendedFor(const QString &product);
    static bool IsIntendedFor(const ProductType &prdType);

    QMap<QString, QString> GetTileDirectories() { return m_tileDirs; }
    QMap<QString, QString> GetProductFilesByTile(const QString &pattern, bool isQi = false);

    QStringList GetSourceProducts(const QString &tileFilter = "");
    bool HasSource(const QString &srcPrd);
private:
    static QMap<QString, ProductType> m_mapHighLevelProductTypeInfos;
    bool GetProductAcqDatesFromName(const QString &productName, QDateTime &minDate, QDateTime &maxDate);

    void ExtractTilesDirs();

    QString GetTileId(const QString &tileFilePath);
    QString GetProductTileFile(const QString &tileDir, const QString &fileNameSubstrFilter, bool isQiData);

    QString GetIppFile();
    QStringList GetTextFileLines(const QString &filePath, const QString &lineFilterStr = "", int maxLines = -1);

private :
    QMap<QString, QString> m_tileDirs;
};

} // end of namespace products
} // // end of namespace orchestrator

#endif // GENERICHIGHLEVELPRODUCTHELPER_H
