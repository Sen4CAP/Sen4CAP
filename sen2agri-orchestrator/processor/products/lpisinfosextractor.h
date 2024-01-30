#ifndef LPISINFOSEXTRACTOR_H
#define LPISINFOSEXTRACTOR_H

#include "model.hpp"

namespace orchestrator
{
namespace products
{
typedef struct LpisInfos {
    QDateTime productDate;
    QDateTime insertedDate;

    // LPIS informations
    QString productName;
    QString productPath;
    QString defaultOptGeomShpPath;
    QString defaultSarGeomShpPath;
    QString csvPath;
    QString gpkgPath;
    QMap<QString, QString> s2TiledRasters;  // TileName to Raster paths
    QMap<QString, QString> s1TiledRasters;  // TileName to Raster paths

    QMap<QString, QString> buf5mShapes;  // EPSG to 5m Buffer shapefiles
    QMap<QString, QString> buf10mShapes;  // EPSG to 10m Buffer shapefiles


} LpisInfos;

class LpisInfosExtractor
{
public:
    LpisInfosExtractor();
    void SetLpisProducts(const ProductList &lpisPrds);
     QMap<int, LpisInfos> GetLpisInfos() const;

    void SetIdFieldName(const QString &idFieldName) { m_idFieldName = idFieldName; }
    void SetOptParcelsPattern(const QString &pattern) { m_optParcelsPattern = pattern; }
    void SetSarParcelsPattern(const QString &pattern) { m_sarParcelsPattern = pattern; }

    void SetCsvFilePattern(const QString &pattern) { m_csvPattern = pattern; }
    void SetGpkgFilePattern(const QString &pattern) { m_gpkgPattern = pattern; }
    void SetTiledRastersPattern(const QString &pattern) { m_tiledRastersPattern = pattern; }
    void SetBufferedShapefilesPattern(const QString &pattern) { m_bufferedShapesPattern = pattern; }

private:
    QMap<int, LpisInfos>  m_lpisInfos;
    QString m_idFieldName;
    QString m_optParcelsPattern;
    QString m_sarParcelsPattern;
    QString m_csvPattern;
    QString m_gpkgPattern;
    QString m_tiledRastersPattern;
    QString m_bufferedShapesPattern;
};

} // end of namespace products
} // // end of namespace orchestrator

#endif // LPISINFOSEXTRACTOR_H
