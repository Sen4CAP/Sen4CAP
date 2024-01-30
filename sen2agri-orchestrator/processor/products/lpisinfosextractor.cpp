#include "lpisinfosextractor.h"
#include "logger.hpp"

using namespace orchestrator::products;

// TODO: To remove m_optParcelsPattern and m_sarParcelsPattern
LpisInfosExtractor::LpisInfosExtractor():
    m_idFieldName("NewID"), m_optParcelsPattern(".*_buf_5m.shp"), m_sarParcelsPattern(".*_buf_10m.shp"),
    m_csvPattern("decl_.*_\\d{4}\\.csv"), m_gpkgPattern("decl_.*_\\d{4}\\.gpkg"),
    m_tiledRastersPattern("decl_.*_\\d{4}_(?<S2TILE>\\d{2}[A-Z]{3})_S(?<SAT>[1-2])\\.tif"),
    m_bufferedShapesPattern("decl_.*_\\d{4}_(?<EPSG>\\d{4,8})_buf_(?<BUFFER>5|10)m\\.shp")
{
}

void LpisInfosExtractor::SetLpisProducts(const ProductList &lpisPrds)
{
    if (lpisPrds.size() == 0) {
        return;
    }

    QRegularExpression reOpt(m_optParcelsPattern);
    QRegularExpression reSar(m_sarParcelsPattern);
    QRegularExpression reCsv(m_csvPattern);
    QRegularExpression reGpkg(m_gpkgPattern);
    QRegularExpression reTileRasters(m_tiledRastersPattern);
    QRegularExpression reBufShps(m_bufferedShapesPattern);

    QRegularExpressionMatch reMatch;

    for(const Product &lpisPrd: lpisPrds) {
        // ignore LPIS products from a year where we already added an LPIS product newer
        QMap<int, LpisInfos>::const_iterator i = m_lpisInfos.find(lpisPrd.created.date().year());
        if (i != m_lpisInfos.end()) {
            if (lpisPrd.inserted < i.value().insertedDate) {
                Logger::info(QStringLiteral("LPIS product %1 ignored as there is another one more recent than it for the same year %2").
                             arg(lpisPrd.fullPath).arg(i.value().productPath));
                continue;
            }
        }

        // If the year is >= 2019, then use LAEA for AMP and COHE and no matter which other for NDVI
        //const QString &prdLpisPath = lpisPrds[lpisPrds.size()-1].fullPath;
        QDir directory(lpisPrd.fullPath);
        Logger::info(QStringLiteral("MDB1: Extracting files for LPIS product %1").
                     arg(lpisPrd.fullPath));


        const QStringList &dirFiles = directory.entryList(QStringList() << "*.shp" << "*.csv" << "*.gpkg" << "*.tif" ,QDir::Files);
        LpisInfos lpisInfo;
        foreach(const QString &fileName, dirFiles) {
            // we don't want for optical products the LAEA projection
            if (reOpt.match(fileName).hasMatch() && (lpisInfo.defaultOptGeomShpPath.size() == 0)) {
                lpisInfo.defaultOptGeomShpPath = directory.filePath(fileName);
            }
            // LAEA projection have priority for 10m buffer
            if (reSar.match(fileName).hasMatch() && lpisInfo.defaultSarGeomShpPath.size() == 0) {
                lpisInfo.defaultSarGeomShpPath = directory.filePath(fileName);
            }

            if (reCsv.match(fileName).hasMatch() && lpisInfo.csvPath.size() == 0) {
                lpisInfo.csvPath = directory.filePath(fileName);

            }
            if (reGpkg.match(fileName).hasMatch() && lpisInfo.gpkgPath.size() == 0) {
                lpisInfo.gpkgPath = directory.filePath(fileName);
            }

            reMatch = reTileRasters.match(fileName);
            if (reMatch.hasMatch()) {
                const QString &tile = reMatch.captured("S2TILE");
                const QString &sat = reMatch.captured("SAT");
                if (!sat.isEmpty() && !tile.isEmpty()) {
                    if (sat == "1")
                        lpisInfo.s1TiledRasters[tile].append(directory.filePath(fileName));
                    else if (sat == "2")
                        lpisInfo.s2TiledRasters[tile].append(directory.filePath(fileName));
                }
            }

            reMatch = reBufShps.match(fileName);
            if (reBufShps.match(fileName).hasMatch()) {
                const QString &epsg = reMatch.captured("EPSG");
                const QString &buffer = reMatch.captured("BUFFER");
                if (!epsg.isEmpty() && !buffer.isEmpty()) {
                    if (buffer == "5")
                        lpisInfo.buf5mShapes[epsg].append(directory.filePath(fileName));
                    else if (buffer == "10")
                        lpisInfo.buf10mShapes[epsg].append(directory.filePath(fileName));
                }
            }
        }
        if (lpisInfo.defaultOptGeomShpPath.size() != 0 &&
                lpisInfo.defaultSarGeomShpPath.size() != 0) {
            lpisInfo.productName = lpisPrd.name;
            lpisInfo.productPath = lpisPrd.fullPath;
            lpisInfo.productDate = lpisPrd.created;
            lpisInfo.insertedDate = lpisPrd.inserted;
            m_lpisInfos[lpisInfo.productDate.date().year()] = lpisInfo;
            Logger::info(QStringLiteral("Using LPIS %1 for year %2").arg(lpisPrd.fullPath).arg(lpisInfo.productDate.date().year()));
        } else {
            Logger::info(QStringLiteral("LPIS infos couldn't be extracted from path %1").arg(lpisPrd.fullPath));
        }
    }
}

QMap<int, LpisInfos> LpisInfosExtractor::GetLpisInfos() const
{
    return m_lpisInfos;
}
