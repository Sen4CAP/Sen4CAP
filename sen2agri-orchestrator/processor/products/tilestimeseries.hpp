#ifndef TILESTIMESERIESHELPER_H
#define TILESTIMESERIESHELPER_H

#include "model.hpp"
#include "productdetails.h"
namespace orchestrator
{
namespace tiletimeseries
{

typedef struct InfoTileFile {
    QString metaFile;
    Satellite satId;
    QString acquisitionDate;
    QStringList additionalFiles;    // optional
    ProductDetails srcPrdDetails;
    inline bool operator==(const InfoTileFile& rhs) {
        if(satId != rhs.satId) {
            return false;
        }
        if(acquisitionDate != rhs.acquisitionDate) {
            return false;
        }
        if(metaFile != rhs.metaFile) {
            return false;
        }
        if(additionalFiles.size() > 0 && rhs.additionalFiles.size() > 0 && additionalFiles[0] != rhs.additionalFiles[0]) {
            return false;
        }
        return true;
    }
} InfoTileFile;

typedef struct {
    QString tileMetaFile;
    ProductDetails srcPrdDetails;
} TileMetadataDetails;

typedef struct {
    QString tileId;
    QList<InfoTileFile> temporalTilesFileInfos;
    //the unique sattelites ids from the above list
    QList<Satellite> uniqueSatteliteIds;
    Satellite primarySatelliteId;
    QMap<Satellite, TileList> satIntersectingTiles;

    QList<TileMetadataDetails> GetTileTimeSeriesInfoFiles() const {
        QList<TileMetadataDetails> retList;
        for(const InfoTileFile &fileInfo: temporalTilesFileInfos) {
            retList.append({fileInfo.metaFile, fileInfo.srcPrdDetails});
        }
        return retList;
    }

    QStringList GetTemporalTileAcquisitionDates() const
    {
        QStringList retList;
        for(const InfoTileFile &fileInfo: temporalTilesFileInfos) {
            retList.append(fileInfo.acquisitionDate);
        }
        return retList;
    }

} TileTimeSeriesInfo;

class TilesTimeSeries
{
public:
    TilesTimeSeries();
    TilesTimeSeries(const QList<ProductDetails> &allPrdsDetails);
    void SetProducts(const QList<ProductDetails> &allPrdsDetails);

    void InitializeFrom(const TilesTimeSeries &timeSeries, const QMap<QString, QStringList> &mapSecSatFilesForPrimaryTile);

    QMap<QString, TileTimeSeriesInfo> GetTileTimeseriesInfos() const;
    QStringList GetTileIds() const;
    TileTimeSeriesInfo GetTileTimeSeriesInfo(const QString &tileId) const;
    QList<TileMetadataDetails> GetTileTimeSeriesInfoFiles(const QString &tileId) const;
    QList<Satellite> GetSatellites() const;
    Satellite GetPrimarySatellite() const;

    static Satellite GetPrimarySatelliteId(const QList<Satellite> &satIds);
    bool AddSatteliteIntersectingProducts(const QList<TileTimeSeriesInfo> &individualTileInfos,
                                          const QStringList &listSecondarySatLoadedProds, Satellite secondarySatId,
                                          TileTimeSeriesInfo &primarySatInfos);
    bool TemporalTileInfosHasFile(const TileTimeSeriesInfo &temporalTileInfo, const QString &filePath);

private:
    QMap<QString, TileTimeSeriesInfo> m_mapTiles;
    QList<Satellite> m_satIds;
    Satellite m_primarySatellite;
};

} // end of namespace products
} // // end of namespace orchestrator

#endif // TILESTIMESERIESHELPER_H
