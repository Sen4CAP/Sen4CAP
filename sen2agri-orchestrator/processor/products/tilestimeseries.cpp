#include "tilestimeseries.hpp"

#include "producthelperfactory.h"

using namespace orchestrator::tiletimeseries;
using namespace orchestrator::products;

bool compareTileInfoFilesDates(const InfoTileFile& info1,const InfoTileFile& info2)
{
  return (info1.srcPrdDetails.GetProduct().created <
          info2.srcPrdDetails.GetProduct().created);
}

TilesTimeSeries::TilesTimeSeries()
{

}

// TODO: Move this to another location (See also ProductHelper)
Satellite TilesTimeSeries::GetPrimarySatelliteId(const QList<Satellite> &satIds) {
    // Get the primary satellite id
    Satellite retSatId = Satellite::Sentinel2;
    if(satIds.contains(Satellite::Sentinel2)) {
        retSatId = Satellite::Sentinel2;
    } else if(satIds.size() >= 1) {
        // check if all satellites in the list are the same
        const Satellite &refSatId = satIds[0];
        bool bAllSameSat = true;
        for(const Satellite &satId: satIds) {
            if(satId != refSatId) {
                bAllSameSat = false;
                break;
            }
        }
        if(bAllSameSat) {
            retSatId = satIds[0];
        }
    }

    return retSatId;
}


TilesTimeSeries::TilesTimeSeries(const QList<ProductDetails> &allPrdsDetails)
{
    SetProducts(allPrdsDetails);
}

QMap<QString, TileTimeSeriesInfo> TilesTimeSeries::GetTileTimeseriesInfos() const
{
    return m_mapTiles;
}

QStringList TilesTimeSeries::GetTileIds() const
{
    return m_mapTiles.keys();
}

TileTimeSeriesInfo TilesTimeSeries::GetTileTimeSeriesInfo(const QString &tileId) const
{
    QMap<QString, TileTimeSeriesInfo>::const_iterator i = m_mapTiles.find(tileId) ;
    if (i != m_mapTiles.end())
        return i.value();
    return TileTimeSeriesInfo();
}

QList<TileMetadataDetails>  TilesTimeSeries::GetTileTimeSeriesInfoFiles(const QString &tileId) const
{
    const TileTimeSeriesInfo &info = GetTileTimeSeriesInfo(tileId);
    return info.GetTileTimeSeriesInfoFiles();
}

QList<Satellite> TilesTimeSeries::GetSatellites() const
{
    return m_satIds;
}
Satellite TilesTimeSeries::GetPrimarySatellite() const
{
    return m_primarySatellite;
}

void TilesTimeSeries::SetProducts(const QList<ProductDetails> &allPrdsDetails)
{
    for(const ProductDetails &tileDetails: allPrdsDetails) {
        // get from the tile ID also the info about tile to determine satellite ID
        Satellite satId = (Satellite)tileDetails.GetProduct().satId;
        std::unique_ptr<ProductHelper> prdHelper = ProductHelperFactory::GetProductHelper(tileDetails);
        const QStringList &tileIds = tileDetails.GetProduct().tiles;
        if (tileIds.size() == 0)
            continue;
        const QString &tileId = tileIds.at(0);
        const QString &acqTimeStr = tileDetails.GetProduct().created.toString("yyyyMMdd");

        if(!m_satIds.contains(satId))
            m_satIds.append(satId);

        const QStringList &metaFiles = prdHelper->GetProductMetadataFiles();
        if(metaFiles.size() > 0 && m_mapTiles.contains(tileId)) {
            TileTimeSeriesInfo &infos = m_mapTiles[tileId];
            // The tile acquisition date should be filled later
            infos.temporalTilesFileInfos.append({metaFiles[0], satId, acqTimeStr,
                                                 {}, tileDetails});
            if(!infos.uniqueSatteliteIds.contains(satId))
                infos.uniqueSatteliteIds.append(satId);
        } else {
            TileTimeSeriesInfo infos;
            infos.tileId = tileId;
            // The tile acquisition date should be filled later
            infos.temporalTilesFileInfos.append({metaFiles[0], satId, acqTimeStr,
                                                 {}, tileDetails});
            infos.uniqueSatteliteIds.append(satId);
            m_mapTiles[tileId] = infos;
        }
    }

    // Get the primary satellite id
    m_primarySatellite = GetPrimarySatelliteId(m_satIds);

    // now update also the primary satelite id
    QMap<QString, TileTimeSeriesInfo>::iterator i;
    for (i = m_mapTiles.begin(); i != m_mapTiles.end(); ++i) {
        // this time, get a copy from the map and not the reference to info as we do not want to alter the input map
        TileTimeSeriesInfo &info = i.value();
        // fill the primary satellite ID here
        info.primarySatelliteId = m_primarySatellite;
    }
}

void TilesTimeSeries::InitializeFrom(const TilesTimeSeries &timeSeries, const QMap<QString, QStringList> &mapSecSatFilesForPrimaryTile)
{
    // Get the primary satellite id
    m_primarySatellite = timeSeries.GetPrimarySatellite();
    m_satIds = timeSeries.GetSatellites();

    // this time, get a copy from the map and not the reference to info as we do not want to alter the input map
    const QList<TileTimeSeriesInfo> &individualTileInfos = timeSeries.GetTileTimeseriesInfos().values();
    for (TileTimeSeriesInfo info : individualTileInfos) {
        bool isPrimarySatIdInfo = ((info.uniqueSatteliteIds.size() == 1) &&
                                   (m_primarySatellite == info.uniqueSatteliteIds[0]));
        if (isPrimarySatIdInfo) {
            for(Satellite satId: m_satIds) {
                // if is a secondary satellite id, then get the tiles from the database
                if(info.primarySatelliteId != satId) {
                    // get the metadata tiles for all found products intersecting the current tile
                    const QStringList &prdsTilesMetaFiles = mapSecSatFilesForPrimaryTile[info.tileId];

                    // add the intersecting products for this satellite id to the current info
                    AddSatteliteIntersectingProducts(individualTileInfos, prdsTilesMetaFiles, satId, info);
                }
            }
            // at this stage we know that the infos have only one unique satellite id
            // we keep in the returning map only the tiles from the primary satellite
            // Sort the products by date as maybe we added secondary products at the end
            qSort(info.temporalTilesFileInfos.begin(), info.temporalTilesFileInfos.end(), compareTileInfoFilesDates);

            // add the tile info
            m_mapTiles[info.tileId] = info;
        }
    }
}

bool TilesTimeSeries::AddSatteliteIntersectingProducts(const QList<TileTimeSeriesInfo> &individualTileInfos,
                                                     const QStringList &listSecondarySatLoadedProds, Satellite secondarySatId,
                                                     TileTimeSeriesInfo &primarySatInfos) {

    // mapSatellitesTilesInfos contains the sattelites infos but each tile has only  products from its satellite
    // (note that we have also here tiles from the secondary satellite)
    bool bUpdated = false;
    // iterate all tiles in the map
    for (const TileTimeSeriesInfo &info : individualTileInfos) {
        // if we have a secondary product type
        if(info.uniqueSatteliteIds.contains(secondarySatId)) {
            // check if the tile meta appears in the list of loaded secondary products that were intersecting the primary ones
            for(const InfoTileFile &temporalTileFileInfo: info.temporalTilesFileInfos) {
                if(listSecondarySatLoadedProds.contains(temporalTileFileInfo.metaFile) &&
                        !TemporalTileInfosHasFile(primarySatInfos, temporalTileFileInfo.metaFile)) {
                    // add it to the target primary sattelite information list
                    primarySatInfos.temporalTilesFileInfos.append({temporalTileFileInfo.metaFile, temporalTileFileInfo.satId, "", {}, temporalTileFileInfo.srcPrdDetails});
                    if(!primarySatInfos.uniqueSatteliteIds.contains(secondarySatId)) {
                        primarySatInfos.uniqueSatteliteIds.append(secondarySatId);
                    }
                    bUpdated = true;
                }
            }
        }
    }
    return bUpdated;
}

bool TilesTimeSeries::TemporalTileInfosHasFile(const TileTimeSeriesInfo &temporalTileInfo, const QString &filePath)
{
    for(const InfoTileFile &fileInfo: temporalTileInfo.temporalTilesFileInfos) {
        if(fileInfo.metaFile == filePath)
            return true;
    }
    return false;
}
