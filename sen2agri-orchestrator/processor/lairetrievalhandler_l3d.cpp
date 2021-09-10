#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "lairetrievalhandler_l3d.hpp"
#include "processorhandlerhelper.h"
#include "json_conversions.hpp"
#include "logger.hpp"

#include "products/generichighlevelproducthelper.h"
using namespace orchestrator::products;

QStringList LaiRetrievalHandlerL3D::GetSpecificReprocessingArgs(const std::map<QString, QString> &)
{
    QStringList specificArgs = {"-algo", "fit",
                                "-genall", "1"};
    return specificArgs;
}

ProductType LaiRetrievalHandlerL3D::GetOutputProductType()
{
    return ProductType::L3DProductTypeId;
}

QString LaiRetrievalHandlerL3D::GetOutputProductShortName()
{
    return "L3D";
}

void LaiRetrievalHandlerL3D::WriteExecutionSpecificParamsValues(const std::map<QString, QString> &,
                                                                std::ofstream &)
{
    // nothing to add here
}

QString LaiRetrievalHandlerL3D::GetPrdFormatterRasterFlagName()
{
    return "-processor.vegetation.filelaifit";
}

QString LaiRetrievalHandlerL3D::GetPrdFormatterMskFlagName()
{
    return "-processor.vegetation.filelaifitflgs";
}

QList<QMap<QString, TileTimeSeriesInfo>> LaiRetrievalHandlerL3D::ExtractL3BMapTiles(EventProcessingContext &ctx,
                                                   const JobSubmittedEvent &,
                                                   const QStringList &l3bProducts,
                                                   const QMap<Satellite, TileList> &siteTiles)
{
    QList<QMap<QString, TileTimeSeriesInfo>> retList;
    const QMap<QString, TileTimeSeriesInfo> &l3bMapTiles = GetL3BMapTiles(ctx, l3bProducts, siteTiles);
    retList.append(l3bMapTiles);
    return retList;
}

ProductList LaiRetrievalHandlerL3D::GetScheduledJobProductList(SchedulingContext &ctx, int siteId, const QDateTime &seasonStartDate,
                                                               const QDateTime &seasonEndDate, const QDateTime &,
                                                               const ConfigurationParameterValueMap &)
{
    return ctx.GetProducts(siteId, (int)ProductType::L3BProductTypeId, seasonStartDate, seasonEndDate);
}

bool LaiRetrievalHandlerL3D::AcceptSchedJobProduct(const QString &, Satellite )
{
    return true;
}


//TODO: This function should receive the Product and QList<Product> instead of just product path as these can be got from DB
//      The Product contains already the tiles, the full path and the acquisition date so can be avoided parsing files
QMap<QString, TileTimeSeriesInfo> LaiRetrievalHandlerL3D::GetL3BMapTiles(EventProcessingContext &ctx,
                                                                            const QStringList &l3bProducts,
                                                                            const QMap<Satellite, TileList> &siteTiles)
{
    QMap<QString, TileTimeSeriesInfo> retL3bMapTiles;
    GenericHighLevelProductHelper prdHelper, prdHelper2;
    for(const QString &l3bProd: l3bProducts) {
        prdHelper.SetProduct(l3bProd);
        const QStringList &l3BTiles = prdHelper.GetTileIdsFromProduct();
        for(const auto &tileId : l3BTiles) {

            // TODO: see if a limitation is needed based on the satellite ID (only S2?)

            Satellite tileSatId = ProcessorHandlerHelper::GetSatIdForTile(siteTiles, tileId);
            if(!retL3bMapTiles.contains(tileId)) {
                TileTimeSeriesInfo newTileInfos;
                newTileInfos.tileId = tileId;
                // add the tile infos to the map
                retL3bMapTiles[tileId] = newTileInfos;
            }
            TileTimeSeriesInfo &tileInfo = retL3bMapTiles[tileId];
            for(const QString &curL3bPrd: l3bProducts) {
                prdHelper2.SetProduct(curL3bPrd);
                const QDateTime &minDate = prdHelper2.GetAcqDate();
                // Fill the tile information for the current tile from the current product
                AddTileFileInfo(ctx, tileInfo, curL3bPrd, tileId, siteTiles, tileSatId, minDate);
            }
            if(tileInfo.temporalTilesFileInfos.size() > 0) {
                 // update the primary satellite information
                 tileInfo.primarySatelliteId = ProductHelper::GetPrimarySatelliteId(tileInfo.uniqueSatteliteIds);
            }
        }
    }
    // if primary and secondary satellites, then keep only the tiles from primary satellite
    // as we don't want to have in the resulted product combined primary and secondary satellites tiles
    //return retL3bMapTiles;
    return FilterSecondaryProductTiles(retL3bMapTiles, siteTiles);
}
