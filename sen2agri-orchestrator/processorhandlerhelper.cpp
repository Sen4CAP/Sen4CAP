#include "processorhandlerhelper.h"
#include "qdiriterator.h"
#include <QRegularExpression>

#include "qdatetime.h"
#include "qjsonobject.h"
#include "logger.hpp"
#include "json_utils.hpp"
#include <unordered_map>

#include "productdetailsbuilder.h"

#include "processor/products/producthelper.h"
#include "processor/products/producthelperfactory.h"
using namespace orchestrator::products;

// For unordered map and QString as key
namespace std {
  template<> struct hash<QString> {
    std::size_t operator()(const QString& s) const noexcept {
      return (size_t) qHash(s);
    }
  };
}

ProcessorHandlerHelper::ProcessorHandlerHelper() {}

QStringList ProcessorHandlerHelper::GetTextFileLines(const QString &filePath) {
    QFile inputFile(filePath);
    QStringList lines;
    if (inputFile.open(QIODevice::ReadOnly))
    {
       QTextStream in(&inputFile);
       while (!in.atEnd())
       {
          lines.append(in.readLine());
       }
       inputFile.close();
    }
    return lines;
}

QString ProcessorHandlerHelper::GetFileNameFromPath(const QString &filePath) {
    QFileInfo fileInfo(filePath);
    return fileInfo.fileName();
}

bool ProcessorHandlerHelper::GetStrataFile(const QString &refDir, QString &strataShapeFile) {
    bool bRet = false;
    QDirIterator itStrataDir(refDir, QStringList() << "strata" << "STRATA", QDir::Dirs);
    while (itStrataDir.hasNext()) {
        QDirIterator itStrataFile(itStrataDir.next(), QStringList() << "*.shp" << "*.SHP", QDir::Files);
        // get the last strata shape file found
        while (itStrataFile.hasNext()) {
            strataShapeFile = itStrataFile.next();
            bRet = true;
        }
    }

    return bRet;
}

bool ProcessorHandlerHelper::GetCropReferenceFile(const QString &refDir, QString &shapeFile, QString &referenceRasterFile, QString &strataShapeFile) {
    bool bRet = GetCropReferenceFile(refDir, shapeFile, referenceRasterFile);
    GetStrataFile(refDir, strataShapeFile);

    return bRet;
}

bool ProcessorHandlerHelper::GetCropReferenceFile(const QString &refDir, QString &shapeFile, QString &referenceRasterFile) {
    bool bRet = false;
    if(refDir.isEmpty()) {
        return bRet;
    }

    QDirIterator it(refDir, QStringList() << "*.shp" << "*.SHP", QDir::Files);
    // get the last shape file found
    while (it.hasNext()) {
        shapeFile = it.next();
        bRet = true;
    }
    // even if the shape file was found, search also for the reference raster for no-insitu case
    QDirIterator it2(refDir, QStringList() << "*.tif" << "*.TIF", QDir::Files);
    // get the last reference raster file found
    while (it2.hasNext()) {
        referenceRasterFile = it2.next();
        bRet = true;
    }

    return bRet;
}

QList<ProductDetails> ProcessorHandlerHelper::GetProductDetails(const ProductList &prds, EventProcessingContext &ctx)
{
    return ProductDetailsBuilder::CreateDetails(prds, ctx);
}

QMap<QDate, QList<ProductDetails>> ProcessorHandlerHelper::GroupByDate(const QList<ProductDetails> &prdsDetails)
{
    QMap<QDate, QList<ProductDetails>> mapDateTiles;
    QMap<QDate, QList<Satellite>> mapDateTilesSats;
    // group tiles by their date
    for(const auto &prdDetails : prdsDetails) {
        // get the date of the current tile (normally, we could take only for the first tile
        // and then add all other tiles of the product directly but is simpler this way)
        const QDateTime &tileDateTime = prdDetails.GetProduct().created;
        const QDate &tileDate = tileDateTime.date();
        // add it to the list of the tiles from the current date
        if(mapDateTiles.contains(tileDate)) {
            QList<ProductDetails> &dateTiles = mapDateTiles[tileDate];
            if(!dateTiles.contains(prdDetails)) {
                dateTiles.append(prdDetails);

                // add also the satellite
                QList<Satellite> &listSat = mapDateTilesSats[tileDate];
                listSat.append((Satellite)prdDetails.GetProduct().satId);
            }
        } else {
            QList<ProductDetails> dateTiles;
            dateTiles.append(prdDetails);
            mapDateTiles[tileDate] = dateTiles;

            // add also the satellite
            QList<Satellite> listSat;
            listSat.append((Satellite)prdDetails.GetProduct().satId);
            mapDateTilesSats[tileDate] = listSat;
        }
    }
    // Now, we should iterate the tiles list and remove the secondary satellites from that date
    // as if we have the primary satellite, there is no need to consider also the secondary satellite for that date
    QMap<QDate, QList<ProductDetails>> retMapDateTiles;
    for(const auto &date : mapDateTiles.keys()) {
        QList<ProductDetails> filteredDateTilesList;
        const QList<ProductDetails> &dateTilesList = mapDateTiles[date];
        const QList<Satellite> &dateTilesSatsList = mapDateTilesSats[date];
        Satellite primarySatId = ProductHelper::GetPrimarySatelliteId(dateTilesSatsList);
        // normally we must have the same number as we above added in the same time in the two lists
        for(int i = 0; i<dateTilesList.size(); i++) {
            if(primarySatId == dateTilesSatsList[i]) {
                filteredDateTilesList.append(dateTilesList[i]);
            }
        }
        // normally, we must have here at least one tile so there is no need to make any check on size of list
        retMapDateTiles[date] = filteredDateTilesList;
    }
    return retMapDateTiles;

}

bool ProcessorHandlerHelper::GetIntevalFromProducts(const QList<ProductDetails> &products, QDateTime &minTime, QDateTime &maxTime)
{
    for(const ProductDetails &prod: products) {
        const QDateTime &curDate = prod.GetProduct().created;
        if(curDate.isValid()) {
            if(!minTime.isValid() || minTime > curDate)
                minTime = curDate;
            if(!maxTime.isValid() || maxTime < curDate)
                maxTime = curDate.addSecs(SECONDS_IN_DAY-1);
        }
    }

    return (minTime.isValid() && maxTime.isValid());
}

bool ProcessorHandlerHelper::GetIntevalFromProducts(const ProductList &products, QDateTime &minTime, QDateTime &maxTime) {
    for(const Product &prod: products) {
        const QDateTime &curDate = prod.created;
        if(curDate.isValid()) {
            if(!minTime.isValid() || minTime > curDate)
                minTime = curDate;
            if(!maxTime.isValid() || maxTime < curDate)
                maxTime = curDate.addSecs(SECONDS_IN_DAY-1);
        }
    }

    return (minTime.isValid() && maxTime.isValid());
}

bool ProcessorHandlerHelper::GetIntevalFromProducts(const QStringList &productsList, QDateTime &minTime, QDateTime &maxTime)
{
    for(const QString &prod: productsList) {
        std::unique_ptr<ProductHelper> helper = ProductHelperFactory::GetProductHelper(prod);
        const QDateTime &curDate = helper->GetAcqDate();
        if(curDate.isValid()) {
            if(!minTime.isValid() || minTime > curDate)
                minTime = curDate;
            if(!maxTime.isValid() || maxTime < curDate)
                maxTime = curDate.addSecs(SECONDS_IN_DAY-1);
        }
    }

    return (minTime.isValid() && maxTime.isValid());
}


void ProcessorHandlerHelper::UpdateMinMaxTimes(const QDateTime &newTime, QDateTime &minTime, QDateTime &maxTime)
{
    if (newTime.isValid()) {
        if (!minTime.isValid() && !maxTime.isValid()) {
            minTime = newTime;
            maxTime = newTime;
        } else if (newTime < minTime) {
            minTime = newTime;
        } else if (newTime > maxTime) {
            maxTime = newTime;
        }
    }
}

QString ProcessorHandlerHelper::GetMapValue(const std::map<QString, QString> &configParameters, const QString &key, const QString &defVal) {
    std::map<QString, QString>::const_iterator it = configParameters.find(key);
    if(it != configParameters.end()) {
        return it->second;
    }
    return defVal;
}

bool ProcessorHandlerHelper::GetBoolConfigValue(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                                const QString &key, const QString &cfgPrefix, bool defVal) {
    const QString &strVal = GetStringConfigValue(parameters, configParameters, key, cfgPrefix);
    // if key not set or empty string
    if (strVal.trimmed().size() == 0) {
        return defVal;
    }
    if (QString::compare(strVal, "true", Qt::CaseInsensitive) == 0) {
        return true;
    }
    if (QString::compare(strVal, "false", Qt::CaseInsensitive) == 0) {
        return false;
    }
    return (GetIntConfigValue(parameters, configParameters, key, cfgPrefix, (int)defVal) != 0);
}

int ProcessorHandlerHelper::GetIntConfigValue(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                                const QString &key, const QString &cfgPrefix, int defVal) {
    bool ok;
    const QString &strVal = GetStringConfigValue(parameters, configParameters, key, cfgPrefix);
    int val = strVal.toInt(&ok, 10);
    return (ok ? val : defVal);
}

QString ProcessorHandlerHelper::GetStringConfigValue(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                                const QString &key, const QString &cfgPrefix) {
    QString fullKey(cfgPrefix);
    fullKey += key;

    QString retVal;
    if (parameters.contains(key)) {
        retVal = parameters[key].toString();
    } else if (parameters.contains(fullKey)) {
        retVal = parameters[fullKey].toString();
    } else {
        retVal = GetMapValue(configParameters, fullKey);
    }
    return retVal;
}

bool ProcessorHandlerHelper::GetParameterValueAsInt(const QJsonObject &parameters, const QString &key,
                                              int &outVal) {
    return getJsonValueAsInt(parameters, key, outVal);
}

bool ProcessorHandlerHelper::GetParameterValueAsString(const QJsonObject &parameters, const QString &key,
                                              QString &outVal) {
    return getJsonValueAsString(parameters, key, outVal);
}

QDateTime ProcessorHandlerHelper::GetDateTimeFromString(const QString &strTime) {
    QDateTime date = QDateTime::fromString(strTime, "yyyyMMdd");
    if (!date.isValid()) {
        date = QDateTime::fromString(strTime, "yyyy-MM-dd");
    }
    return date;
}

QDateTime ProcessorHandlerHelper::GetLocalDateTime(const QString &strTime) {
    QDateTime dateTime = GetDateTimeFromString(strTime);
    dateTime.setTimeSpec(Qt::UTC); // mark the timestamp as UTC (but don't convert it)
    dateTime = dateTime.toLocalTime();
    if (dateTime.isDaylightTime()) {
        dateTime = dateTime.addSecs(-3600);
    }
    return dateTime;
}

Satellite ProcessorHandlerHelper::GetSatIdForTile(const QMap<Satellite, TileList> &mapSatTiles, const QString &tileId)
{
    for(const auto &satId : mapSatTiles.keys())
    {
        const TileList &listTiles = mapSatTiles.value(satId);
        for(const Tile &tile: listTiles) {
            if(tile.tileId == tileId) {
                return satId;
            }
        }
    }
    return Satellite::Invalid;
}

QStringList ProcessorHandlerHelper::EnsureMonoDateProductUniqueProc(const QString &filePath, EventProcessingContext &ctx, const QStringList &processingItems,
            int procId, int siteId, int curJobId)
{
    QStringList filteredProcessingItems;

    std::unordered_map<QString, int> mapPresence;
    std::for_each(processingItems.begin(), processingItems.end(), [&mapPresence](const QString &procItem) {
        mapPresence[procItem] = 1;
    });

    QDir().mkpath(QFileInfo(filePath).absolutePath());
    QFile file( filePath );

    // Get the active jobs of this site
    const JobIdsList &activeJobIds = ctx.GetActiveJobIds(procId, siteId);

    // First read all the entries in the file to see what are the products that are currently processing
    QMap<QString, int> curProcPrds;
    if (file.open(QIODevice::ReadOnly))
    {
       QTextStream in(&file);
       while (!in.atEnd())
       {
           const QString &line = in.readLine();
           const QStringList &pieces = line.split(';');
           // first is the processing item identifier (path but can be also a product id) and
           // then the job id that is processing it
           if (pieces.size() == 2) {
               const QString &procItem = pieces[0];
               int jobId = pieces[1].toInt();
               if (mapPresence.find(procItem) != mapPresence.end()) {
                   continue;
               }
               // if the job processing the product is still active, keep the product
               if (activeJobIds.contains(jobId)) {
                   curProcPrds[procItem] = jobId;
               }
           }
       }
       file.close();
    }
    // add the products that will be processed next
    for (int i = 0; i<processingItems.size(); i++) {
        const QString &procItem =  processingItems[i];
        if (!curProcPrds.contains(procItem)) {
            curProcPrds[procItem] = curJobId;
            filteredProcessingItems.append(procItem);
        }
        // else, if the product was already in this list, then it means it was already scheduled for processing
        // by another schedule operation
    }

    if ( file.open(QIODevice::ReadWrite | QFile::Truncate) )
    {
        if (curProcPrds.size() > 0) {
            QTextStream stream( &file );
            for(auto prdInfo : curProcPrds.keys()) {
                stream << prdInfo << ";" << curProcPrds.value(prdInfo) << endl;
            }
        }
    }
    return filteredProcessingItems;
}

void ProcessorHandlerHelper::CleanupCurrentProductIdsForJob(const QString &filePath, int jbId, const QStringList &processingItems) {
    QFile file( filePath );
    // First read all the entries in the file to see what are the products that are currently processing
    QMap<QString, int> curProcPrds;
    if (file.open(QIODevice::ReadOnly)) {
        QTextStream in(&file);
        while (!in.atEnd()) {
            const QString &line = in.readLine();
            const QStringList &pieces = line.split(';');
            if (pieces.size() == 2) {
                const QString &procItem = pieces[0];
                int jobId = pieces[1].toInt();
                if (jobId == jbId && processingItems.contains(procItem)) {
                    continue;
                }
                curProcPrds[procItem] = jobId;
            }
        }
        file.close();
    }

    if ( file.open(QIODevice::ReadWrite | QFile::Truncate) )
    {
        QTextStream stream( &file );
        for(auto prdInfo : curProcPrds.keys()) {
            stream << prdInfo << ";" << curProcPrds.value(prdInfo) << endl;
        }
    }
}
