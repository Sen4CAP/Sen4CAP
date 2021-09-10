#ifndef PROCESSORHANDLERHELPER_H
#define PROCESSORHANDLERHELPER_H

#include "model.hpp"
#include "eventprocessingcontext.hpp"


#include "processor/products/tilestimeseries.hpp"
using namespace orchestrator::tiletimeseries;

typedef std::map<QString, QString> TQStrQStrMap;
typedef std::pair<QString, QString> TQStrQStrPair;

// 23*3600+59*60+59
#define SECONDS_IN_DAY 86400
class ProcessorHandlerHelper
{
public:
    ProcessorHandlerHelper();

    static QStringList GetTextFileLines(const QString &filePath);
    static QString GetFileNameFromPath(const QString &filePath);

    static bool GetCropReferenceFile(const QString &refDir, QString &shapeFile, QString &referenceRasterFile);
    static bool GetStrataFile(const QString &refDir, QString &strataShapeFile);
    static bool GetCropReferenceFile(const QString &refDir, QString &shapeFile, QString &referenceRasterFile, QString &strataShapeFile);

    static bool CompareProductDates(const ProductDetails &prd1, const ProductDetails &prd2);
    static QList<ProductDetails> GetProductDetails(const ProductList &prds, EventProcessingContext &ctx);
    static TilesTimeSeries GroupTiles(EventProcessingContext &ctx, int siteId, const QList<ProductDetails> &productDetails, ProductType productType);
    static QMap<QDate, QList<ProductDetails>> GroupByDate(const QList<ProductDetails> &prdDetails);
    static bool GetIntevalFromProducts(const QList<ProductDetails> &prdDetails, QDateTime &minTime, QDateTime &maxTime);
    static bool GetIntevalFromProducts(const ProductList &products, QDateTime &minTime, QDateTime &maxTime);
    static bool GetIntevalFromProducts(const QStringList &productsList, QDateTime &minTime, QDateTime &maxTime);

    static void UpdateMinMaxTimes(const QDateTime &newTime, QDateTime &minTime, QDateTime &maxTime);

    static QString GetMapValue(const std::map<QString, QString> &configParameters, const QString &key, const QString &defVal = "");
    static bool GetBoolConfigValue(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                            const QString &key, const QString &cfgPrefix);
    static int GetIntConfigValue(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                          const QString &key, const QString &cfgPrefix);
    static QString GetStringConfigValue(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                            const QString &key, const QString &cfgPrefix);

    static bool GetParameterValueAsInt(const QJsonObject &parameters, const QString &key, int &outVal);
    static bool GetParameterValueAsString(const QJsonObject &parameters, const QString &key, QString &outVal);

    static QDateTime GetDateTimeFromString(const QString &strTime);
    static QDateTime GetLocalDateTime(const QString &strTime);
    static Satellite GetSatIdForTile(const QMap<Satellite, TileList> &mapSatTiles, const QString &tileId);
    static QStringList EnsureMonoDateProductUniqueProc(const QString &filePath, EventProcessingContext &ctx,
                                                       const QStringList &processingItems, int procId, int siteId, int curJobId);
    static void CleanupCurrentProductIdsForJob(const QString &filePath, int jbId, const QStringList &processingItems);

};

#endif // PROCESSORHANDLERHELPER_H
