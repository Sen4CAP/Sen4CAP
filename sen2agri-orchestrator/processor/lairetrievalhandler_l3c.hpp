#ifndef LAIRETRIEVALHANDLERL3C_HPP
#define LAIRETRIEVALHANDLERL3C_HPP

#include "lairetrhandler_multidt_base.hpp"

class LaiRetrievalHandlerL3C : public LaiRetrievalHandlerMultiDateBase
{
private:
    QStringList GetSpecificReprocessingArgs(const std::map<QString, QString> &configParameters) override;
    ProductType GetOutputProductType() override;
    QString GetOutputProductShortName() override;
    void WriteExecutionSpecificParamsValues(const std::map<QString, QString> &configParameters, std::ofstream &stream) override;
    QString GetPrdFormatterRasterFlagName() override;
    QString GetPrdFormatterMskFlagName() override;
    QList<QMap<QString, TileTimeSeriesInfo>> ExtractL3BMapTiles(EventProcessingContext &ctx, const JobSubmittedEvent &event,
                                                       const QStringList &l3bProducts,
                                                       const QMap<Satellite, TileList> &siteTiles) override;
    ProductList GetScheduledJobProductList(SchedulingContext &ctx, int siteId, const QDateTime &seasonStartDate,
                                           const QDateTime &seasonEndDate, const QDateTime &qScheduledDate,
                                           const ConfigurationParameterValueMap &requestOverrideCfgValues) override;
    bool AcceptSchedJobProduct(const QString &l2aPrdHdrPath, Satellite satId) override;

    QStringList GetL3BProductsSinceStartOfSeason(EventProcessingContext &ctx, int siteId, const QStringList &listExistingPrds);

    QMap<QString, TileTimeSeriesInfo> GetL3BMapTiles(EventProcessingContext &ctx, const QString &newestL3BProd,
                                                        const QStringList &l3bProducts,
                                                        const QMap<Satellite, TileList> &siteTiles,
                                                        int limitL3BPrdsPerTile);
    QDateTime GetL3BLastAcqDate(const QStringList &listL3bPrds);
    QDate GetSiteFirstSeasonStartDate(EventProcessingContext &ctx,int siteId);
    bool HasSufficientProducts(const TileTimeSeriesInfo &tileInfo,
                               const Satellite &tileSatId, int limitL3BPrdsPerTile);

};

#endif // LAIRETRIEVALHANDLERNEW_HPP

