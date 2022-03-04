#include "compositehandlers1.hpp"

CompositeHandlerS1::CompositeHandlerS1() :
    GenericCompositeHandler(S1_COMPOSITE_CFG_PREFIX, {"AMP"})
{
}

QStringList CompositeHandlerS1::GetAlwaysEnabledMarkerNames()
{
    return {"AMP"};
}


QList<FilterAndGroupingOptions> CompositeHandlerS1::GetFilterAndGroupingOptions(const GenericCompositeJobPayload &jobCfg)
{
    const QString &polarisations = ProcessorHandlerHelper::GetStringConfigValue(jobCfg.parameters, jobCfg.configParameters,
                                                               "polarisations", S1_COMPOSITE_CFG_PREFIX);

    if (polarisations == "VV" || polarisations == "VH") {
        return {{polarisations, "_" + polarisations + "_"}};
    }
    // Both VV and VH filtering and grouping
    return QList<FilterAndGroupingOptions>({{"VH", "_VH_"},
                                            {"VV", "_VV_"}});
}

QMap<QString, QList<ProductMarkerInfo>> CompositeHandlerS1::Filter(const FilterAndGroupingOptions &filter,
                                                                   const QMap<QString, QList<ProductMarkerInfo>> &tileFilesInfo)
{
    QMap<QString, QList<ProductMarkerInfo>> ret;
    for(auto tile: tileFilesInfo.keys()) {
        const auto &infos = tileFilesInfo.value(tile);
        if (infos.size() > 0) {
            QList<ProductMarkerInfo> resInfos;
            std::copy_if(infos.cbegin(),
                         infos.cend(),
                         std::back_inserter(resInfos),
                         [&resInfos, &filter](const ProductMarkerInfo &info)
                         {
                             return QFileInfo(info.prdFileInfo.inFilePath).fileName().contains(filter.filter);
                         });
            if (resInfos.size() > 0) {
                ret[tile] = resInfos;
            }
        }
    }
    return ret;
}
