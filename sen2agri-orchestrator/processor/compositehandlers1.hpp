#ifndef CompositeHandlerS1_HPP
#define CompositeHandlerS1_HPP

#include "genericcompositehandlerbase.hpp"

#define S1_COMPOSITE_CFG_PREFIX "processor.l3_s1_comp."

class CompositeHandlerS1 : public GenericCompositeHandler
{
public:
    CompositeHandlerS1();

protected:
    virtual QStringList GetAlwaysEnabledMarkerNames();
    virtual ProductType GetOutputProductType() { return ProductType::S1CompositeProductTypeId; }

    virtual QList<FilterAndGroupingOptions> GetFilterAndGroupingOptions(const GenericCompositeJobPayload &jobCfg);
    virtual QMap<QString, QList<ProductMarkerInfo>> Filter(const FilterAndGroupingOptions &filter,
                                                           const QMap<QString, QList<ProductMarkerInfo>> &tileFilesInfo);
};


#endif // CompositeHandlerS1_HPP
