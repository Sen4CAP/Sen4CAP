#ifndef CompositeHandlerIndicators_HPP
#define CompositeHandlerIndicators_HPP

#include "genericcompositehandlerbase.hpp"

#define INDICATORS_COMPOSITE_CFG_PREFIX "processor.l3_ind_comp."

class CompositeHandlerIndicators : public GenericCompositeHandler
{
public:
    CompositeHandlerIndicators();
    virtual ProductType GetOutputProductType() { return ProductType::L3IndicatorsCompositeProductTypeId; }

};


#endif // CompositeHandlerIndicators_HPP
