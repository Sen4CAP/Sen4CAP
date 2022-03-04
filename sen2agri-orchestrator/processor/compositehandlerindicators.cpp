#include "compositehandlerindicators.hpp"

CompositeHandlerIndicators::CompositeHandlerIndicators() :
    GenericCompositeHandler(INDICATORS_COMPOSITE_CFG_PREFIX, {"NDVI", "LAI", "FAPAR", "FCOVER"})
{
}


