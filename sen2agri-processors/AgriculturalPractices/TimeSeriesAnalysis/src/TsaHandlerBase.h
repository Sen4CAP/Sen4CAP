#ifndef TsaHandlerBase_H
#define TsaHandlerBase_H

#include "TimeSeriesAnalysisTypes.h"

class TsaHandlerBase
{
public:
    TsaHandlerBase();

    virtual bool IsShorteningHarvestInterval(const FieldInfoType &) { return false; }

};

#endif
