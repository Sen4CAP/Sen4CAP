#ifndef ZARRHANDLER_HPP
#define ZARRHANDLER_HPP

#include "processorhandler.hpp"

#define ZARR_CFG_PREFIX "processor.zarr."

class ZarrHandler : public ProcessorHandler
{
public:
public:
    ZarrHandler();
private:
    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &evt) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;
    void HandleProductAvailableImpl(EventProcessingContext &ctx,
                                    const ProductAvailableEvent &event) override;

    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;

private:
    void CreateTasksAndSteps(EventProcessingContext &ctx, const JobSubmittedEvent &evt, const QList<Product> &prdInfos);
    void CreateZarrStep(const Product &prdInfo, TaskToSubmit &task, NewStepList &steps);
    int GetProductsFromSchedReq(EventProcessingContext &ctx, const JobSubmittedEvent &event,
                                QJsonObject &parameters, ProductList &outPrdsList);
};


#endif // ZARRHANDLER_HPP
