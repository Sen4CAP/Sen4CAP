#ifndef TREXHANDLER_HPP
#define TREXHANDLER_HPP

#include "processorhandler.hpp"
#include "s4c_utils.hpp"

#define TREX_CFG_PREFIX "processor.trex."

class TRexHandler : public ProcessorHandler
{
public:
public:
    TRexHandler();
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
    void CreateTasksAndSteps(EventProcessingContext &ctx, const JobSubmittedEvent &evt);
    void CreateTRexUpdaterStep(EventProcessingContext &ctx, const JobSubmittedEvent &evt, TaskToSubmit &task, NewStepList &steps);
};


#endif // TREXHANDLER_HPP
