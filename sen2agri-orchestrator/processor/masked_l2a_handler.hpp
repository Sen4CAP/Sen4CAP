#ifndef MASKEDL2AHANDLER_HPP
#define MASKEDL2AHANDLER_HPP

#include "processorhandler.hpp"
#include "s4c_utils.hpp"

#define MASKED_L2A_CFG_PREFIX "processor.l2a_msk."

class MaskedL2AHandler : public ProcessorHandler
{
public:
    typedef struct {
        Product l2aPrd;
        Product fmaskPrd;
        int maskValidVal;
        bool HasFMaskPrd() const {return fmaskPrd.productTypeId == ProductType::FMaskProductTypeId;}
    } InputPrdInfo;

public:
    MaskedL2AHandler();
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
    void CreateTasksAndSteps(EventProcessingContext &ctx, const JobSubmittedEvent &evt, const QList<InputPrdInfo> &prdInfos);
    QList<InputPrdInfo> GetInputProductsToProcess(EventProcessingContext &ctx, const JobSubmittedEvent &evt,
                                                  const QJsonObject &parameters, const ProductList &l2aPrdsList);
    bool GetPairedProduct(EventProcessingContext &ctx,  int siteId, ProductType targetPrdType, int dwnHistId, Product &outFMaskPrd);
    bool CreateMaskedL2AProduct(EventProcessingContext &ctx, const TaskFinishedEvent &event);
    void CreateValidityMaskExtractorStep(EventProcessingContext &ctx, const JobSubmittedEvent &evt,
                                         const InputPrdInfo &prdInfo, TaskToSubmit &task, NewStepList &steps, bool compress, bool cog,
                                         bool continueOnMissingInput);

    int GetProductsFromSchedReq(EventProcessingContext &ctx, const JobSubmittedEvent &event,
                                             QJsonObject &parameters, ProductList &outPrdsList);
    void SubmitEndOfLaiTask(EventProcessingContext &ctx, const JobSubmittedEvent &event, const QList<TaskToSubmit> &allTasksList);
    QList<InputPrdInfo> FilterInputProducts(EventProcessingContext &ctx, int siteId, int jobId, const QList<InputPrdInfo> &prdsToProcess);
};


#endif // MASKEDL2AHANDLER_HPP
