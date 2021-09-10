#pragma once

#include "processorhandler.hpp"
#include "optional.hpp"

typedef struct {
    int jobId;
    int siteId;
    int resolution;

    QString referencePolygons;
    QString referenceRaster;
    QString strataShp;

    QString lutPath;
    QString appsMem;

    QString randomSeed;
    QString sampleRatio;
    QString temporalResamplingMode;
    QString window;
    QString nbcomp;
    QString spatialr;
    QString ranger;
    QString minsize;
    QString minarea;
    QString classifier;
    QString fieldName;
    QString classifierRfNbTrees;
    QString classifierRfMinSamples;
    QString classifierRfMaxDepth;
    QString classifierSvmKernel;
    QString classifierSvmOptimize;

    QString nbtrsample;
    QString lmbd;
    QString erode_radius;
    QString alpha;

    bool skipSegmentation;

    int tileThreadsHint;
    std::experimental::optional<int> maxParallelism;
} CropMaskJobConfig;

class CropMaskHandler : public ProcessorHandler
{
private:
    void SetProcessorDescription(const ProcessorDescription &procDescr) override;
    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &event) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;

    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;

    void GetJobConfig(EventProcessingContext &ctx,const JobSubmittedEvent &event,CropMaskJobConfig &cfg);
    QList<std::reference_wrapper<TaskToSubmit>> CreateTasks(QList<TaskToSubmit> &outAllTasksList);
    NewStepList CreateSteps(EventProcessingContext &ctx, const JobSubmittedEvent &event, QList<TaskToSubmit> &allTasksList,
                            const CropMaskJobConfig &cfg, const QList<ProductDetails> &prdDetails);
    QStringList GetCropTypeTaskArgs(EventProcessingContext &ctx, const JobSubmittedEvent &event, const CropMaskJobConfig &cfg,
                                        const QList<ProductDetails> &listProducts, TaskToSubmit &cropMaskTask);
};
