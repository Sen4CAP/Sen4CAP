#pragma once

#include "processorhandler.hpp"
#include "optional.hpp"

#define S2A_CM_PREFIX "processor.l4a."

typedef struct {
    int jobId;
    int siteId;
    int resolution;
    QList<ProductDetails> productDetails;
    QDateTime startDate;
    QDateTime endDate;

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

    QString referenceDataSource;

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
    QList<std::reference_wrapper<TaskToSubmit>> CreateTasks(const CropMaskJobConfig &jobCfg, QList<TaskToSubmit> &outAllTasksList);
    NewStepList CreateSteps(EventProcessingContext &ctx, const JobSubmittedEvent &event, QList<TaskToSubmit> &allTasksList,
                            const CropMaskJobConfig &cfg);
    QStringList GetEarthSignatureTaskArgs(const CropMaskJobConfig &cfg, TaskToSubmit &earthSignatureTask);
    QStringList GetCropMaskTaskArgs(EventProcessingContext &ctx, const JobSubmittedEvent &event, const CropMaskJobConfig &cfg, TaskToSubmit &cropMaskTask);
};
