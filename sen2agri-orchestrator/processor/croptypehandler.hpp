#pragma once

#include "processorhandler.hpp"
#include "optional.hpp"

class CropTypeHandler : public ProcessorHandler
{
    typedef struct {
        int jobId;
        int siteId;
        int resolution;

        QString referencePolygons;
        QString cropMask;

        QString appsMem;
        QString lutPath;

        QString randomSeed;
        QString temporalResamplingMode;
        QString sampleRatio;

        QString classifier;
        QString fieldName;
        QString classifierRfNbTrees;
        QString classifierRfMinSamples;
        QString classifierRfMaxDepth;
        QString classifierSvmKernel;
        QString classifierSvmOptimize;

        int tileThreadsHint;
        std::experimental::optional<int> maxParallelism;

        QString strataShp;
    } CropTypeJobConfig;

private:
    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &event) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;

    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;
    void GetJobConfig(EventProcessingContext &ctx,const JobSubmittedEvent &event,CropTypeJobConfig &cfg);
    QList<std::reference_wrapper<TaskToSubmit>> CreateTasks(QList<TaskToSubmit> &outAllTasksList);
    NewStepList CreateSteps(EventProcessingContext &ctx, const JobSubmittedEvent &event, QList<TaskToSubmit> &allTasksList,
                            const CropTypeJobConfig &cfg, const QList<ProductDetails> &productDetails);
    QStringList GetCropTypeTaskArgs(EventProcessingContext &ctx, const JobSubmittedEvent &event, const CropTypeJobConfig &cfg,
                                        const QList<ProductDetails> &productDetails, TaskToSubmit &cropTypeTask);
};
