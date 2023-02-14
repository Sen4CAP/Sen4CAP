#pragma once

#include "processorhandler.hpp"
#include "optional.hpp"

#define S2A_CT_PREFIX "processor.l4b."
class CropTypeHandler : public ProcessorHandler
{
    typedef struct {
        int jobId;
        int siteId;
        int resolution;
        QList<ProductDetails> productDetails;
        QDateTime startDate;
        QDateTime endDate;

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

        QString referenceDataSource;
    } CropTypeJobConfig;

private:
    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &event) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;

    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;
    void GetJobConfig(EventProcessingContext &ctx,const JobSubmittedEvent &event,CropTypeJobConfig &cfg);
    QList<std::reference_wrapper<TaskToSubmit>> CreateTasks(const CropTypeJobConfig &jobCfg, QList<TaskToSubmit> &outAllTasksList);
    NewStepList CreateSteps(EventProcessingContext &ctx, const JobSubmittedEvent &event, QList<TaskToSubmit> &allTasksList,
                            const CropTypeJobConfig &cfg);
    QStringList GetEarthSignatureTaskArgs(const CropTypeJobConfig &cfg, TaskToSubmit &earthSignatureTask);
    QStringList GetCropTypeTaskArgs(EventProcessingContext &ctx, const JobSubmittedEvent &event, const CropTypeJobConfig &cfg, TaskToSubmit &cropTypeTask);
};
