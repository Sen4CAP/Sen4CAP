#pragma once

#include "model.hpp"
#include "eventprocessingcontext.hpp"
#include "schedulingcontext.h"
#include "processorhandlerhelper.h"

#include <memory>
#include <mutex>

#define PRODUCTS_LOCATION_CFG_KEY "archiver.archive_path"
#define PRODUCT_FORMATTER_OUT_PROPS_FILE "product_properties.txt"
#define CLOUD_OPTIMIZED_GEOTIFF_ENABLED "processor."
#define ORCHESTRATOR_CFG_KEYS_ROOT "general.orchestrator."

#define USE_DOCKER_STEP_CFG_KEY "use_docker"
#define DOCKER_IMAGE_STEP_CFG_KEY "docker_image"
#define DOCKER_ADD_MOUNTS_STEP_CFG_KEY "docker_additional_mounts"

typedef ProcessorHandlerHelper::TileTemporalFilesInfo TileTemporalFilesInfo;
typedef std::map<QString, QString> TQStrQStrMap;
typedef std::pair<QString, QString> TQStrQStrPair;

class ProcessorHandler
{
public:
    virtual ~ProcessorHandler();
    virtual void SetProcessorDescription(const ProcessorDescription &procDescr) {processorDescr = procDescr;}

    void HandleProductAvailable(EventProcessingContext &ctx, const ProductAvailableEvent &event);
    void HandleJobSubmitted(EventProcessingContext &ctx, const JobSubmittedEvent &event);
    void HandleTaskFinished(EventProcessingContext &ctx, const TaskFinishedEvent &event);

    ProcessorJobDefinitionParams GetProcessingDefinition(SchedulingContext &ctx, int siteId, int scheduledDate,
                                          const ConfigurationParameterValueMap &requestOverrideCfgValues);

protected:
    NewStep CreateTaskStep(TaskToSubmit &task, const QString &stepName, const QStringList &stepArgs);

    QString GetFinalProductFolder(EventProcessingContext &ctx, int jobId, int siteId);
    QString GetFinalProductFolder(EventProcessingContext &ctx, int jobId, int siteId, const QString &productName);
    QString GetFinalProductFolder(const std::map<QString, QString> &cfgKeys, const QString &siteName, const QString &processorName);
    bool NeedRemoveJobFolder(EventProcessingContext &ctx, int jobId, const QString &procName);
    bool RemoveJobFolder(EventProcessingContext &ctx, int jobId, const QString &procName);
    QString GetProductFormatterOutputProductPath(EventProcessingContext &ctx, const TaskFinishedEvent &event);
    QString GetProductFormatterProductName(EventProcessingContext &ctx, const TaskFinishedEvent &event);
    QString GetProductFormatterQuicklook(EventProcessingContext &ctx, const TaskFinishedEvent &event);
    QString GetProductFormatterFootprint(EventProcessingContext &ctx, const TaskFinishedEvent &event);
    bool GetSeasonStartEndDates(const SeasonList &seasons,
                                   QDateTime &startTime, QDateTime &endTime,
                                   const QDateTime &executionDate,
                                   const ConfigurationParameterValueMap &requestOverrideCfgValues);

    bool GetSeasonStartEndDates(SchedulingContext &ctx, int siteId,
                                   QDateTime &startTime, QDateTime &endTime,
                                   const QDateTime &executionDate,
                                   const ConfigurationParameterValueMap &requestOverrideCfgValues);
    bool GetBestSeasonToMatchDate(SchedulingContext &ctx, int siteId,
                                  QDateTime &startTime, QDateTime &endTime,
                                  const QDateTime &executionDate, const ConfigurationParameterValueMap &requestOverrideCfgValues);
    QStringList GetL2AInputProductNames(const JobSubmittedEvent &event);
    QStringList GetL2AInputProducts(EventProcessingContext &ctx, const JobSubmittedEvent &event);
    QStringList GetL2AInputProductsTiles(EventProcessingContext &ctx, const JobSubmittedEvent &event,
                                    QMap<QString, QStringList> &mapProductToTilesMetaFiles);
    QStringList GetL2AInputProductsTiles(EventProcessingContext &ctx, const JobSubmittedEvent &event);

    QMap<QString, TileTemporalFilesInfo> GroupTiles(EventProcessingContext &ctx, int siteId, const QStringList &listAllProductsTiles, ProductType productType);
    QString GetProductFormatterTile(const QString &tile);

    void SubmitTasks(EventProcessingContext &ctx, int jobId, const QList<std::reference_wrapper<TaskToSubmit> > &tasks);
    QMap<ProcessorHandlerHelper::SatelliteIdType, TileList> GetSiteTiles(EventProcessingContext &ctx, int siteId);
    bool IsCloudOptimizedGeotiff(const std::map<QString, QString> &configParameters);

private:
    virtual void HandleProductAvailableImpl(EventProcessingContext &ctx,
                                            const ProductAvailableEvent &event);
    virtual void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                        const JobSubmittedEvent &event) = 0;
    virtual void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                        const TaskFinishedEvent &event) = 0;
    virtual ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) = 0;

protected:
    ProcessorDescription processorDescr;
};
