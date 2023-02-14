#pragma once

#include "model.hpp"
#include "eventprocessingcontext.hpp"
#include "schedulingcontext.h"
#include "processorhandlerhelper.h"

#include <memory>
#include <mutex>

#define PRODUCTS_LOCATION_CFG_KEY "archiver.archive_path"
#define PRODUCT_FORMATTER_OUT_PROPS_FILE "product_properties.txt"
#define PRODUCT_FORMATTER_IN_PRD_IDS_FILE "input_product_ids.txt"

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
    void HandleJobUnsuccefulStop(EventProcessingContext &ctx, int jobId);

    ProcessorJobDefinitionParams GetProcessingDefinition(SchedulingContext &ctx, int siteId, int scheduledDate,
                                          const ConfigurationParameterValueMap &requestOverrideCfgValues);

    static QStringList FilterProducts(const QStringList &products, const ProductType &prdType);
    static QStringList GetInputProductNames(const QJsonObject &parameters, const QString &paramsCfgKey);
    static QStringList GetInputProductNames(const QJsonObject &parameters, const ProductType &prdType = ProductType::InvalidProductTypeId);
    static ProductList GetInputProducts(EventProcessingContext &ctx, const QJsonObject &parameters, int siteId,
                                            const ProductType &prdType = ProductType::InvalidProductTypeId,
                                            QDateTime *pMinDate = NULL, QDateTime *pMaxDate = NULL);
protected:
    NewStep CreateTaskStep(TaskToSubmit &task, const QString &stepName, const QStringList &stepArgs);

    QString GetFinalProductFolder(EventProcessingContext &ctx, int jobId, int siteId);
    bool NeedRemoveJobFolder(EventProcessingContext &ctx, int jobId, const QString &procName);
    bool RemoveJobFolder(EventProcessingContext &ctx, int jobId, const QString &procName);
    QString GetTaskOutputPathFromEvt(EventProcessingContext &ctx, const TaskFinishedEvent &event);
    void WriteOutputProductPath(TaskToSubmit &prdCreatorTask, const QString &prdPath);
    QString GetOutputProductPath(EventProcessingContext &ctx, const TaskFinishedEvent &outPrdCreatorTaskEndEvt);
    void WriteOutputProductSourceProductIds(TaskToSubmit &prdCreatorTask, const QList<int> &parentIds);
    ProductIdsList GetOutputProductParentProductIds(EventProcessingContext &ctx, const TaskFinishedEvent &outPrdCreatorTaskEndEvt);
    QString GetOutputProductName(EventProcessingContext &ctx, const TaskFinishedEvent &event);
    QString GetProductFormatterQuicklook(EventProcessingContext &ctx, const TaskFinishedEvent &prdFrmtTaskEndEvt);
    QString GetProductFormatterFootprint(EventProcessingContext &ctx, const TaskFinishedEvent &prdFrmtTaskEndEvt);
    Season GetSeason(ExecutionContextBase &ctx, int siteId, const QDateTime &executionDate);
    bool GetSeasonStartEndDates(const SeasonList &seasons,
                                   QDateTime &startTime, QDateTime &endTime,
                                   const QDateTime &executionDate,
                                   const ConfigurationParameterValueMap &requestOverrideCfgValues);

    bool GetSeasonStartEndDates(ExecutionContextBase &ctx, int siteId,
                                   QDateTime &startTime, QDateTime &endTime,
                                   const QDateTime &executionDate,
                                   const ConfigurationParameterValueMap &requestOverrideCfgValues);
    bool GetBestSeasonToMatchDate(ExecutionContextBase &ctx, int siteId,
                                  QDateTime &startTime, QDateTime &endTime,
                                  const QDateTime &executionDate, const ConfigurationParameterValueMap &requestOverrideCfgValues);
    QString GetProductFormatterTile(const QString &tile);
    TilesTimeSeries GroupL2ATiles(EventProcessingContext &ctx, const QList<ProductDetails> &productDetails);

    void SubmitTasks(EventProcessingContext &ctx, int jobId, const QList<std::reference_wrapper<TaskToSubmit> > &tasks);
    QMap<Satellite, TileList> GetSiteTiles(EventProcessingContext &ctx, int siteId);
    bool IsCloudOptimizedGeotiff(const std::map<QString, QString> &configParameters);
    static bool IsL2AValidityMaskEnabled(EventProcessingContext &ctx, const QJsonObject &parameters, int siteId);
    QStringList GetDefaultProductFormatterArgs(EventProcessingContext &ctx, TaskToSubmit &productFormatterTask,
                                               int jobId, int siteId, const QString &level, const QString &timePeriod,
                                                const QString &processor, const QStringList &additionalParameters,
                                                bool isVectPrd = false, const QString &gipp = "", bool compress = true);
    bool CheckAllAncestorProductCreation(ExecutionContextBase &ctx, int siteId, const ProductType &prdType,
                                         const QDateTime &startDate, const QDateTime &endDate);

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
