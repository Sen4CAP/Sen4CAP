#pragma once

#include <QString>
#include <QDBusArgument>
#include <QMetaType>
#include <QDate>
#include <QDateTime>

#include <optional.hpp>

void registerMetaTypes();

class ConfigurationParameterInfo
{
public:
    QString key;
    int categoryId;
    QString friendlyName;
    QString dataType;
    bool isAdvanced;

    ConfigurationParameterInfo();
    ConfigurationParameterInfo(
        QString key, int categoryId, QString friendlyName, QString dataType, bool isAdvanced);

    static void registerMetaTypes();
};

typedef QList<ConfigurationParameterInfo> ConfigurationParameterInfoList;

Q_DECLARE_METATYPE(ConfigurationParameterInfo)
Q_DECLARE_METATYPE(ConfigurationParameterInfoList)

QDBusArgument &operator<<(QDBusArgument &argument, const ConfigurationParameterInfo &parameter);
const QDBusArgument &operator>>(const QDBusArgument &argument,
                                ConfigurationParameterInfo &parameter);

class ConfigurationParameterValue
{
public:
    QString key;
    std::experimental::optional<int> siteId;
    QString value;

    ConfigurationParameterValue();
    ConfigurationParameterValue(QString key,
                                std::experimental::optional<int> siteId,
                                QString value);

    static void registerMetaTypes();
};

typedef QList<ConfigurationParameterValue> ConfigurationParameterValueList;
typedef QMap<QString, ConfigurationParameterValue> ConfigurationParameterValueMap;

Q_DECLARE_METATYPE(ConfigurationParameterValue)
Q_DECLARE_METATYPE(ConfigurationParameterValueList)
Q_DECLARE_METATYPE(ConfigurationParameterValueMap)

QDBusArgument &operator<<(QDBusArgument &argument, const ConfigurationParameterValue &parameter);
const QDBusArgument &operator>>(const QDBusArgument &argument,
                                ConfigurationParameterValue &parameter);

class JobConfigurationParameterValue
{
public:
    QString key;
    QString value;

    JobConfigurationParameterValue();
    JobConfigurationParameterValue(QString key, QString value);

    static void registerMetaTypes();
};

typedef QList<JobConfigurationParameterValue> JobConfigurationParameterValueList;

Q_DECLARE_METATYPE(JobConfigurationParameterValue)
Q_DECLARE_METATYPE(JobConfigurationParameterValueList)

QDBusArgument &operator<<(QDBusArgument &argument, const JobConfigurationParameterValue &parameter);
const QDBusArgument &operator>>(const QDBusArgument &argument,
                                JobConfigurationParameterValue &parameter);

class ConfigurationCategory
{
public:
    int categoryId;
    QString name;
    bool allowPerSiteCustomization;

    ConfigurationCategory();
    ConfigurationCategory(int categoryId, QString name, bool allowPerSiteCustomization);

    static void registerMetaTypes();
};

typedef QList<ConfigurationCategory> ConfigurationCategoryList;

Q_DECLARE_METATYPE(ConfigurationCategory)
Q_DECLARE_METATYPE(ConfigurationCategoryList)

QDBusArgument &operator<<(QDBusArgument &argument, const ConfigurationCategory &category);
const QDBusArgument &operator>>(const QDBusArgument &argument, ConfigurationCategory &category);

class Site
{
public:
    int siteId;
    QString name;
    QString shortName;

    Site();
    Site(int siteId, QString name, QString shortName);

    static void registerMetaTypes();
};

typedef QList<Site> SiteList;

Q_DECLARE_METATYPE(Site)
Q_DECLARE_METATYPE(SiteList)

QDBusArgument &operator<<(QDBusArgument &argument, const Site &site);
const QDBusArgument &operator>>(const QDBusArgument &argument, Site &site);

class ConfigurationSet
{
public:
    ConfigurationCategoryList categories;
    ConfigurationParameterInfoList parameterInfo;
    ConfigurationParameterValueList parameterValues;
    SiteList sites;
    bool isAdmin;

    ConfigurationSet();
    ConfigurationSet(ConfigurationCategoryList categories,
                     ConfigurationParameterInfoList parameterInfo,
                     ConfigurationParameterValueList parameterValues,
                     SiteList sites,
                     bool isAdmin);

    static void registerMetaTypes();
};

Q_DECLARE_METATYPE(ConfigurationSet)

QDBusArgument &operator<<(QDBusArgument &argument, const ConfigurationSet &parameter);
const QDBusArgument &operator>>(const QDBusArgument &argument, ConfigurationSet &parameter);

class ConfigurationUpdateAction
{
public:
    QString key;
    std::experimental::optional<int> siteId;
    std::experimental::optional<QString> value;

    ConfigurationUpdateAction();
    ConfigurationUpdateAction(QString key,
                              std::experimental::optional<int> siteId,
                              std::experimental::optional<QString> value);

    static void registerMetaTypes();
};

typedef QList<ConfigurationUpdateAction> ConfigurationUpdateActionList;

Q_DECLARE_METATYPE(ConfigurationUpdateAction)
Q_DECLARE_METATYPE(ConfigurationUpdateActionList)

QDBusArgument &operator<<(QDBusArgument &argument, const ConfigurationUpdateAction &action);
const QDBusArgument &operator>>(const QDBusArgument &argument, ConfigurationUpdateAction &action);

class JobConfigurationUpdateAction
{
public:
    QString key;
    QString value;

    JobConfigurationUpdateAction();
    JobConfigurationUpdateAction(QString key, QString value);

    static void registerMetaTypes();
};

typedef QList<JobConfigurationUpdateAction> JobConfigurationUpdateActionList;

Q_DECLARE_METATYPE(JobConfigurationUpdateAction)
Q_DECLARE_METATYPE(JobConfigurationUpdateActionList)

QDBusArgument &operator<<(QDBusArgument &argument, const JobConfigurationUpdateAction &action);
const QDBusArgument &operator>>(const QDBusArgument &argument,
                                JobConfigurationUpdateAction &action);

class KeyedMessage
{
public:
    QString key;
    QString text;

    KeyedMessage();
    KeyedMessage(QString key, QString text);

    static void registerMetaTypes();
};

typedef QList<KeyedMessage> KeyedMessageList;

Q_DECLARE_METATYPE(KeyedMessage)
Q_DECLARE_METATYPE(KeyedMessageList)

QDBusArgument &operator<<(QDBusArgument &argument, const KeyedMessage &message);
const QDBusArgument &operator>>(const QDBusArgument &argument, KeyedMessage &message);

enum class ProductType {
    InvalidProductTypeId = 0,
    L2AProductTypeId = 1,
    L3AProductTypeId = 2,
    L3BProductTypeId = 3,
    L3EProductTypeId = 4,
    L4AProductTypeId = 5,
    L4BProductTypeId = 6,
    L1CProductTypeId = 7,
    L3CProductTypeId = 8,
    L3DProductTypeId = 9,
    S4CS1L2AmpProductTypeId = 10,
    S4CS1L2CoheProductTypeId = 11,
    S4CL4AProductTypeId = 12,
    S4CL4BProductTypeId = 13,
    S4CLPISProductTypeId = 14,
    S4CL4CProductTypeId = 15,
    S4CL3CProductTypeId = 16,
    S4MDB1ProductTypeId = 17,
    S4MDB2ProductTypeId = 18,
    S4MDB3ProductTypeId = 19,
    S4MDBL4AOptMainProductTypeId = 20,
    S4MDBL4AOptReProductTypeId = 21,
    S4MDBL4ASarMainProductTypeId = 22,
    S4MDBL4ASarTempProductTypeId = 23,
    // TODO                      = 24,
    FMaskProductTypeId           = 25,
    MaskedL2AProductTypeId       = 26,
    S4SPermCropsProductTypeId    = 27,
    S4SYieldFeatProductTypeId    = 28,
    ERA5WeatherProductTypeId     = 29
};

QDBusArgument &operator<<(QDBusArgument &argument, const ProductType &productType);
const QDBusArgument &operator>>(const QDBusArgument &argument, ProductType &productType);

typedef QStringList TileIdList;

class Product
{
public:
    int productId;
    ProductType productTypeId;
    int siteId;
    QString fullPath;
    QDateTime created;
    QDateTime inserted;
    int satId;
    QString name;
    QString quicklookImage;
    QString geog;
    int orbitId;
    TileIdList tiles;
    int downloaderHistoryId;

    Product();
    Product(int productId,
            ProductType productTypeId,
            int siteId,
            QString fullPath,
            QDateTime created,
            QDateTime inserted,
            int satId,
            QString name,
            QString quicklookImage,
            QString geog,
            int orbitId,
            TileIdList tiles,
            int downloaderHistoryId);

    static void registerMetaTypes();
};

typedef QList<Product> ProductList;

Q_DECLARE_METATYPE(Product)
Q_DECLARE_METATYPE(ProductList)

QDBusArgument &operator<<(QDBusArgument &argument, const Product &message);
const QDBusArgument &operator>>(const QDBusArgument &argument, Product &message);

class L1CProduct
{
public:
    int productId;
    int satelliteId;
    int statusId;
    int siteId;
    QString fullPath;
    QDateTime created;
    QDateTime inserted;

    L1CProduct();
    L1CProduct(int productId,
            int satelliteId,
            int statusId,
            int siteId,
            QString fullPath,
            QDateTime created,
            QDateTime inserted);

    static void registerMetaTypes();
};

typedef QList<L1CProduct> L1CProductList;
typedef QMap<int,int> ProductIdToDwnHistIdMap;
typedef QList<int> ProductIdsList;
typedef QList<int> StatusesList;
typedef QList<int> SatellitesList;

Q_DECLARE_METATYPE(L1CProduct)
Q_DECLARE_METATYPE(L1CProductList)

QDBusArgument &operator<<(QDBusArgument &argument, const L1CProduct &message);
const QDBusArgument &operator>>(const QDBusArgument &argument, L1CProduct &message);

enum class Satellite
{
    Invalid = 0,
    Sentinel2 = 1,
    Landsat8 = 2,
    Sentinel1 = 3,
};

class Tile
{
public:
    Satellite satellite;
    QString tileId;

    Tile();
    Tile(Satellite satellite, QString tileId);
};

typedef QList<Tile> TileList;

class ProductToArchive
{
public:
    int productId;
    QString currentPath;
    QString archivePath;

    ProductToArchive();
    ProductToArchive(int productId, QString currentPath, QString archivePath);

    static void registerMetaTypes();
};

typedef QList<ProductToArchive> ProductToArchiveList;

Q_DECLARE_METATYPE(ProductToArchive)
Q_DECLARE_METATYPE(ProductToArchiveList)

QDBusArgument &operator<<(QDBusArgument &argument, const ProductToArchive &message);
const QDBusArgument &operator>>(const QDBusArgument &argument, ProductToArchive &message);

class ArchivedProduct
{
public:
    int productId;
    QString archivePath;

    ArchivedProduct();
    ArchivedProduct(int productId, QString archivePath);

    static void registerMetaTypes();
};

typedef QList<ArchivedProduct> ArchivedProductList;

Q_DECLARE_METATYPE(ArchivedProduct)
Q_DECLARE_METATYPE(ArchivedProductList)

QDBusArgument &operator<<(QDBusArgument &argument, const ArchivedProduct &message);
const QDBusArgument &operator>>(const QDBusArgument &argument, ArchivedProduct &message);

enum class JobStartType {
    Triggered = 1,
    Requested = 2,
    Scheduled = 3
};

QDBusArgument &operator<<(QDBusArgument &argument, JobStartType startType);
const QDBusArgument &operator>>(const QDBusArgument &argument, JobStartType &startType);

enum class ExecutionStatus {
    Submitted = 1,
    PendingStart,
    NeedsInput,
    Running,
    Paused,
    Finished,
    Cancelled,
    Error
};

typedef QList<ExecutionStatus> ExecutionStatusList;

Q_DECLARE_METATYPE(ExecutionStatusList)

QDBusArgument &operator<<(QDBusArgument &argument, ExecutionStatus status);
const QDBusArgument &operator>>(const QDBusArgument &argument, ExecutionStatus &status);

QDBusArgument &operator<<(QDBusArgument &argument, const ExecutionStatusList &statusList);
const QDBusArgument &operator>>(const QDBusArgument &argument, ExecutionStatusList &statusList);

class NewJob
{
public:
    QString name;
    QString description;
    int processorId;
    int siteId;
    JobStartType startType;
    QString parametersJson;
    JobConfigurationUpdateActionList configuration;

    NewJob();
    NewJob(QString name,
           QString description,
           int processorId,
           int siteId,
           JobStartType startType,
           QString parametersJson,
           JobConfigurationUpdateActionList configuration);

    static void registerMetaTypes();
};

Q_DECLARE_METATYPE(NewJob)

QDBusArgument &operator<<(QDBusArgument &argument, const NewJob &job);
const QDBusArgument &operator>>(const QDBusArgument &argument, NewJob &job);

typedef QList<int> TaskIdList;

Q_DECLARE_METATYPE(TaskIdList)

class NewTask
{
public:
    int jobId;
    QString module;
    QString parametersJson;
    TaskIdList parentTasks;

    NewTask();
    NewTask(int jobId, QString module, QString parametersJson, TaskIdList parentTasks);

    static void registerMetaTypes();
};

Q_DECLARE_METATYPE(NewTask)

QDBusArgument &operator<<(QDBusArgument &argument, const NewTask &task);
const QDBusArgument &operator>>(const QDBusArgument &argument, NewTask &task);

class NewStep
{
public:
    int taskId;
    QString name;
    QString parametersJson;

    NewStep();
    NewStep(int taskId, QString name, QString parametersJson);

    static void registerMetaTypes();
};

typedef QList<NewStep> NewStepList;

Q_DECLARE_METATYPE(NewStep)
Q_DECLARE_METATYPE(NewStepList)

QDBusArgument &operator<<(QDBusArgument &argument, const NewStep &step);
const QDBusArgument &operator>>(const QDBusArgument &argument, NewStep &step);

class ExecutionStatistics
{
public:
    QString node;
    int32_t exitCode;
    int64_t userCpuMs;
    int64_t systemCpuMs;
    int64_t durationMs;
    int32_t maxRssKb;
    int32_t maxVmSizeKb;
    int64_t diskReadBytes;
    int64_t diskWriteBytes;
    QString stdOutText;
    QString stdErrText;

    ExecutionStatistics();
    ExecutionStatistics(QString node,
                        int32_t exitCode,
                        int64_t userCpuMs,
                        int64_t systemCpuMs,
                        int64_t durationMs,
                        int32_t maxRssKb,
                        int32_t maxVmSizeKb,
                        int64_t diskReadBytes,
                        int64_t diskWriteBytes,
                        QString stdOutText,
                        QString stdErrText);

    static void registerMetaTypes();
};

Q_DECLARE_METATYPE(ExecutionStatistics)

QDBusArgument &operator<<(QDBusArgument &argument, const ExecutionStatistics &statistics);
const QDBusArgument &operator>>(const QDBusArgument &argument, ExecutionStatistics &statistics);

enum class EventType {
    TaskRunnable = 1,
    TaskFinished,
    ProductAvailable,
    JobCancelled,
    JobPaused,
    JobResumed,
    JobSubmitted,
    StepFailed
};

QDBusArgument &operator<<(QDBusArgument &argument, EventType event);
const QDBusArgument &operator>>(const QDBusArgument &argument, EventType &event);

class TaskRunnableEvent
{
public:
    int jobId;
    int processorId;
    int taskId;

    TaskRunnableEvent();
    TaskRunnableEvent(int jobId, int processorId, int taskId);

    QString toJson() const;

    static TaskRunnableEvent fromJson(const QString &json);

    static void registerMetaTypes();
};

Q_DECLARE_METATYPE(TaskRunnableEvent)

QDBusArgument &operator<<(QDBusArgument &argument, const TaskRunnableEvent &event);
const QDBusArgument &operator>>(const QDBusArgument &argument, TaskRunnableEvent &event);

class TaskFinishedEvent
{
public:
    int processorId;
    int siteId;
    int jobId;
    int taskId;
    QString module;

    TaskFinishedEvent();
    TaskFinishedEvent(int processorId, int siteId, int jobId, int taskId, QString module);

    QString toJson() const;

    static TaskFinishedEvent fromJson(const QString &json);

    static void registerMetaTypes();
};

Q_DECLARE_METATYPE(TaskFinishedEvent)

QDBusArgument &operator<<(QDBusArgument &argument, const TaskFinishedEvent &event);
const QDBusArgument &operator>>(const QDBusArgument &argument, TaskFinishedEvent &event);

class ProductAvailableEvent
{
public:
    int productId;

    ProductAvailableEvent();
    ProductAvailableEvent(int productId);

    QString toJson() const;

    static ProductAvailableEvent fromJson(const QString &json);

    static void registerMetaTypes();
};

Q_DECLARE_METATYPE(ProductAvailableEvent)

QDBusArgument &operator<<(QDBusArgument &argument, const ProductAvailableEvent &event);
const QDBusArgument &operator>>(const QDBusArgument &argument, ProductAvailableEvent &event);

class JobCancelledEvent
{
public:
    int jobId;

    JobCancelledEvent();
    JobCancelledEvent(int jobId);

    QString toJson() const;

    static JobCancelledEvent fromJson(const QString &json);

    static void registerMetaTypes();
};

Q_DECLARE_METATYPE(JobCancelledEvent)

QDBusArgument &operator<<(QDBusArgument &argument, const JobCancelledEvent &event);
const QDBusArgument &operator>>(const QDBusArgument &argument, JobCancelledEvent &event);

class JobPausedEvent
{
public:
    int jobId;

    JobPausedEvent();
    JobPausedEvent(int jobId);

    QString toJson() const;

    static JobPausedEvent fromJson(const QString &json);

    static void registerMetaTypes();
};

Q_DECLARE_METATYPE(JobPausedEvent)

QDBusArgument &operator<<(QDBusArgument &argument, const JobPausedEvent &event);
const QDBusArgument &operator>>(const QDBusArgument &argument, JobPausedEvent &event);

class JobResumedEvent
{
public:
    int jobId;
    int processorId;

    JobResumedEvent();
    JobResumedEvent(int jobId, int processorId);

    QString toJson() const;

    static JobResumedEvent fromJson(const QString &json);

    static void registerMetaTypes();
};

Q_DECLARE_METATYPE(JobResumedEvent)

QDBusArgument &operator<<(QDBusArgument &argument, const JobResumedEvent &event);
const QDBusArgument &operator>>(const QDBusArgument &argument, JobResumedEvent &event);

class JobSubmittedEvent
{
public:
    int jobId;
    int processorId;
    int siteId;
    QString parametersJson;

    JobSubmittedEvent();
    JobSubmittedEvent(int jobId, int processorId, int siteId, QString parametersJson);

    QString toJson() const;

    static JobSubmittedEvent fromJson(const QString &json);

    static void registerMetaTypes();
};

Q_DECLARE_METATYPE(JobSubmittedEvent)

QDBusArgument &operator<<(QDBusArgument &argument, const JobSubmittedEvent &event);
const QDBusArgument &operator>>(const QDBusArgument &argument, JobSubmittedEvent &event);

class StepFailedEvent
{
public:
    int jobId;
    int taskId;
    QString stepName;

    StepFailedEvent();
    StepFailedEvent(int jobId, int taskId, QString stepName);

    QString toJson() const;

    static StepFailedEvent fromJson(const QString &json);

    static void registerMetaTypes();
};

Q_DECLARE_METATYPE(StepFailedEvent)

QDBusArgument &operator<<(QDBusArgument &argument, const StepFailedEvent &event);
const QDBusArgument &operator>>(const QDBusArgument &argument, StepFailedEvent &event);

class UnprocessedEvent
{
public:
    int eventId;
    EventType type;
    QString dataJson;
    QDateTime submittedTime;
    std::experimental::optional<QDateTime> processingStartedTime;

    UnprocessedEvent();
    UnprocessedEvent(int eventId,
                     EventType type,
                     QString dataJson,
                     QDateTime submittedTime,
                     std::experimental::optional<QDateTime> processingStartedTime);

    static void registerMetaTypes();
};

typedef QList<UnprocessedEvent> UnprocessedEventList;

Q_DECLARE_METATYPE(UnprocessedEvent)
Q_DECLARE_METATYPE(UnprocessedEventList)

QDBusArgument &operator<<(QDBusArgument &argument, const UnprocessedEvent &event);
const QDBusArgument &operator>>(const QDBusArgument &argument, UnprocessedEvent &event);

class NodeStatistics
{
public:
    QString node;
    double cpuUser;
    double cpuSystem;
    int64_t memTotalKb;
    int64_t memUsedKb;
    int64_t swapTotalKb;
    int64_t swapUsedKb;
    double loadAvg1;
    double loadAvg5;
    double loadAvg15;
    int64_t diskTotalBytes;
    int64_t diskUsedBytes;

    NodeStatistics();
    NodeStatistics(QString node,
                   double cpuUser,
                   double cpuSystem,
                   int64_t memTotalKb,
                   int64_t memUsedKb,
                   int64_t swapTotalKb,
                   int64_t swapUsedKb,
                   double loadAvg1,
                   double loadAvg5,
                   double loadAvg15,
                   int64_t diskTotalBytes,
                   int64_t diskUsedBytes);

    static void registerMetaTypes();
};

Q_DECLARE_METATYPE(NodeStatistics)

QDBusArgument &operator<<(QDBusArgument &argument, const NodeStatistics &statistics);
const QDBusArgument &operator>>(const QDBusArgument &argument, NodeStatistics &statistics);

class StepArgument
{
public:
    QString value;

    StepArgument();
    StepArgument(QString value);

    static void registerMetaTypes();
};

typedef QList<StepArgument> StepArgumentList;

Q_DECLARE_METATYPE(StepArgument)
Q_DECLARE_METATYPE(StepArgumentList)

QDBusArgument &operator<<(QDBusArgument &argument, const StepArgument &stepArgument);
const QDBusArgument &operator>>(const QDBusArgument &argument, StepArgument &stepArgument);

class NewExecutorStep
{
public:
    int processorId;
    int taskId;
    QString processorPath;
    QString stepName;
    StepArgumentList arguments;

    NewExecutorStep();
    NewExecutorStep(int processorId,
                    int taskId,
                    QString processorPath,
                    QString stepName,
                    StepArgumentList arguments);

    QString toJson() const;
    static NewExecutorStep fromJson(const QString &json);

    static void registerMetaTypes();
};

typedef QList<NewExecutorStep> NewExecutorStepList;

Q_DECLARE_METATYPE(NewExecutorStep)
Q_DECLARE_METATYPE(NewExecutorStepList)

QDBusArgument &operator<<(QDBusArgument &argument, const NewExecutorStep &step);
const QDBusArgument &operator>>(const QDBusArgument &argument, NewExecutorStep &step);

class JobStepToRun
{
public:
    int taskId;
    QString module;
    QString stepName;
    QString parametersJson;

    JobStepToRun();
    JobStepToRun(int taskId, QString module, QString stepName, QString parametersJson);

    static void registerMetaTypes();
};

typedef QList<JobStepToRun> JobStepToRunList;

class JobStep : public JobStepToRun
{
public:
    QList<int> precedingTaskIds;

    JobStep();
    JobStep(int taskId, QString module, QString stepName, QString parametersJson, QList<int> taskIds);

};

typedef QList<JobStep> JobStepList;

Q_DECLARE_METATYPE(JobStepToRun)
Q_DECLARE_METATYPE(JobStepToRunList)

QDBusArgument &operator<<(QDBusArgument &argument, const JobStepToRun &step);
const QDBusArgument &operator>>(const QDBusArgument &argument, JobStepToRun &step);

class StepConsoleOutput
{
public:
    int taskId;
    QString stepName;
    QString stdOutText;
    QString stdErrText;

    StepConsoleOutput();
    StepConsoleOutput(int taskId, QString stepName, QString stdOutText, QString stdErrText);

    static void registerMetaTypes();
};

typedef QList<StepConsoleOutput> StepConsoleOutputList;

Q_DECLARE_METATYPE(StepConsoleOutput)
Q_DECLARE_METATYPE(StepConsoleOutputList)

QDBusArgument &operator<<(QDBusArgument &argument, const StepConsoleOutput &stepOutput);
const QDBusArgument &operator>>(const QDBusArgument &argument, StepConsoleOutput &stepOutput);

class NewProduct
{
public:
    ProductType productType;
    int processorId;
    int satelliteId;
    int siteId;
    int jobId;
    QString fullPath;
    QDateTime createdTimestamp;
    QString name;
    QString quicklookImage;
    QString footprint;
    std::experimental::optional<int> orbitId;
    TileIdList tiles;
    ProductIdsList parentProductIds;

    NewProduct();
    NewProduct(ProductType productType,
               int processorId,
               int siteId,
               int jobId,
               QString fullPath,
               QDateTime createdTimestamp,
               QString name,
               QString quicklookImage,
               QString footprint,
               std::experimental::optional<int> orbitId,
               TileIdList tiles,
               ProductIdsList parentProductIds);
    NewProduct(ProductType productType,
               int processorId,
               int satelliteId,
               int siteId,
               int jobId,
               QString fullPath,
               QDateTime createdTimestamp,
               QString name,
               QString quicklookImage,
               QString footprint,
               std::experimental::optional<int> orbitId,
               TileIdList tiles,
               ProductIdsList parentProductIds);

    static void registerMetaTypes();
};

class DashboardSearch
{
public:
    std::experimental::optional<int> siteId;
    std::experimental::optional<int> processorId;

    DashboardSearch();
    DashboardSearch(std::experimental::optional<int> siteId,
                    std::experimental::optional<int> processorId);

    static void registerMetaTypes();
};

Q_DECLARE_METATYPE(DashboardSearch)

QDBusArgument &operator<<(QDBusArgument &argument, const DashboardSearch &search);
const QDBusArgument &operator>>(const QDBusArgument &argument, DashboardSearch &search);

class ProcessorDescription
{
public:
    int processorId;
    QString shortName;
    QString fullName;

    ProcessorDescription();
    ProcessorDescription(int processorId, QString shortName, QString fullName);

    static void registerMetaTypes();
};

Q_DECLARE_METATYPE(ProcessorDescription)

QDBusArgument &operator<<(QDBusArgument &argument, const ProcessorDescription &product);
const QDBusArgument &operator>>(const QDBusArgument &argument, ProcessorDescription &product);

typedef QList<ProcessorDescription> ProcessorDescriptionList;

//** For scheduler component
struct ScheduledTaskStatus
{
    int id;
    int taskId;

    QDateTime nextScheduledRunTime;

    QDateTime lastSuccesfullScheduledRun; // last succ. scheduleded launch
    QDateTime lastSuccesfullTimestamp;    // the moment of last launch
    QDateTime lastRetryTime;

    QDateTime estimatedRunTime; // last succ. scheduleded launch
};

enum RepeatType {
    REPEATTYPE_ONCE = 0,
    REPEATTYPE_CYCLIC = 1,
    REPEATTYPE_ONDATE = 2
};
struct ScheduledTask
{
    ScheduledTask(int ti,
                  QString tn,
                  int pi,
                  int si,
                  int seasonId,
                  QString pp,
                  int rt,
                  int rad,
                  int rmd,
                  QDateTime fst,
                  int rp,
                  int tp,
                  ScheduledTaskStatus &ts);
    ScheduledTask()
    {
    }

    int taskId;
    QString taskName;
    int processorId;
    int siteId;
    int seasonId;
    QString processorParameters;

    int repeatType;       /*once, cyclic, on_date*/
    int repeatAfterDays;  /* nr of days to cycle the task */
    int repeatOnMonthDay; /* the day of the month to run the task */

    QDateTime firstScheduledRunTime; /* first configured run-time */

    int retryPeriod; /* minutes or hours ? to retry if the preconditions are not met */

    int taskPriority;

    ScheduledTaskStatus taskStatus;
};
//** END For scheduler component

//** For orchestartor API
struct ProcessingRequest
{
    int processorId;
    int siteId;
    int ttNextScheduledRunTime;
    QString parametersJson; // or map<string, string>

    QString toJson() const;

    static ProcessingRequest fromJson(const QString &json);

};
Q_DECLARE_METATYPE(ProcessingRequest)
QDBusArgument &operator<<(QDBusArgument &argument, const ProcessingRequest &request);
const QDBusArgument &operator>>(const QDBusArgument &argument, ProcessingRequest &request);

struct JobDefinition
{
    bool isValid;
    int processorId;
    int siteId;
    QString jobDefinitionJson;

    QString toJson() const;

    static JobDefinition fromJson(const QString &json);

};
Q_DECLARE_METATYPE(JobDefinition)
QDBusArgument &operator<<(QDBusArgument &argument, const JobDefinition &job);
const QDBusArgument &operator>>(const QDBusArgument &argument, JobDefinition &job);

struct ProcessorJobDefinitionParams
{
    bool isValid;
    ProductList productList;
    QString jsonParameters;
};
Q_DECLARE_METATYPE(ProcessorJobDefinitionParams)
QDBusArgument &operator<<(QDBusArgument &argument, const ProcessorJobDefinitionParams &job);
const QDBusArgument &operator>>(const QDBusArgument &argument, ProcessorJobDefinitionParams &job);

typedef QList<int> JobIdsList;

//** END for orchestartor API

class Season
{
public:
    int seasonId;
    int siteId;
    QString name;
    QDate startDate;
    QDate endDate;
    QDate midDate;
    bool enabled;

    Season();
    Season(int seasonId, int siteId, QString name, QDate startDate, QDate endDate, QDate midDate, bool enabled);
};

typedef QList<Season> SeasonList;
