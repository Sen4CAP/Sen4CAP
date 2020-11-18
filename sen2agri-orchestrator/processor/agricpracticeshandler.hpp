#ifndef AGRICPRACTICESHANDLER_HPP
#define AGRICPRACTICESHANDLER_HPP

#include "processorhandler.hpp"
#include "s4c_utils.hpp"
#include "s4c_mdb1_dataextract_steps_builder.hpp"

#define L4C_AP_CFG_PREFIX   "processor.s4c_l4c."

typedef struct AgricPracticesSiteCfg {
    AgricPracticesSiteCfg() {

    }
    QString additionalFilesRootDir;

    // Common parameters
    QString country;
    QString year;
    // map from practice name to practices file path
    QMap<QString, QString> practices;

    // LPIS informations
    QString fullDeclsFilePath;

    // TSA parameters
    TQStrQStrMap ccTsaParams;
    TQStrQStrMap flTsaParams;
    TQStrQStrMap nfcTsaParams;
    TQStrQStrMap naTsaParams;

    // TSA minimum acquisitions
    QString tsaMinAcqsNo;

} AgricPracticesSiteCfg;

enum AgricPractOperation {none = 0x00,
                          catchCrop = 0x02,
                          fallow = 0x04,
                          nfc = 0x08,
                          harvestOnly = 0x10,
                          timeSeriesAnalysis = (catchCrop|fallow|nfc|harvestOnly),
                          all = 0xFF};

class AgricPracticesHandler;

typedef struct AgricPracticesJobPayload {
    AgricPracticesJobPayload(EventProcessingContext *pContext, const JobSubmittedEvent &evt) : event(evt) {
        pCtx = pContext;
        parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
        configParameters = pCtx->GetJobConfigurationParameters(evt.jobId, L4C_AP_CFG_PREFIX);
        siteShortName = pContext->GetSiteShortName(evt.siteId);
        int jobVal;
        isScheduledJob = ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "scheduled_job", jobVal) && (jobVal == 1);
        execOper = GetExecutionOperation(parameters, configParameters);
        siteCfg.country = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters,
                                                                   "country", L4C_AP_CFG_PREFIX);
        siteCfg.tsaMinAcqsNo = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters,
                                                                        "tsa_min_acqs_no", L4C_AP_CFG_PREFIX);
        siteCfg.year = GetYear(parameters, configParameters, siteShortName);
    }
    static AgricPractOperation GetExecutionOperation(const QJsonObject &parameters,
                                                     const std::map<QString, QString> &configParameters);
    static QString GetYear(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                      const QString &siteShortName);

    EventProcessingContext *pCtx;
    JobSubmittedEvent event;
    QJsonObject parameters;
    std::map<QString, QString> configParameters;

    AgricPracticesSiteCfg siteCfg;

    QString siteShortName;

    // parameters used for data extraction step
    bool isScheduledJob;

    QDateTime seasonStartDate;
    QDateTime seasonEndDate;

    QDateTime minDate;
    QDateTime maxDate;

    AgricPractOperation execOper;

} AgricPracticesJobPayload;

class AgricPracticesHandler : public ProcessorHandler
{
public:
    static AgricPractOperation GetExecutionOperation(const QString &str);

private:
    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &evt) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;
    void HandleProductAvailableImpl(EventProcessingContext &ctx,
                                    const ProductAvailableEvent &event) override;

    void CreateTasks(const AgricPracticesJobPayload &jobCfg, QList<TaskToSubmit> &outAllTasksList, const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder);
    void CreateSteps(QList<TaskToSubmit> &allTasksList,
                     const AgricPracticesJobPayload &siteCfg, const S4CMarkersDB1DataExtractStepsBuilder &dataExtrStepsBuilder, NewStepList &steps);
    void WriteExecutionInfosFile(const QString &executionInfosPath,
                                 const QStringList &listProducts);
    QStringList GetExportProductLauncherArgs(const AgricPracticesJobPayload &jobCfg,
                                            const QString &productFormatterPrdFileIdFile);
    QStringList GetProductFormatterArgs(TaskToSubmit &productFormatterTask, const AgricPracticesJobPayload &jobCfg,
                                        const QStringList &listFiles);

    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;

private:
    bool GetL4CConfigForSiteId(AgricPracticesJobPayload &jobCfg);

    QString GetL4CConfigFilePath(AgricPracticesJobPayload &jobCfg);
    bool LoadL4CConfigFile(AgricPracticesJobPayload &jobCfg, const QString &siteCfgFilePath);

//    bool ValidateProductsForOperation(const AgricPracticesJobPayload &jobCfg, const QStringList &ndviFiles,
//                                      const QStringList &ampFiles, const QStringList &coheFiles);
    // QStringList GetIdsExtractorArgs(const AgricPracticesJobCfg &siteCfg, const QString &outFile, const QString &finalTargetDir);
    // QStringList GetPracticesExtractionArgs(const AgricPracticesJobCfg &siteCfg, const QString &outFile, const QString &practice, const QString &finalTargetDir);
    QStringList GetFilesMergeArgs(const QStringList &listInputPaths, const QString &outFileName, const QDateTime &prdMaxDate);
    QStringList GetTimeSeriesAnalysisArgs(const AgricPracticesJobPayload &jobCfg, const QString &practice, const QString &practicesFile,
                                          const QMap<QString, QString> &inFiles,
                                          const QString &outDir);
    QString BuildMergeResultFileName(const QString &country, const QString &year, const ProductType &prdsType);
    QString BuildPracticesTableResultFileName(const QString &practice, const QString &year, const QString &country = "");

    int CreateMergeTasks(QList<TaskToSubmit> &outAllTasksList, const QString &taskName,
                          int minPrdDataExtrIndex, int maxPrdDataExtrIndex, int &curTaskIdx);
    int CreateTSATasks(const AgricPracticesJobPayload &jobCfg, QList<TaskToSubmit> &outAllTasksList,
                       const QString &practiceName,
                       const QList<int> &mergeTaskIdxs, int &curTaskIdx);

//    QString CreateStepForLPISSelection(const QString &practice, const AgricPracticesJobCfg &jobCfg,
//                                              QList<TaskToSubmit> &allTasksList, NewStepList &steps, int &curTaskIdx);

    QString CreateStepsForFilesMerge(const AgricPracticesJobPayload &jobCfg, const ProductType &prdType, const QStringList &dataExtrDirs,
                                  NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx);

    QStringList CreateTimeSeriesAnalysisSteps(const AgricPracticesJobPayload &jobCfg, const QString &practice,
                                              const QMap<QString, QString> &mergedFiles,
                                              NewStepList &steps, QList<TaskToSubmit> &allTasksList, int &curTaskIdx);

    TQStrQStrMap LoadParamsFromFile(QSettings &settings, const QString &practicePrefix, const QString &sectionName, const AgricPracticesSiteCfg &cfg);
    void UpdatePracticesParams(const TQStrQStrMap &defVals, TQStrQStrMap &sectionVals);

    void UpdatePracticesParams(const QJsonObject &parameters, std::map<QString, QString> &configParameters, const TQStrQStrMap &cfgVals, const QString &prefix, TQStrQStrMap *params);

    QString GetTsaExpectedPractice(const QString &practice);
    bool IsOperationEnabled(AgricPractOperation oper, AgricPractOperation expected);
    bool GetPrevL4CProduct(const AgricPracticesJobPayload &jobCfg,  const QDateTime &seasonStart, const QDateTime &curDate, QString &prevL4cProd);
    ProductList GetLpisProduct(ExecutionContextBase *pCtx, int siteId);
    //QStringList GetAdditionalFilesAsList(const QString &files, const AgricPracticesSiteCfg &cfg);

    QString GetShortNameForProductType(const ProductType &prdType);
    QString GetProcessorDirValue(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                 const QString &key, const QString &siteShortName, const QString &year, const QString &defVal );
    QString GetTsInputTablesDir(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                const QString &siteShortName, const QString &year, const QString &practice);

    QMap<QString, QString> GetPracticeTableFiles(const QJsonObject &parameters,
                                                 const std::map<QString, QString> &configParameters,
                                                 const QString &siteShortName, const QString &year);
    bool CheckExecutionPreconditions(ExecutionContextBase *pCtx, const QJsonObject &parameters,
                                        const std::map<QString, QString> &configParameters, int siteId,
                                        const QString &siteShortName, const QString &year, QString &errMsg);
};

#endif // AGRICPRACTICESHANDLER_HPP
