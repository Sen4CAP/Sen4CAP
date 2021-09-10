#ifndef LAIRETRIEVALHANDLERL3BNEW_HPP
#define LAIRETRIEVALHANDLERL3BNEW_HPP

#include "processorhandler.hpp"

#define L3B_CFG_PREFIX   "processor.l3b."

class LaiRetrievalHandlerL3BNew : public ProcessorHandler
{
public:
    typedef struct {
        typedef struct {
            QString model;
            QString errModel;
            bool NeedsModelGeneration() const { return (model == "" || errModel == ""); }
        } ModelInfos;
        ModelInfos modelInfos;
        QString tileFile;
        QStringList tileIds;
        QString prdExternalMskFile;
        Product parentProductInfo;
    } TileInfos;

    typedef struct {
        QString tileId;
        QString tileFile;
        QString inPrdExtMsk;

        QString ndviFile;
        QString laiFile;
        QString faparFile;
        QString fcoverFile;

        QString statusFlagsFile;
        QString statusFlagsFileResampled;
        QString inDomainFlagsFile;
        QString laiDomainFlagsFile;
        QString faparDomainFlagsFile;
        QString fcoverDomainFlagsFile;

        QString anglesFile;
    } TileResultFiles;

    class L3BJobContext {
        public:
            L3BJobContext(LaiRetrievalHandlerL3BNew *parent, EventProcessingContext *pContext, const JobSubmittedEvent &evt) : event(evt) {
                QString procPrefix("processor." + parent->processorDescr.shortName + ".");
                pCtx = pContext;
                parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
                configParameters = pCtx->GetJobConfigurationParameters(evt.jobId, L3B_CFG_PREFIX);
                // siteShortName = pContext->GetSiteShortName(evt.siteId);
                laiCfgFile = configParameters[procPrefix + "lai.laibandscfgfile"];
                bGenNdvi = IsParamOrConfigKeySet(parameters, configParameters, "produce_ndvi", procPrefix + "filter.produce_ndvi");
                bGenLai = IsParamOrConfigKeySet(parameters, configParameters, "produce_lai", procPrefix + "filter.produce_lai");
                bGenFapar = IsParamOrConfigKeySet(parameters, configParameters, "produce_fapar", procPrefix + "filter.produce_fapar");
                bGenFCover = IsParamOrConfigKeySet(parameters, configParameters, "produce_fcover", procPrefix + "filter.produce_fcover");
                bGenInDomainFlags = IsParamOrConfigKeySet(parameters, configParameters, "indomflags", procPrefix + "filter.produce_in_domain_flags");

                int resolution = 0;
                if(!ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "resolution", resolution) ||
                        resolution == 0) {
                    resolution = 10;    // TODO: We should configure the default resolution in DB
                }
                resolutionStr = QString::number(resolution);

                bRemoveTempFiles = parent->NeedRemoveJobFolder(*pCtx, event.jobId, parent->processorDescr.shortName);

                lutFile = ProcessorHandlerHelper::GetMapValue(configParameters, procPrefix + "lai.lut_path");

            }

            bool IsParamOrConfigKeySet(const QJsonObject &parameters, std::map<QString, QString> &configParameters,
                                       const QString &cmdLineParamName, const QString &cfgParamKey, bool defVal = true) {
                bool bIsConfigKeySet = defVal;
                if(parameters.contains(cmdLineParamName)) {
                    const auto &value = parameters[cmdLineParamName];
                    if(value.isDouble())
                        bIsConfigKeySet = (value.toInt() != 0);
                    else if(value.isString()) {
                        bIsConfigKeySet = (value.toString() == "1");
                    }
                } else {
                    if (cfgParamKey != "") {
                        bIsConfigKeySet = ((configParameters[cfgParamKey]).toInt() != 0);
                    }
                }
                return bIsConfigKeySet;
            }

            EventProcessingContext *pCtx;
            JobSubmittedEvent event;
            QJsonObject parameters;
            std::map<QString, QString> configParameters;

            //QString siteShortName;
            QString laiCfgFile;
            bool bGenNdvi;
            bool bGenLai;
            bool bGenFapar;
            bool bGenFCover;
            bool bGenInDomainFlags;
            QString resolutionStr;
            bool bRemoveTempFiles;
            QString lutFile;
    };


private:

    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &evt) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;

    void CreateTasksForNewProduct(const L3BJobContext &jobCtx, QList<TaskToSubmit> &outAllTasksList,
                                   const QList<TileInfos> &tileInfosList);
    int CreateAnglesTasks(int parentTaskId, QList<TaskToSubmit> &outAllTasksList, int nCurTaskIdx, int & nAnglesTaskId);
    int CreateBiophysicalIndicatorTasks(int parentTaskId, QList<TaskToSubmit> &outAllTasksList,
                                         QList<std::reference_wrapper<const TaskToSubmit>> &productFormatterParentsRefs,
                                         int nCurTaskIdx);

    void GetModelFileList(const QString &folderName, const QString &modelPrefix, QStringList &outModelsList);
    void WriteExecutionInfosFile(const QString &executionInfosPath, const QList<TileResultFiles> &tileResultFilesList);
    void WriteInputPrdIdsInfosFile(const QString &outFilePath, const QList<TileInfos> &prdTilesInfosList);

    QStringList GetCreateAnglesArgs(const QString &inputProduct, const QString &anglesFile);
    QStringList GetGdalTranslateAnglesNoDataArgs(const QString &anglesFile, const QString &resultAnglesFile);
    QStringList GetGdalBuildAnglesVrtArgs(const QString &anglesFile, const QString &resultVrtFile);
    QStringList GetGdalTranslateResampleAnglesArgs(const QString &vrtFile, const QString &resultResampledAnglesFile);
    QStringList GetGenerateInputDomainFlagsArgs(const QString &xmlFile,  const QString &laiBandsCfg,
                                                const QString &outFlagsFileName, const QString &outRes);
    QStringList GetGenerateOutputDomainFlagsArgs(const QString &xmlFile, const QString &laiRasterFile,
                                                const QString &laiBandsCfg, const QString &indexName,
                                                const QString &outFlagsFileName,  const QString &outCorrectedLaiFile, const QString &outRes);

    QStringList GetNdviRviExtractionNewArgs(const QString &inputProduct, const QString &msksFlagsFile,
                                            const QString &ndviFile, const QString &resolution, const QString &laiBandsCfg);
    QStringList GetLaiProcessorArgs(const QString &xmlFile, const QString &anglesFileName, const QString &resolution,
                                    const QString &laiBandsCfg, const QString &monoDateLaiFileName, const QString &indexName);
    QStringList GetQuantifyImageArgs(const QString &inFileName, const QString &outFileName);
    QStringList GetMonoDateMskFlagsArgs(const QString &inputProduct, const QString &extMsk, const QString &monoDateMskFlgsFileName, const QString &monoDateMskFlgsResFileName, const QString &resStr);
    QStringList GetLaiMonoProductFormatterArgs(TaskToSubmit &productFormatterTask, const L3BJobContext &jobCtx, const QList<TileInfos> &prdTilesInfosList, const QList<TileResultFiles> &tileResultFilesList);
    NewStepList GetStepsForMonodateLai(const L3BJobContext &jobCtx,
                                       const QList<TileInfos> &prdTilesList, QList<TaskToSubmit> &allTasksList, int tasksStartIdx);
    int GetStepsForStatusFlags(const L3BJobContext &jobCtx, QList<TaskToSubmit> &allTasksList, int curTaskIdx,
                                TileResultFiles &tileResultFileInfo, NewStepList &steps, QStringList &cleanupTemporaryFilesList);
    int GetStepsForNdvi(const L3BJobContext &jobCtx, QList<TaskToSubmit> &allTasksList, int curTaskIdx,
                                TileResultFiles &tileResultFileInfo, NewStepList &steps, QStringList &cleanupTemporaryFilesList);
    int GetStepsForAnglesCreation(QList<TaskToSubmit> &allTasksList, int curTaskIdx, TileResultFiles &tileResultFileInfo, NewStepList &steps, QStringList &cleanupTemporaryFilesList);
    int GetStepsForMonoDateBI(const L3BJobContext &jobCtx, QList<TaskToSubmit> &allTasksList,
                               const QString &indexName, int curTaskIdx, TileResultFiles &tileResultFileInfo,
                              NewStepList &steps, QStringList &cleanupTemporaryFilesList);
    int GetStepsForInDomainFlags(const L3BJobContext &jobCtx, QList<TaskToSubmit> &allTasksList, int curTaskIdx, TileResultFiles &tileResultFileInfo, NewStepList &steps,
                                QStringList &cleanupTemporaryFilesList);

    const QString& GetDefaultCfgVal(std::map<QString, QString> &configParameters, const QString &key, const QString &defVal);

    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;
    QSet<QString> GetTilesFilter(const L3BJobContext &jobCtx);
    bool FilterTile(const QSet<QString> &tilesSet, const ProductDetails &prdDetails);
    void InitTileResultFiles(const TileInfos &tileInfo, TileResultFiles &tileResultFileInfo);

    void HandleProduct(const L3BJobContext &jobCtx, const QList<TileInfos> &prdTilesList, QList<TaskToSubmit> &allTasksList);
    void SubmitEndOfLaiTask(EventProcessingContext &ctx, const JobSubmittedEvent &event,
                            const QList<TaskToSubmit> &allTasksList);

private:
    int UpdateJobSubmittedParamsFromSchedReq(const L3BJobContext &jobCtx, ProductList &prdsToProcess);
    ProductList GetL2AProductsNotProcessedProductProvenance(const L3BJobContext &jobCtx,
                                                            const QDateTime &startDate, const QDateTime &endDate);
    QString GetSiteCurrentProcessingPrdsFile(EventProcessingContext &ctx, int jobId, int siteId);

    friend class LaiRetrievalHandler;
    friend class L3BJobContext;
};

#endif // LAIRETRIEVALHANDLERL3BNEW_HPP

