#pragma once

#include "processorhandler.hpp"
#include "optional.hpp"

#define S4S_PERM_CROPS_CFG_PREFIX "processor.s4s_perm_crop."

class S4SPermanentCropHandler : public ProcessorHandler
{
    typedef struct S4SPermanentCropJobConfig {
        S4SPermanentCropJobConfig(EventProcessingContext *pContext, const JobSubmittedEvent &evt)
            : event(evt) {
            pCtx = pContext;
            siteShortName = pContext->GetSiteShortName(evt.siteId);
            configParameters = pCtx->GetJobConfigurationParameters(evt.jobId, S4S_PERM_CROPS_CFG_PREFIX);
            parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
            startDate = ProcessorHandlerHelper::GetDateTimeFromString(
                        ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "start_date", S4S_PERM_CROPS_CFG_PREFIX));
            endDate = ProcessorHandlerHelper::GetDateTimeFromString(
                        ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "end_date", S4S_PERM_CROPS_CFG_PREFIX));

            if (!startDate.isValid() || !endDate.isValid()) {
                filterProductNames = ProcessorHandler::GetInputProductNames(parameters);
                const ProductList &prds = ProcessorHandler::GetInputProducts(*pCtx, parameters, configParameters, event.siteId,
                                                                             ProductType::L2AProductTypeId, S4S_PERM_CROPS_CFG_PREFIX);
                const QList<ProductDetails> &productDetails = ProcessorHandlerHelper::GetProductDetails(prds, *pCtx);
                bool ret = ProcessorHandlerHelper::GetIntevalFromProducts(prds, startDate, endDate);
                if (!ret || productDetails.size() == 0) {
                    // try to get the start and end date if they are given
                    pCtx->MarkJobFailed(event.jobId);
                    throw std::runtime_error(
                        QStringLiteral(
                            "PermanentCrops: No products provided at input or no products available in the specified interval")
                            .toStdString());
                }

                // get tile ids from the selected products
                const TilesTimeSeries &mapTiles = ProcessorHandler::GroupL2ATiles(*pCtx, productDetails);
                // normally, we can use only one list by we want (not necessary) to have the
                // secondary satellite tiles after the main satellite tiles
                for (const auto &tileId : mapTiles.GetTileIds()) {
                    tileIds.append(tileId);
                }
            } else {
                const TileList &tiles = pContext->GetSiteTiles(event.siteId, (int)Satellite::Sentinel2);
                if (tiles.size() == 0) {
                    pCtx->MarkJobFailed(event.jobId);
                    throw std::runtime_error(
                        QStringLiteral("PermanentCrops: No tiles defined for site with id = %1").arg(evt.siteId).toStdString());
                }
                std::transform(tiles.cbegin(), tiles.cend(), std::back_inserter(tileIds), [](const Tile & tile) {return tile.tileId ; } );
            }
            year = endDate.date().year();           // TODO: see if this is valid
            // change to the beginning of the next day to avoid losing products that are in the same date as the end date
            // We update this after the year extraction as adding 1 day might move to the next year
            endDate = endDate.addDays(1);
        }
//        void SetSamplesInfosProducts(const QString &sampleFile) {
//            samplesShapePath = sampleFile;
//        }

        EventProcessingContext *pCtx;
        JobSubmittedEvent event;

        QString siteShortName;
        QDateTime startDate;
        QDateTime endDate;
        QStringList tileIds;
        QStringList filterProductNames;

        std::map<QString, QString> configParameters;
        QJsonObject parameters;
        int year;
        // QString samplesShapePath;

    } S4SPermanentCropJobConfig;

private:
    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &event) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;

    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;
    QList<std::reference_wrapper<TaskToSubmit>> CreateTasks(QList<TaskToSubmit> &outAllTasksList, const S4SPermanentCropJobConfig &cfg);
    NewStepList CreateSteps(QList<TaskToSubmit> &allTasksList,
                            const S4SPermanentCropJobConfig &cfg);

    QStringList GetExtractInputsTaskArgs(const S4SPermanentCropJobConfig &cfg, const QString &outFile, const QString &tileId);
    QStringList GetExtractParcelsTaskArgs(int siteId, int year, const QString &outFile);
    QStringList GetBuildVrtTaskArgs(const QString &inputsListFile, const QString &fullStackVrtPath, const QString &workingDir);
    QStringList GetBuildFullStackTifTaskArgs(const QString &inputFilesListPath, const QString &fullStackTifPath, const QString &workingDir);
    QStringList GetPolygonClassStatisticsTaskArgs(const QString &image, const QString &samples, const QString &fieldName, const QString &sampleStats);
    QStringList GetSampleSelectionTaskArgs(const QString &image, const QString &samples, const QString &fieldName, const QString &sampleStats, const QString &outRates, const QString &selectedUpdateSamples);
    QStringList GetSampleExtractionTaskArgs(const QString &image, const QString &fullStackTifPath, const QString &fieldName, const QString &finalUpdateSamples);
    QStringList GetSamplesRasterizationTaskArgs(const QString &reflStackTif, const QString &fullStackVrtPath, const QString &fieldName, int valToReplace, int replacingValue, const QString &outputFile);
    QStringList GetBroceliandeTaskArgs(const S4SPermanentCropJobConfig &cfg, const TaskToSubmit &task, const QString &fullStackVrtPath, const QString &samples, const QString &output);
    QStringList GetCropInfosExtractionTaskArgs(const QStringList &imgs, const QString &exp, const QString &out);
    QStringList GetPostProcessingTaskArgs(const QString &input, const QString &output);
    QStringList GetCropSieveTaskArgs(const QString &annualCrop, const QString &annualSieve);

    // QString ExtractSamplesInfos(const S4SPermanentCropJobConfig &cfg);
    QStringList GetTileIdsFromProducts(EventProcessingContext &ctx, const QList<ProductDetails> &productDetails);
    bool IsScheduledJobRequest(const QJsonObject &parameters);
    QStringList GetProductFormatterArgs(TaskToSubmit &productFormatterTask, const S4SPermanentCropJobConfig &cfg,
                                        const QStringList &listFiles);
};

