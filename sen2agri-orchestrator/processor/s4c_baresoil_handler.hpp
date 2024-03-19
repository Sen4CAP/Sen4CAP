#pragma once

#include "processorhandler.hpp"
#include "optional.hpp"
#include "s4c_mdb1_dataextract_steps_builder.hpp"
#include "products/generichighlevelproducthelper.h"

#define S4C_BARE_SOIL_CFG_PREFIX "processor.s4c_bare_soil."

class S4CBareSoilHandler : public ProcessorHandler
{
    typedef struct S4CBareSoilJobConfig {
        S4CBareSoilJobConfig(EventProcessingContext *pContext, const JobSubmittedEvent &evt)
            : event(evt), isScheduled(false) {
            pCtx = pContext;
            siteShortName = pContext->GetSiteShortName(evt.siteId);
            configParameters = pCtx->GetJobConfigurationParameters(evt.jobId, S4C_BARE_SOIL_CFG_PREFIX);
            parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();

            startDate = ProcessorHandlerHelper::GetDateTimeFromString(
                        ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "start_date", S4C_BARE_SOIL_CFG_PREFIX));
            endDate = ProcessorHandlerHelper::GetDateTimeFromString(
                        ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "end_date", S4C_BARE_SOIL_CFG_PREFIX));

            year = endDate.date().year();           // TODO: see if this is valid
            // change to the beginning of the next day to avoid losing products that are in the same date as the end date
            // We update this after the year extraction as adding 1 day might move to the next year
            endDate = endDate.addDays(1);

            const TileList &tiles = pCtx->GetSiteTiles(event.siteId, (int)Satellite::Sentinel2);
            if (tiles.size() == 0) {
                pCtx->MarkJobFailed(event.jobId);
                throw std::runtime_error(
                    QStringLiteral("Bare Soil: No tiles defined for site with id = %1").arg(evt.siteId).toStdString());
            }
            std::transform(tiles.cbegin(), tiles.cend(), std::back_inserter(siteTiles), [](const Tile & tile) {return tile.tileId ; } );

            lpisPath = ExtractLpisPath(year);

            const ProductList &mdb1PrdsList = pCtx->GetProducts(event.siteId, (int)ProductType::S4MDB1ProductTypeId,
                                                                               startDate, endDate);
            if (mdb1PrdsList.size() == 0) {
                pCtx->MarkJobFailed(event.jobId);
                throw std::runtime_error(QStringLiteral("Bare Soil: No MDB1 products were found in database for site %1 and interval %2 - %3.")
                                         .arg(siteShortName)
                                         .arg(startDate.toString())
                                         .arg(endDate.toString()).toStdString());
            }
            const ProductList &mdbL4APrdsList = pCtx->GetProducts(event.siteId, (int)ProductType::S4MDBL4ASarMainProductTypeId,
                                                                               startDate, endDate);
            if (mdbL4APrdsList.size() == 0) {
                pCtx->MarkJobFailed(event.jobId);
                throw std::runtime_error(QStringLiteral("Bare Soil: No MDB L4A Sar Main products were found in database for site %1 and interval %2 - %3.")
                                         .arg(siteShortName)
                                         .arg(startDate.toString())
                                         .arg(endDate.toString()).toStdString());
            }
            mdb1PrdPath = mdb1PrdsList.at(mdb1PrdsList.size()-1).fullPath;
            mdbL4SarMainPrdPath = mdbL4APrdsList.at(mdbL4APrdsList.size()-1).fullPath;

            calibBSNdviThr = ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, "calib_bs_ndvi_thr", S4C_BARE_SOIL_CFG_PREFIX, 0.15);
            calibNBSNdviThr = ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, "calib_nbs_ndvi_thr", S4C_BARE_SOIL_CFG_PREFIX, 0.45);

            calibBSNdwiThr = ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, "calib_bs_ndwi_thr", S4C_BARE_SOIL_CFG_PREFIX, 0.);
            calibNBSNdwiThr = ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, "calib_nbs_ndwi_thr", S4C_BARE_SOIL_CFG_PREFIX, 0.3);

            calibBSNdtiThr = ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, "calib_bs_ndti_thr", S4C_BARE_SOIL_CFG_PREFIX, 0.1);
            calibNBSNdtiThr = ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, "calib_nbs_ndti_thr", S4C_BARE_SOIL_CFG_PREFIX, 0.25);

            calibNBSFcoverThr = ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, "calib_nbs_fcover_thr", S4C_BARE_SOIL_CFG_PREFIX, 0.01);

            modelEstimatorsNo = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "model_estimator_no", S4C_BARE_SOIL_CFG_PREFIX, 30);

            markersLongPeriod = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "markers_long_period", S4C_BARE_SOIL_CFG_PREFIX, 60);
            markersShortPeriod = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "markers_short_period", S4C_BARE_SOIL_CFG_PREFIX, 30);
            markersS2PeriodsNo = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "markers_s2_periods_no", S4C_BARE_SOIL_CFG_PREFIX, 3);
            markersS1PeriodsNo = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "markers_s1_periods_no", S4C_BARE_SOIL_CFG_PREFIX, 4);

            markersS2BSThreshold = ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, "markers_bs_s2_threshold", S4C_BARE_SOIL_CFG_PREFIX, 0.75);
            markersS2NBSThreshold = ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, "markers_nbs_s2_threshold", S4C_BARE_SOIL_CFG_PREFIX, 0.8);
            markersS1BSThreshold = ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, "markers_bs_s1_threshold", S4C_BARE_SOIL_CFG_PREFIX, 0.65);
            markersS1NBSThreshold = ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, "markers_nbs_s1_threshold", S4C_BARE_SOIL_CFG_PREFIX, 0.7);
        }

        QString ExtractLpisPath(int year) {
            // We take it the last LPIS product for this site.
            const ProductList &lpisPrds = S4CUtils::GetLpisProduct(pCtx, event.siteId);
            if (lpisPrds.size() == 0) {
                pCtx->MarkJobFailed(event.jobId);
                throw std::runtime_error(QStringLiteral("No LPIS product found in database for the MDB1 execution for site %1.").
                                         arg(siteShortName).toStdString());
            }

            Product retLpisPrd;
            for(const Product &lpisPrd: lpisPrds) {
                if (lpisPrd.created.date().year() == year) {
                    if (!retLpisPrd.created.isValid() || (lpisPrd.inserted > retLpisPrd.inserted)) {
                        retLpisPrd = lpisPrd;
                    }
                }
            }
            if (retLpisPrd.fullPath.size() == 0) {
                pCtx->MarkJobFailed(event.jobId);
                throw std::runtime_error(QStringLiteral("Bare Soil: No LPIS product was found in database for site %1 and year %2.")
                                         .arg(siteShortName)
                                         .arg(year).toStdString());
            }

            return retLpisPrd.fullPath;
        }

        EventProcessingContext *pCtx;
        JobSubmittedEvent event;

        QString siteShortName;
        QDateTime startDate;
        QDateTime endDate;
        QStringList siteTiles;
        QString mdb1PrdPath;
        QString mdbL4SarMainPrdPath;
        QString lpisPath;

        float calibBSNdviThr;
        float calibNBSNdviThr;

        float calibBSNdwiThr;
        float calibNBSNdwiThr;

        float calibBSNdtiThr;
        float calibNBSNdtiThr;

        float calibNBSFcoverThr;

        int modelEstimatorsNo;

        int markersLongPeriod;
        int markersShortPeriod;
        int markersS2PeriodsNo;
        int markersS1PeriodsNo;
        float markersS2BSThreshold;
        float markersS2NBSThreshold;
        float markersS1BSThreshold;
        float markersS1NBSThreshold;

        std::map<QString, QString> configParameters;
        QJsonObject parameters;
        bool isScheduled;
        int year;

    } S4CBareSoilJobConfig;

private:
    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &event) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;

    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;
    QList<std::reference_wrapper<TaskToSubmit>> CreateTasks(const S4CBareSoilJobConfig &cfg, QList<TaskToSubmit> &outAllTasksList);
    NewStepList CreateSteps(QList<TaskToSubmit> &allTasksList,
                            const S4CBareSoilJobConfig &cfg);
    QStringList GetS2CalibrationTaskArgs(const S4CBareSoilJobConfig &cfg, const QString &s2CalibPath);
    QStringList GetS1CalibrationTaskArgs(const S4CBareSoilJobConfig &cfg, const QString &s2CalibPath, const QString &s1CalibPath);
    QStringList GetS2ModelTaskArgs(const S4CBareSoilJobConfig &cfg, const QString &s2CalibPath, const QString &s2Results, const QString &outputModel, const QString &figImportancePath);
    QStringList GetS1ModelTaskArgs(const S4CBareSoilJobConfig &cfg, const QString &s1CalibPath, const QString &s1Results, const QString &outputModel, const QString &figImportancePath);
    QStringList GetMarkersTaskArgs(const S4CBareSoilJobConfig &cfg, const QString &s2Results, const QString &s1Results, const QString &outputS2Markers,
                                   const QString &outputS1Markers, const QString &outputAllMarkers);
    QStringList GetProductFormatterArgs(TaskToSubmit &productFormatterTask, const S4CBareSoilJobConfig &cfg, const QStringList &listFiles);
};

