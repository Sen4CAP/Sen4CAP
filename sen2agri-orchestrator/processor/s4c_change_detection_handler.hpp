#pragma once

#include "processorhandler.hpp"
#include "optional.hpp"
#include "s4c_mdb1_dataextract_steps_builder.hpp"
#include "products/generichighlevelproducthelper.h"
#include "products/lpisinfosextractor.h"

#define S4C_CHANGE_DETECTION_CFG_PREFIX "processor.s4c_change_detection."

class S4CBareSoilStepsBuilder;

//processor.s4c_change_detection.reference_site
//processor.s4c_change_detection.ref_start_date
//processor.s4c_change_detection.ref_end_date

class S4CChangeDetectionHandler : public ProcessorHandler
{
    typedef struct SiteConfig {
        SiteConfig() {}
        void initialize(EventProcessingContext *pContext, const std::map<QString, QString> &configParameters, const QJsonObject &parameters,
                   int siteId, int jobId, const QString &siteCfgKeyPrefix = "") {

            bool isRef = siteCfgKeyPrefix.length() > 0;

            this->siteId = siteId;
            siteShortName = pContext->GetSiteShortName(siteId);

            const QString &startDateCfg = siteCfgKeyPrefix + "start_date";
            const QString &endDateCfg = siteCfgKeyPrefix + "end_date";
            startDate = ProcessorHandlerHelper::GetDateTimeFromString(
                        ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, startDateCfg, S4C_CHANGE_DETECTION_CFG_PREFIX));
            endDate = ProcessorHandlerHelper::GetDateTimeFromString(
                        ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, endDateCfg, S4C_CHANGE_DETECTION_CFG_PREFIX));
            year = endDate.date().year();           // TODO: see if this is valid
            // change to the beginning of the next day to avoid losing products that are in the same date as the end date
            // We update this after the year extraction as adding 1 day might move to the next year
            endDate = endDate.addDays(1);
            const TileList &tiles = pContext->GetSiteTiles(siteId, (int)Satellite::Sentinel2);
            if (tiles.size() == 0) {
                pContext->MarkJobFailed(jobId);
                throw std::runtime_error(
                    QStringLiteral("Change Detection: No tiles defined for site with id = %1").arg(siteId).toStdString());
            }
            std::transform(tiles.cbegin(), tiles.cend(), std::back_inserter(siteTiles), [](const Tile & tile) {return tile.tileId ; } );

            lpisCsvPath = ExtractLpisPath(pContext, siteId, jobId, year);

            const ProductList &mdb1PrdsList = pContext->GetProducts(siteId, (int)ProductType::S4MDB1ProductTypeId,
                                                                               startDate, endDate);
            if (mdb1PrdsList.size() == 0) {
                pContext->MarkJobFailed(jobId);
                throw std::runtime_error(QStringLiteral("Change Detection: No MDB1 products were found in database for site %1 and interval %2 - %3.")
                                         .arg(siteShortName)
                                         .arg(startDate.toString())
                                         .arg(endDate.toString()).toStdString());
            }
            const ProductList &mdbL4APrdsList = pContext->GetProducts(siteId, (int)ProductType::S4MDBL4AOptMainProductTypeId,
                                                                               startDate, endDate);
            if (mdbL4APrdsList.size() == 0) {
                pContext->MarkJobFailed(jobId);
                throw std::runtime_error(QStringLiteral("Change Detection: No MDB L4A OPT Main products were found in database for site %1 and interval %2 - %3.")
                                         .arg(siteShortName)
                                         .arg(startDate.toString())
                                         .arg(endDate.toString()).toStdString());
            }
            const ProductList &bsMarkersPrdsList = pContext->GetProducts(siteId, (int)ProductType::S4CBareSoilProductTypeId,
                                                                               startDate, endDate);
            if (bsMarkersPrdsList.size() == 0) {
                pContext->MarkJobFailed(jobId);
                throw std::runtime_error(QStringLiteral("Change Detection: No Bare Soil products were found in database for site %1 and interval %2 - %3.")
                                         .arg(siteShortName)
                                         .arg(startDate.toString())
                                         .arg(endDate.toString()).toStdString());
            }

            mdb1PrdPath = mdb1PrdsList.at(mdb1PrdsList.size()-1).fullPath;
            mdbL4OptMainPrdPath = mdbL4APrdsList.at(mdbL4APrdsList.size()-1).fullPath;
            bareSoilPrdPath = bsMarkersPrdsList.at(bsMarkersPrdsList.size()-1).fullPath;

            // check also for mapping files
            mdb1IdsMappingFile = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters,
                                                                              siteCfgKeyPrefix + "mdb1_ids_mapping", S4C_CHANGE_DETECTION_CFG_PREFIX);
            bsIdsMappingFile = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters,
                                                                              siteCfgKeyPrefix + "bs_ids_mapping", S4C_CHANGE_DETECTION_CFG_PREFIX);

            grassland_ttdayss2_thr	=	        ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "grassland_ttdayss2_thr", S4C_CHANGE_DETECTION_CFG_PREFIX, 0);
            grassland_ttdayss2_incr	=	        ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "grassland_ttdayss2_incr", S4C_CHANGE_DETECTION_CFG_PREFIX, 2);
            grassland_ratiostab_min_thr	=	    ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "grassland_ratiostab_min_thr", S4C_CHANGE_DETECTION_CFG_PREFIX, 0);
            grassland_ratiostab_max_thr	=	    ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "grassland_ratiostab_max_thr", S4C_CHANGE_DETECTION_CFG_PREFIX, isRef ? 50 : 25);
            grassland_ratiostab_min_incr	=	ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "grassland_ratiostab_min_incr", S4C_CHANGE_DETECTION_CFG_PREFIX, 1);
            grassland_ratiostab_max_incr	=	ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "grassland_ratiostab_max_incr", S4C_CHANGE_DETECTION_CFG_PREFIX, 1.5);
            grassland_consecstab_thr	=	    ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "grassland_consecstab_thr", S4C_CHANGE_DETECTION_CFG_PREFIX, isRef ? 0 : 1);
            grassland_consecstab_incr	=	    ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "grassland_consecstab_incr", S4C_CHANGE_DETECTION_CFG_PREFIX, 1);
            permcrops_ttdayss2_thr	=	        ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "permcrops_ttdayss2_thr", S4C_CHANGE_DETECTION_CFG_PREFIX, 0);
            permcrops_ttdayss2_incr	=	        ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "permcrops_ttdayss2_incr", S4C_CHANGE_DETECTION_CFG_PREFIX, 3);
            permcrops_areaveg_thr	=	        ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "permcrops_areaveg_thr", S4C_CHANGE_DETECTION_CFG_PREFIX, isRef ? 50 : 25);
            permcrops_areaveg_incr	=	        ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "permcrops_areaveg_incr", S4C_CHANGE_DETECTION_CFG_PREFIX, 1);
            permcrops_ratiostab_thr	=	        ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "permcrops_ratiostab_thr", S4C_CHANGE_DETECTION_CFG_PREFIX, 20);
            permcrops_ratiostab_incr	=	    ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "permcrops_ratiostab_incr", S4C_CHANGE_DETECTION_CFG_PREFIX, 1);
            arableland_ttdayss2_thr	=	        ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "arableland_ttdayss2_thr", S4C_CHANGE_DETECTION_CFG_PREFIX, 0);
            arableland_ttdayss2_incr	=	    ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, siteCfgKeyPrefix + "arableland_ttdayss2_incr", S4C_CHANGE_DETECTION_CFG_PREFIX, 1);

        }
        QString ExtractLpisPath(EventProcessingContext *pCtx, int siteId, int jobId, int year) {
            // We take it the last LPIS product for this site.
            const ProductList &lpisPrds = S4CUtils::GetLpisProduct(pCtx, siteId);
            if (lpisPrds.size() == 0) {
                pCtx->MarkJobFailed(jobId);
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
                pCtx->MarkJobFailed(jobId);
                throw std::runtime_error(QStringLiteral("Change Detection: No LPIS product was found in database for site %1 and year %2.")
                                         .arg(siteShortName)
                                         .arg(year).toStdString());
            }

            orchestrator::products::LpisInfosExtractor extractor;
            extractor.SetLpisProducts({retLpisPrd});
            const QMap<int, orchestrator::products::LpisInfos> &allLpisInfos = extractor.GetLpisInfos();
            // Maybe we found some LPIS in database but we want to be sure they are also on disk and with the expected structure
            QMap<int, orchestrator::products::LpisInfos>::const_iterator i = allLpisInfos.find(year);
            if (i == allLpisInfos.end() || i.value().csvPath.length() == 0) {
                pCtx->MarkJobFailed(jobId);
                throw std::runtime_error(QStringLiteral("No LPIS product found in database for the Change Detection execution for site %1 and year %2")
                                         .arg(siteShortName)
                                         .arg(year).toStdString());
            }
            return i.value().csvPath;
        }

        int siteId;
        QString siteShortName;
        QDateTime startDate;
        QDateTime endDate;
        QStringList siteTiles;
        QString mdb1PrdPath;
        QString mdbL4OptMainPrdPath;
        QString bareSoilPrdPath;
        QString lpisCsvPath;
        int year;
        QString mdb1IdsMappingFile;
        QString bsIdsMappingFile;

        float grassland_ttdayss2_thr;
        float grassland_ttdayss2_incr;
        float grassland_ratiostab_min_thr;
        float grassland_ratiostab_max_thr;
        float grassland_ratiostab_min_incr;
        float grassland_ratiostab_max_incr;
        float grassland_consecstab_thr;
        float grassland_consecstab_incr;
        float permcrops_ttdayss2_thr;
        float permcrops_ttdayss2_incr;
        float permcrops_areaveg_thr;
        float permcrops_areaveg_incr;
        float permcrops_ratiostab_thr;
        float permcrops_ratiostab_incr;
        float arableland_ttdayss2_thr;
        float arableland_ttdayss2_incr;

    } SiteConfig;

    typedef struct S4CChangeDetectionJobConfig {
        S4CChangeDetectionJobConfig(EventProcessingContext *pContext, const JobSubmittedEvent &evt) :
            pCtx(pContext), event(evt)
        {
            const std::map<QString, QString> &configParameters = pContext->GetJobConfigurationParameters(evt.jobId, S4C_CHANGE_DETECTION_CFG_PREFIX);
            const QJsonObject &parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
            int refSiteId = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "ref_site_id", S4C_CHANGE_DETECTION_CFG_PREFIX, -1);
            if (refSiteId == -1) {
                pContext->MarkJobFailed(evt.jobId);
                throw std::runtime_error(QStringLiteral("Change Detection: The reference site id for site %1.")
                                         .arg( pContext->GetSiteShortName(evt.siteId)).toStdString());
            }

            refSiteCfg.initialize(pContext, configParameters, parameters, refSiteId, evt.jobId, "ref_");
            currentSiteCfg.initialize(pContext, configParameters, parameters, evt.siteId, evt.jobId);

            sitesIdsMappingFile = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters,
                                                                              "sites_ids_mapping", S4C_CHANGE_DETECTION_CFG_PREFIX);

            refIdsMappingColName = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters,
                                                                              "ref_ids_mapping_col_name", S4C_CHANGE_DETECTION_CFG_PREFIX);
            if (refIdsMappingColName.length() == 0) {
                refIdsMappingColName = "NewID_ref";
            }
        }

        EventProcessingContext *pCtx;
        JobSubmittedEvent event;
        SiteConfig refSiteCfg;
        SiteConfig currentSiteCfg;
        QString refIdsMappingColName;
        QString sitesIdsMappingFile;    // TODO: This should be removed

    } S4CChangeDetectionJobConfig;

private:
    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &event) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;

    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;
    QList<std::reference_wrapper<TaskToSubmit>> CreateTasks(const S4CChangeDetectionJobConfig &cfg, QList<TaskToSubmit> &outAllTasksList);
    NewStepList CreateSteps(QList<TaskToSubmit> &allTasksList,
                            const S4CChangeDetectionJobConfig &cfg, NewStepList &allSteps);

    QStringList GeCommonParcelsExtractionTaskArgs(const S4CChangeDetectionJobConfig &cfg, const QString &outPath);
    QStringList GetLpisFilteringTaskArgs(const S4CChangeDetectionJobConfig &cfg, bool isRefSite, const QString &filteringIdsFile, const QString &outPath);
    QStringList GetLaiOutliersTaskArgs(const S4CChangeDetectionJobConfig &cfg, bool isRefSite, const QString &lpisCsv, const QString &outFile);
    QStringList GetVegGrowthMarkersTaskArgs(const S4CChangeDetectionJobConfig &cfg, bool isRefSite, const QString &lpisCsv, const QString &out);
    QStringList GetBSFilteredMarkersTaskArgs(const S4CChangeDetectionJobConfig &cfg, bool isRefSite, const QString &lpisCsv, const QString &out);
    QStringList GetChangeDetectionComputationTaskArgs(const S4CChangeDetectionJobConfig &cfg, const QString &lpisCsv, bool isRefSite,
                                                      const QString &refLpisFilteredPath, const QString &laiOutliers,
                                                      const QString &vegGrowthMarkers, const QString &bsMarkers,
                                                      const QString &idsMappingFile, const QString &out);
    QStringList GetChangeDetectionConsolidationTaskArgs(const S4CChangeDetectionJobConfig &cfg, const QString &changeDetRef,
                                                        const QString &changeDetCurrent, const QString &idsMappingFile, const QString &out);
    QStringList GetProductFormatterArgs(TaskToSubmit &productFormatterTask, const S4CChangeDetectionJobConfig &cfg, const QStringList &listFiles);
};

