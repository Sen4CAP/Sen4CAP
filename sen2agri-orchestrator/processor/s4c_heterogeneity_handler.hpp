#pragma once

#include "processorhandler.hpp"
#include "optional.hpp"
#include "products/generichighlevelproducthelper.h"
#include "products/lpisinfosextractor.h"

#define S4C_HETEROGENEITY_CFG_PREFIX "processor.s4c_heterog."

class S4CHeterogeneityHandler : public ProcessorHandler
{
    typedef struct TileInfoMaps
    {
        // Band name to raster L2A/NDVI files
        QMap<QString, QStringList> mapRasters;
        // Band name to mask L2A/NDVI files
        QMap<QString, QStringList> mapMasks;
        // Band name to dates
        QMap<QString, QList<int>> mapDates;

    } TileInfoMaps;

    typedef struct S4CHeterogneneityJobConfig {
        S4CHeterogneneityJobConfig(EventProcessingContext *pContext, const JobSubmittedEvent &evt)
            : event(evt) {
            pCtx = pContext;
            siteShortName = pContext->GetSiteShortName(evt.siteId);
            configParameters = pCtx->GetJobConfigurationParameters(evt.jobId, S4C_HETEROGENEITY_CFG_PREFIX);
            parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();

            startDate = ProcessorHandlerHelper::GetDateTimeFromString(
                        ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters,
                                                                     "start_date", S4C_HETEROGENEITY_CFG_PREFIX));
            endDate = ProcessorHandlerHelper::GetDateTimeFromString(
                        ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters,
                                                                     "end_date", S4C_HETEROGENEITY_CFG_PREFIX));

            year = endDate.date().year();       // TODO: see if this is valid or another method should be used

            clusteringPeriod = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "clustering_period", S4C_HETEROGENEITY_CFG_PREFIX, 30);
            if(clusteringPeriod < 20) {
                clusteringPeriod = 20;
            }

            // get the interval from the first day of the first month until the end of the month of the last month
            int days = startDate.date().addDays(0 - startDate.date().day())
                    .daysTo(endDate.date().addDays(endDate.date().daysInMonth() - endDate.date().day()));
            // add an additional cluster if remaining days in month - TBC
            int addCluster = (((days % clusteringPeriod) > 20) ? 1 : 0);
            clusteringIntervals = ((days / clusteringPeriod) + addCluster);

            s2MaxDist = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "temporal_resampling_max_dist", S4C_HETEROGENEITY_CFG_PREFIX, 30);
            winRadius = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "temporal_resampling_windows_radius", S4C_HETEROGENEITY_CFG_PREFIX, 15);

            maskValue = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "mask_value", S4C_HETEROGENEITY_CFG_PREFIX, 0);
            nanValue = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "nan_value", S4C_HETEROGENEITY_CFG_PREFIX, -10000);

            s1TemporalResamplingInterval = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "s1_temporal_resampling_interval", S4C_HETEROGENEITY_CFG_PREFIX, 7);
            s2TemporalResamplingInterval = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "s2_temporal_resampling_interval", S4C_HETEROGENEITY_CFG_PREFIX, 10);
            if (s1TemporalResamplingInterval <= 0) {
                s1TemporalResamplingInterval = 7;
            }
            if (s2TemporalResamplingInterval <= 0) {
                s2TemporalResamplingInterval = 10;
            }

            s1NumImages = clusteringPeriod / s1TemporalResamplingInterval;
            s2NumImages = clusteringPeriod / s2TemporalResamplingInterval;

            // n_cl parameters
            s1ClustersNo = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "s1_clusters_number", S4C_HETEROGENEITY_CFG_PREFIX, 5);
            s2ClustersNo = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "s2_clusters_number", S4C_HETEROGENEITY_CFG_PREFIX, 4);

            // Spatial smoothing parameters
            isolatedPixelsThr = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "isolated_pixels_thr", S4C_HETEROGENEITY_CFG_PREFIX, 1);
            smoothingRadius = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "isolated_pixels_smoothing_radius", S4C_HETEROGENEITY_CFG_PREFIX, 1);

            // Spatial connectivity parameters
            searchRadiusS1 = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "search_radius_s1", S4C_HETEROGENEITY_CFG_PREFIX, 1);
            searchRadiusS2 = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "search_radius_s2", S4C_HETEROGENEITY_CFG_PREFIX, 1);
            fullConnectivity = ProcessorHandlerHelper::GetBoolConfigValue(parameters, configParameters, "full_connectivity", S4C_HETEROGENEITY_CFG_PREFIX, false);

            // Cluster analysis parameters
            clustPixNumS1 = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "s1_min_cluster_pixels", S4C_HETEROGENEITY_CFG_PREFIX, 20);
            clustPixNumS2 = ProcessorHandlerHelper::GetIntConfigValue(parameters, configParameters, "s2_min_cluster_pixels", S4C_HETEROGENEITY_CFG_PREFIX, 20);

            ndviThrDist = ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, "ndvi_clust_dist_thr", S4C_HETEROGENEITY_CFG_PREFIX, 0.17);
            compactnessThrS1 = 2*searchRadiusS1 + 1;
            compactnessThrS2 = 2*searchRadiusS2 + 1;
            percentHetero = ProcessorHandlerHelper::GetFloatConfigValue(parameters, configParameters, "percentage_hererogeneity", S4C_HETEROGENEITY_CFG_PREFIX, 0.9);

            const ProductList &s2Products = GetInputProducts(*pContext, parameters, configParameters, event.siteId,
                                          ProductType::MaskedL2AProductTypeId, S4C_HETEROGENEITY_CFG_PREFIX,
                                          &startDate, &endDate);
            l2aProductDetails = ProcessorHandlerHelper::GetProductDetails(s2Products, *pCtx);
            FilterOpticalProductDetails();
            if (l2aProductDetails.size() == 0) {
                pCtx->MarkJobFailed(event.jobId);
                throw std::runtime_error(QStringLiteral("No Masked L2A input products found in database for the Heterogeneity "
                                                        "execution for site %1 and startDate = %2 and endDate = %3.")
                                         .arg(siteShortName)
                                         .arg(startDate.toString())
                                         .arg(startDate.toString())
                                         .toStdString());
            }

            const ProductList &l3bProducts = GetInputProducts(*pContext, parameters, configParameters, event.siteId,
                                          ProductType::L3BProductTypeId, S4C_HETEROGENEITY_CFG_PREFIX);
            l3bProductDetails = ProcessorHandlerHelper::GetProductDetails(l3bProducts, *pCtx);
            if (l3bProductDetails.size() == 0) {
                pCtx->MarkJobFailed(event.jobId);
                throw std::runtime_error(QStringLiteral("No L3B input products found in database for the Heterogeneity "
                                                        "execution for site %1 and startDate = %2 and endDate = %3.")
                                         .arg(siteShortName)
                                         .arg(startDate.toString())
                                         .arg(startDate.toString())
                                         .toStdString());
            }
            existingCTSARDir = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "existing_ct_dir", S4C_HETEROGENEITY_CFG_PREFIX);

            UpdateOpticalPrdsTileBandMaps();
            UpdateLpisInfos();
        }

        void UpdateOpticalPrdsTileBandMaps();
        void UpdateLpisInfos();
        void FilterOpticalProductDetails();

        int GetNoOfImagesInPeriod(Satellite sat) const;
        int GetNoOfClusters(Satellite sat) const;

        EventProcessingContext *pCtx;
        JobSubmittedEvent event;

        QString siteShortName;
        QDateTime startDate;
        QDateTime endDate;

        QList<ProductDetails> l2aProductDetails;
        QList<ProductDetails> l3bProductDetails;
        QMap<QString, TileInfoMaps> tileInfos;
        QStringList outDates;
        QString existingCTSARDir;

        std::map<QString, QString> configParameters;
        QJsonObject parameters;
        int year;
        orchestrator::products::LpisInfos lpisInfos;

        double s2MaxDist;    // S2 temporal resampling max dist
        double winRadius;    // S2 temporal resampling window radius
        int    maskValue;    // S2 temporal resampling mask value
        int    nanValue;     // S2 temporal resampling NaN value
        int    s1TemporalResamplingInterval;     // S1 temporal resampling interval, default 7 days
        int    s2TemporalResamplingInterval;     // S2 temporal resampling interval, default 10 days

        // processor specific parameters
        // Number of days for the periods P (clustering period). Default 30 days and minimum 20 days
        int clusteringPeriod;
        int clusteringIntervals;

        // Spatial smoothing parameters
        int smoothingRadius;

        // Spatial connectivity parameters
        // RadiusC - Radius of search (window of 3x3 pixels when radius = 1)
        bool fullConnectivity;
        int searchRadiusS1;
        int searchRadiusS2;

        // Isolated pixels removal parameters
        int isolatedPixelsThr;
        int noDataValue;

        // Cluster analysis parameters
        int clustPixNumS1;          // Minimum number of S1 pixels needed to take into consideration the cluster
        int clustPixNumS2;          // Minimum number of S2 pixels needed to take into consideration the cluster
        float ndviThrDist;          //Threshold of the NDVI distance calculated between clusters
        int compactnessThrS1;       // Threshold of the compactness in the S2 analysis (! Varies according to the radiusC S2! which has a default value = 3)
        int compactnessThrS2;       // same as above
        float percentHetero;        // Pixels percentage corresponding to the biggest cluster in the parcel. If the biggest cluster is above PerHetero, the parcel is considered as homogeneous.

    private:
        int s1NumImages;
        int s2NumImages;

        // Number of clusters S1 and S2. Varies according to the landscape.
        // By default, this number is 4 in the S2 clustering and 5 in the S1 clustering of more hilly area.
        int s1ClustersNo;
        int s2ClustersNo;
    } S4CHeterogneneityJobConfig;

public :
    S4CHeterogeneityHandler();

private:
    void HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                const JobSubmittedEvent &event) override;
    void HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                const TaskFinishedEvent &event) override;

    ProcessorJobDefinitionParams GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues) override;
    QList<std::reference_wrapper<TaskToSubmit>> CreateTasks(const S4CHeterogneneityJobConfig &cfg, QList<TaskToSubmit> &outAllTasksList);
    NewStepList CreateSteps(QList<TaskToSubmit> &allTasksList, const S4CHeterogneneityJobConfig &cfg);
    QStringList GetExtractParcelsTaskArgs(const S4CHeterogneneityJobConfig &cfg,const QString &parcelsPath,
                                          const QString &lutPath, const QString &tilesPath,
                                          const QString &opticalPath, const QString &radarPath,
                                          const QString &lpisPath);
    QStringList GetCropTypeTaskArgs(const S4CHeterogneneityJobConfig &cfg,  const QString &workingPath,
                                    const QString &tilesPath, const QString &radarPath, const QString &lpisPath);

    QStringList GetS2TemporalResTaskArgs(const S4CHeterogneneityJobConfig &cfg, const QString &in, const QString &inMsk,
                                         const QList<int> &inDates, const QString &out);
    QStringList GetGdalBuidVrtTaskArgs(const QStringList &files, const QString &vrtFile);

    QStringList GetS1ProductsListArgs(const QString &s1RastersDir, const QString &band, const QString &tile, const QString &s1PrdsListPath);
//    QStringList GetS1VrtTaskArgs(const QString &s1PrdsListPath, const QString &s1VrtPath);
//    QStringList GetS1RasterBuildTaskArgs(const QString &s1VrtPath, const QString &s1OutPath);

    QStringList GetS2ClusterPrepTaskArgs(const S4CHeterogneneityJobConfig &cfg, const QStringList &inFiles, const QString &tile, int periodIdx, const QString &out);
    QStringList GetS1ClusterPrepTaskArgs(const S4CHeterogneneityJobConfig &cfg, const QString &inS1CTDir, const QString &tile, int periodIdx, const QString &out);

    QStringList GetIsolatedPixelsTaskArgs(const S4CHeterogneneityJobConfig &cfg, const QString &in, Satellite sat, const QString &out);
    QStringList GetSpatialConnectivityTaskArgs(const S4CHeterogneneityJobConfig &cfg, const QString &in, Satellite sat, const QString &out);
    QStringList GetS2ClusterAnalysisTaskArgs(const S4CHeterogneneityJobConfig &cfg, const QString &inNdvi, const QString &tile, int periodIdx, const QString &smoothedRaster, const QString &localConRaster,
                                             const QString &out);
    QStringList GetS1ClusterAnalysisTaskArgs(const S4CHeterogneneityJobConfig &cfg, const QString &tile, int periodIdx, const QString &smoothedRaster, const QString &localConRaster,
                                             const QString &out);
    QStringList GetTilesAnalysisMergeTaskArgs(const QStringList &inputFiles, const QString &outFile);
    QStringList GetPeriodAnalysisTaskArgs(const S4CHeterogneneityJobConfig &cfg, const QList<int> periods, const QStringList &periodAnalysisFiles, const QString &outFile);

    QStringList GetProductFormatterArgs(TaskToSubmit &productFormatterTask, const S4CHeterogneneityJobConfig &cfg, const QStringList &listFiles);
};

