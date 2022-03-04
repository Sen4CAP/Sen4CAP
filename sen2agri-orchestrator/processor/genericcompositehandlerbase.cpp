#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "genericcompositehandlerbase.hpp"
#include "processorhandlerhelper.h"
#include "json_conversions.hpp"
#include "logger.hpp"
#include "s4c_utils.hpp"
#include <unordered_map>

#include "products/generichighlevelproducthelper.h"
#include "products/producthelperfactory.h"
using namespace orchestrator::products;

#include <unistd.h>
#include <sys/types.h>

// For unordered map and QString as key
namespace std {
  template<> struct hash<QString> {
    std::size_t operator()(const QString& s) const noexcept {
      return (size_t) qHash(s);
    }
  };
}

QList<MarkerDescriptorType> GenericCompositeHandler::allMarkerFileTypes =
{
    // L2A markers
    {"L2AB01", ProductType::L2AProductTypeId, {"B01"}, "B01", -10000, 0},
    {"L2AB02", ProductType::L2AProductTypeId, {"B02"}, "B02", -10000, 0},
    {"L2AB03", ProductType::L2AProductTypeId, {"B03"}, "B03", -10000, 0},
    {"L2AB04", ProductType::L2AProductTypeId, {"B04"}, "B04", -10000, 0},
    {"L2AB05", ProductType::L2AProductTypeId, {"B05"}, "B05", -10000, 0},
    {"L2AB06", ProductType::L2AProductTypeId, {"B06"}, "B06", -10000, 0},
    {"L2AB07", ProductType::L2AProductTypeId, {"B07"}, "B07", -10000, 0},
    {"L2AB08", ProductType::L2AProductTypeId, {"B08"}, "B08", -10000, 0},
    {"L2AB8A", ProductType::L2AProductTypeId, {"B8A"}, "B8A", -10000, 0},
    {"L2AB09", ProductType::L2AProductTypeId, {"B09"}, "B09", -10000, 0},
    {"L2AB10", ProductType::L2AProductTypeId, {"B10"}, "B10", -10000, 0},
    {"L2AB11", ProductType::L2AProductTypeId, {"B11"}, "B11", -10000, 0},
    {"L2AB12", ProductType::L2AProductTypeId, {"B12"}, "B12", -10000, 0},

    // L3B markers
    {"NDVI", ProductType::L3BProductTypeId, {"SNDVI"}, "", -10000, 0},
    {"LAI", ProductType::L3BProductTypeId, {"SLAIMONO"}, "", -10000, 0},
    {"FAPAR", ProductType::L3BProductTypeId, {"SFAPARMONO"}, "", -10000, 0},
    {"FCOVER", ProductType::L3BProductTypeId, {"SFCOVERMONO"}, "", -10000, 0},

    // S1 markers
    {"AMP", ProductType::S4CS1L2AmpProductTypeId, {"AMP", "BCK"}, "", 0, 0},
    {"COHE", ProductType::S4CS1L2CoheProductTypeId, {"COH"}, "", 0, 0}
};

bool compareByDate(const ProductMarkerInfo &prd1, const ProductMarkerInfo &prd2) {
    return prd1.prdFileInfo.containingPrd.created < prd2.prdFileInfo.containingPrd.created;
}

GenericCompositeHandler::GenericCompositeHandler(const QString &cfgPrefix, const QStringList &markerNames)
    : ProcessorHandler()

{
    m_cfgPrefix = cfgPrefix;
    m_markerNames = markerNames;
}

void GenericCompositeHandler::CreateTasks(const GenericCompositeJobPayload &jobPayload,
                                          const QMap<QString, QList<ProductMarkerInfo> > &tileFileInfos,
                                          QList<TaskToSubmit> &allTasksList) {
    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;

    int i;
    TaskToSubmit productFormatterTask{"product-formatter", {}};

    for(i = 0; i < tileFileInfos.size(); i++) {
        allTasksList.append(TaskToSubmit{"l3-composite", {}});
        productFormatterTask.parentTasks += allTasksList[allTasksList.size()-1];
        allTasksListRef.append(allTasksList[allTasksList.size()-1]);
    }
    allTasksList.append(productFormatterTask);
    allTasksListRef.append(allTasksList[allTasksList.size()-1]);

    SubmitTasks(*(jobPayload.pCtx), jobPayload.event.jobId, allTasksListRef);
}

void GenericCompositeHandler::CreateSteps(const GenericCompositeJobPayload &jobPayload,
                                          QList<TaskToSubmit> &allTasksList,
                                          const QMap<QString, QList<ProductMarkerInfo>> &tileFileInfos,
                                          const QString &additionalFilter)
{
    NewStepList allSteps;
    QMap<QString, QString> results;
    int i = 0;
    QString prdNameSuffix;
    for(auto tile: tileFileInfos.keys()) {
        const auto &infos = tileFileInfos.value(tile);
        if (infos.size() > 0) {
            TaskToSubmit &dataExtractionTask = allTasksList[i];
            const QString &compositeFile = dataExtractionTask.GetFilePath("L3_" + infos[0].markerInfo.marker + "_" + tile + ".tif");
            const NewStep &step = CreateStep(dataExtractionTask, tileFileInfos.value(tile), compositeFile, jobPayload.compositeMethod);
            allSteps.append(step);
            results[tile] = compositeFile;
            if (prdNameSuffix.size() == 0) {
                prdNameSuffix = infos[0].markerInfo.marker;
                if (additionalFilter.size() > 0) {
                    prdNameSuffix += "_";
                    prdNameSuffix += additionalFilter;
                }
            }
        }
        i++;
    }

    // Create the product formatter step
    TaskToSubmit &productFormatterTask = allTasksList[allTasksList.size() - 1];
    const QStringList &productFormatterArgs = GetProductFormatterArgs(jobPayload, productFormatterTask, results, prdNameSuffix);
    allSteps.append(CreateTaskStep(productFormatterTask, "ProductFormatter", productFormatterArgs));

    jobPayload.pCtx->SubmitSteps(allSteps);
}


void GenericCompositeHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                              const JobSubmittedEvent &evt)
{
    GenericCompositeJobPayload jobCfg(&ctx, evt, m_cfgPrefix);

    const QStringList &forcedEnabledMarkers = GetAlwaysEnabledMarkerNames();
    const QList<MarkerDescriptorType> &markers = GetMarkers(jobCfg.parameters, jobCfg.configParameters,
                                                            m_cfgPrefix, m_markerNames, forcedEnabledMarkers);
    QDateTime prdMinDate;
    QDateTime prdMaxDate;
    const QMap<QString, QList<ProductMarkerInfo>> &allTileFileInfos = ExtractFileInfos(ctx, evt, jobCfg.parameters, markers,
                                                                                       prdMinDate, prdMaxDate);
    jobCfg.UpdateMinMaxDates(prdMinDate, prdMaxDate);
    for (auto marker: markers) {
        // Filter the products by marker
        const QMap<QString, QList<ProductMarkerInfo>> &tileFileInfos = FilterByMarkerName(allTileFileInfos, marker.marker);
        const QList<FilterAndGroupingOptions> &filters = GetFilterAndGroupingOptions(jobCfg);
        if (filters.size() > 0) {
            for (const FilterAndGroupingOptions &filter: filters) {
                const QMap<QString, QList<ProductMarkerInfo>> &tileFileInfosFiltered = Filter(filter, tileFileInfos);
                if (tileFileInfosFiltered.size() > 0) {
                    QList<TaskToSubmit> allTasksList;
                    CreateTasks(jobCfg, tileFileInfosFiltered, allTasksList);
                    CreateSteps(jobCfg, allTasksList, tileFileInfosFiltered, filter.name);
                }
            }
        } else {
            QList<TaskToSubmit> allTasksList;
            CreateTasks(jobCfg, tileFileInfos, allTasksList);
            CreateSteps(jobCfg, allTasksList, tileFileInfos);
        }
    }
}

void GenericCompositeHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                              const TaskFinishedEvent &event)
{
    if (event.module == "product-formatter") {
        const QString &prodName = GetOutputProductName(ctx, event);
        const QString &productFolder = GetFinalProductFolder(ctx, event.jobId, event.siteId) + "/" + prodName;
        if(prodName != "") {
            const QString &quicklook = GetProductFormatterQuicklook(ctx, event);
            const QString &footPrint = GetProductFormatterFootprint(ctx, event);
            // Insert the product into the database
            GenericHighLevelProductHelper prdHelper(productFolder);
            ctx.InsertProduct({ GetOutputProductType(), event.processorId, event.siteId,
                                event.jobId, productFolder, prdHelper.GetAcqDate(),
                                prodName, quicklook, footPrint, std::experimental::nullopt, TileIdList(), ProductIdsList()  });
            // mark the job as finished
            ctx.MarkJobFinished(event.jobId);
        } else {
            // mark the job as failed
            ctx.MarkJobFailed(event.jobId);
            Logger::error(QStringLiteral("Cannot insert into database the product with name %1 and folder %2").arg(prodName).arg(productFolder));
        }
        // Now remove the job folder containing temporary files
        RemoveJobFolder(ctx, event.jobId, processorDescr.shortName);
    }
}

ProcessorJobDefinitionParams GenericCompositeHandler::GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                const ConfigurationParameterValueMap &requestOverrideCfgValues)
{
    ProcessorJobDefinitionParams params;
    params.isValid = false;
    params.retryLater = false;

    QDateTime seasonStartDate;
    QDateTime seasonEndDate;
    // extract the scheduled date
    QDateTime qScheduledDate = QDateTime::fromTime_t(scheduledDate);
    GetSeasonStartEndDates(ctx, siteId, seasonStartDate, seasonEndDate, qScheduledDate, requestOverrideCfgValues);
    QDateTime limitDate = seasonEndDate.addMonths(2);
    if(qScheduledDate > limitDate) {
        return params;
    }

    const ConfigurationParameterValueMap &mapCfg = ctx.GetConfigurationParameters(QString(m_cfgPrefix),
                                                                           siteId, requestOverrideCfgValues);
    std::map<QString, QString> configParams;
    for (const auto &p : mapCfg) {
        configParams.emplace(p.key, p.value);
    }
    int procPeriod = mapCfg["processor." + processorDescr.shortName + ".period"].value.toInt();
    if(procPeriod == 0) {
        procPeriod = 30;
    }
    QDateTime startDate = qScheduledDate.addDays(procPeriod);
    if (startDate < seasonStartDate) {
        startDate = seasonStartDate;
    }
    QDateTime endDate = qScheduledDate;
    if (endDate > seasonEndDate.addDays(1)) {
        endDate = seasonEndDate.addDays(1);
    }

    params.jsonParameters.append("{ \"scheduled_job\": \"1\", \"start_date\": \"" + startDate.toString("yyyyMMdd") + "\", " +
                                 "\"end_date\": \"" + endDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_start_date\": \"" + seasonStartDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_end_date\": \"" + seasonEndDate.toString("yyyyMMdd") + "\"}");

    params.isValid = true;

    return params;
}

void GenericCompositeHandler::HandleProductAvailableImpl(EventProcessingContext &, const ProductAvailableEvent &)
{
}

NewStep GenericCompositeHandler::CreateStep(TaskToSubmit &task, const QList<ProductMarkerInfo> &fileInfos,
                                            const QString &outFile, const QString &method)
{
    QStringList args = { "Sen4XGenericComposite", "-out", outFile,
                         "-bv", QString::number(fileInfos[0].markerInfo.noDataVal),
                         "-mskvld", QString::number(fileInfos[0].markerInfo.mskValidVal),
                         "-method", method,
                         "-il" };
    QStringList masks;
    for(auto fileInfo: fileInfos) {
        args += fileInfo.prdFileInfo.inFilePath;
        if (fileInfo.prdFileInfo.mask.size() > 0) {
            masks.append(fileInfo.prdFileInfo.mask);
        }
    }

    // Add masks if available
    if (masks.size() == fileInfos.size()) {
        args += "-msks";
        args += masks;
    }
    return CreateTaskStep(task, "Sen4XGenericComposite", args);
}

QStringList GenericCompositeHandler::GetProductFormatterArgs(const GenericCompositeJobPayload &jobPayload, TaskToSubmit &productFormatterTask,
                                                             const QMap<QString, QString> &tileResults, const QString &prdNameSuffix) {

    const auto &executionInfosPath = productFormatterTask.GetFilePath("executionInfos.xml");
    // WriteExecutionInfosFile(executionInfosPath, jobPayload, productDetails);

    const auto &targetFolder = GetFinalProductFolder(*jobPayload.pCtx, jobPayload.event.jobId, jobPayload.event.siteId);
    const auto &outPropsPath = productFormatterTask.GetFilePath(PRODUCT_FORMATTER_OUT_PROPS_FILE);
    QString strTimePeriod = jobPayload.minDate.toString("yyyyMMddTHHmmss").append("_").append(jobPayload.maxDate.toString("yyyyMMddTHHmmss"));
    QStringList productFormatterArgs = { "ProductFormatter",
                                         "-destroot", targetFolder,
                                         "-fileclass", "OPER",
                                         "-level", GetProductFormatterLevel(),
                                         "-vectprd", "0",
                                         "-baseline", "01.00",
                                         "-siteid", QString::number(jobPayload.event.siteId),
                                         "-timeperiod", strTimePeriod,
                                         "-processor", "l3genericcomposite",
                                         "-outprops", outPropsPath,
                                         "-gipp", executionInfosPath,
                                         "-prdnamesuffix", prdNameSuffix
                                       };
    productFormatterArgs += "-processor.l3genericcomposite.files";
    for(const QString &key: tileResults.keys()) {
        productFormatterArgs += GetProductFormatterTile(key);
        productFormatterArgs += tileResults[key];
    }

    return productFormatterArgs;
}

QList<MarkerDescriptorType> GenericCompositeHandler::GetMarkers(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                   const QString &procKeyPrefix, const QStringList &filteringMarkers, const QStringList &markersForcedEnabled)
{
    QList<MarkerDescriptorType> enabledMarkers;
    bool markerEnabled;
    // check the enabled markers
    for (const auto &marker: allMarkerFileTypes) {
        // filter markers first
        if (filteringMarkers.size() > 0 && !filteringMarkers.contains(marker.marker)) {
            continue;
        }
        markerEnabled = false;
        if (markersForcedEnabled.size() > 0) {
            if (markersForcedEnabled.contains(marker.marker)) {
                markerEnabled = true;
            }
        } else {
            markerEnabled = ProcessorHandlerHelper::GetBoolConfigValue(parameters, configParameters,
                                                                       marker.marker.toLower() + "_enabled", procKeyPrefix);
        }
        if (markerEnabled) {
            enabledMarkers.push_back(marker);
        }
    }
    return enabledMarkers;
}

QList<ProductType> GenericCompositeHandler::GetMarkerProductTypes(const QList<MarkerDescriptorType> &markers) {
    QList<ProductType> prdTypes;
    for (const MarkerDescriptorType &marker: markers) {
        if (!prdTypes.contains(marker.prdType)) {
            prdTypes.push_back(marker.prdType);
        }
    }
    return prdTypes;
}

QMap<QString, QList<ProductMarkerInfo>> GenericCompositeHandler::ExtractFileInfos(EventProcessingContext &ctx, const JobSubmittedEvent &evt,
                                                                                  const QJsonObject &parameters, const QList<MarkerDescriptorType> &markers,
                                                                                  QDateTime &prdMinDate, QDateTime &prdMaxDate) {
    QMap<QString, QList<ProductMarkerInfo>> fileInfos;

    if (markers.size() > 0) {
        const QList<ProductType> &prdTypes = GetMarkerProductTypes(markers);
        for (ProductType prdType: prdTypes) {
            std::unique_ptr<ProductHelper> prdHelper = ProductHelperFactory::GetProductHelper(prdType);
            const ProductList &prds = ProcessorHandler::GetInputProducts(ctx, parameters, evt.siteId, prdType,
                                                                    &prdMinDate, &prdMaxDate);
            if (prds.size() == 0) {
                continue;
            }
            for (const auto &marker: markers) {
                for (const Product &prd: prds) {
                    const QDateTime &prdDate = prd.created;
                    if (!prdDate.isValid()) {
                        continue;
                    }
                    if (marker.prdType == prdType) {
                        prdHelper->SetProduct(prd.fullPath);
                        QStringList masks;
                        if (prdHelper->HasMasks()) {
                            masks = prdHelper->GetProductMasks();
                        }
                        for (const QString &pattern: marker.markerSubstrInFileName) {
                            const QMap<QString, QString> &tileFiles = prdHelper->GetProductFilesByTile(pattern);
                            for (auto tile: tileFiles.keys()) {
                                QList<ProductMarkerInfo> &prdMarkerInfos = fileInfos[tile];
                                const QString &mask = GetByTile(masks, tile);
                                const ProductMarkerInfo &prdMarkerInfo = {marker, tileFiles.value(tile), mask, prd};
                                // Keep the markers sorted by product date
                                prdMarkerInfos.insert(std::upper_bound(prdMarkerInfos.begin(), prdMarkerInfos.end(), prdMarkerInfo, compareByDate), prdMarkerInfo);
                            }
                        }
                    }
                }
            }
        }
    } else {
        Logger::error(QStringLiteral("No enabled markers for job %1!!!").arg(evt.jobId));
    }

    return fileInfos;
}

QMap<QString, QList<ProductMarkerInfo>> GenericCompositeHandler::FilterByMarkerName(const QMap<QString, QList<ProductMarkerInfo>> &tileFileInfos, const QString &marker)
{
    QMap<QString, QList<ProductMarkerInfo>> ret;
    for (auto tile: tileFileInfos.keys()) {
        const QList<ProductMarkerInfo> &infos = tileFileInfos.value(tile);
        for(auto info: infos) {
            if (info.markerInfo.marker == marker) {
                ret[tile].push_back(info);
            }
        }
    }
    return ret;
}

QString GenericCompositeHandler::GetByTile(const QStringList &files, const QString &filter) {
    for (const QString &file : files) {
        if (QFileInfo(file).baseName().contains(filter)) {
            return file;
        }
    }
    return "";
}

QString GenericCompositeHandler::GetProductFormatterLevel()
{
    switch (GetOutputProductType()) {
        case ProductType::S1CompositeProductTypeId:
            return "L3S1COMP";
        default:
            return "L3BICOMP";
    }
}

void GenericCompositeHandler::WriteExecutionInfosFile(const QString &executionInfosPath,
                                               const GenericCompositeJobPayload &jobPayload,
                                               const QList<ProductDetails> &productDetails)
{
    std::ofstream executionInfosFile;
    try {
        executionInfosFile.open(executionInfosPath.toStdString().c_str(), std::ofstream::out);
        executionInfosFile << "<?xml version=\"1.0\" ?>" << std::endl;
        executionInfosFile << "<metadata>" << std::endl;
//        executionInfosFile << "  <General>" << std::endl;

//        executionInfosFile << "  <Dates_information>" << std::endl;
        // TODO: We should get these infos somehow but without parsing here anything
        // executionInfosFile << "    <start_date>" << 2013031 << "</start_date>" << std::endl;
        // executionInfosFile << "    <end_date>" << 20130422 << "</end_date>" << std::endl;
//        executionInfosFile << "    <synthesis_date>" << cfg.l3aSynthesisDate.toStdString()
//                           << "</synthesis_date>" << std::endl;
//        executionInfosFile << "    <synthesis_half>" << cfg.synthalf.toStdString()
//                           << "</synthesis_half>" << std::endl;
//        executionInfosFile << "  </Dates_information>" << std::endl;

        executionInfosFile << "  <XML_files>" << std::endl;
        for (int i = 0; i < productDetails.size(); i++) {
            executionInfosFile << "    <XML_" << std::to_string(i) << ">"
                               << productDetails[i].GetProduct().fullPath.toStdString() << "</XML_" << std::to_string(i)
                               << ">" << std::endl;
        }
        executionInfosFile << "  </XML_files>" << std::endl;
        executionInfosFile << "</metadata>" << std::endl;
        executionInfosFile.close();
    } catch (...) {
    }
}
