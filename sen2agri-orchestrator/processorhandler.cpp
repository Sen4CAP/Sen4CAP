#include "processorhandler.hpp"

#include <QDir>
#include <QFile>
#include <QFileInfo>
#include <QFileInfoList>
#include <fstream>

#include "schedulingcontext.h"
#include "logger.hpp"
#include "stepexecutiondecorator.h"

#include "processor/products/producthelperfactory.h"
using namespace orchestrator::products;

bool removeDir(const QString & dirName)
{
    bool result = true;
    QDir dir(dirName);

    if (dir.exists(dirName)) {
        Q_FOREACH(QFileInfo info, dir.entryInfoList(QDir::NoDotAndDotDot | QDir::System | QDir::Hidden  | QDir::AllDirs | QDir::Files, QDir::DirsFirst)) {
            if (info.isDir()) {
                result = removeDir(info.absoluteFilePath());
            }
            else {
                result = QFile::remove(info.absoluteFilePath());
            }

            if (!result) {
                return result;
            }
        }
        result = dir.rmdir(dirName);
    }
    return result;
}

ProcessorHandler::~ProcessorHandler() {}

void ProcessorHandler::HandleProductAvailable(EventProcessingContext &ctx,
                                              const ProductAvailableEvent &event)
{
    HandleProductAvailableImpl(ctx, event);
}

void ProcessorHandler::HandleJobSubmitted(EventProcessingContext &ctx,
                                          const JobSubmittedEvent &event)
{
    try {
        HandleJobSubmittedImpl(ctx, event);
    } catch (const std::exception &e) {
        try {
            ctx.MarkEmptyJobFailed(event.jobId, e.what());
        } catch (const std::exception &e1) {
            Logger::debug(QStringLiteral("Error marking job failed  %1").arg(e.what()));
        }

        throw std::runtime_error(e.what());
    }
}

void ProcessorHandler::HandleTaskFinished(EventProcessingContext &ctx,
                                          const TaskFinishedEvent &event)
{
    HandleTaskFinishedImpl(ctx, event);
}

void ProcessorHandler::HandleJobUnsuccefulStop(EventProcessingContext &,
                                          int)
{
    // TODO
}

ProcessorJobDefinitionParams ProcessorHandler::GetProcessingDefinition(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                        const ConfigurationParameterValueMap &requestOverrideCfgValues) {
    return GetProcessingDefinitionImpl(ctx, siteId, scheduledDate, requestOverrideCfgValues);
}

void ProcessorHandler::HandleProductAvailableImpl(EventProcessingContext &,
                                                  const ProductAvailableEvent &)
{
}

QString ProcessorHandler::GetFinalProductFolder(EventProcessingContext &ctx, int jobId,
                                                int siteId) {
    auto configParameters = ctx.GetJobConfigurationParameters(jobId, PRODUCTS_LOCATION_CFG_KEY);
    const QString &siteName = ctx.GetSiteShortName(siteId);

    auto it = configParameters.find(PRODUCTS_LOCATION_CFG_KEY);
    if (it == std::end(configParameters)) {
        throw std::runtime_error(QStringLiteral("No final product folder configured for site %1 (id = %2) and processor %3")
                                     .arg(siteName)
                                     .arg(siteId)
                                     .arg(processorDescr.shortName)
                                     .toStdString());
    }
    QString folderName = (*it).second;
    folderName = folderName.replace("{site}", siteName);
    folderName = folderName.replace("{processor}", processorDescr.shortName);

    return folderName;
}

bool ProcessorHandler::NeedRemoveJobFolder(EventProcessingContext &ctx, int jobId, const QString &procName)
{
    QString strKey = "executor.processor." + procName + ".keep_job_folders";
    auto configParameters = ctx.GetJobConfigurationParameters(jobId, strKey);
    auto keepStr = configParameters[strKey];
    bool bRemove = true;
    if(keepStr == "1") bRemove = false;
    return bRemove;
}

bool ProcessorHandler::RemoveJobFolder(EventProcessingContext &ctx, int jobId, const QString &procName)
{
    if(NeedRemoveJobFolder(ctx, jobId, procName)) {
        QString jobOutputPath = ctx.GetJobOutputPath(jobId, procName);
        Logger::debug(QStringLiteral("Removing the job folder %1").arg(jobOutputPath));
        return removeDir(jobOutputPath);
    }
    return false;
}

QString ProcessorHandler::GetTaskOutputPathFromEvt(EventProcessingContext &ctx, const TaskFinishedEvent &event)
{
    return ctx.GetOutputPath(event.jobId, event.taskId, event.module, processorDescr.shortName);
}

void ProcessorHandler::WriteOutputProductPath(TaskToSubmit &prdCreatorTask, const QString &prdPath) {
    const auto &outPropsPath = prdCreatorTask.GetFilePath(PRODUCT_FORMATTER_OUT_PROPS_FILE);
    std::ofstream executionInfosFile;
    try {
        executionInfosFile.open(outPropsPath.toStdString().c_str(), std::ofstream::out);
        executionInfosFile << prdPath.toStdString() << std::endl;
        executionInfosFile.close();
    } catch (...) {
    }
}

QString ProcessorHandler::GetOutputProductPath(EventProcessingContext &ctx,
                                               const TaskFinishedEvent &outPrdCreatorTaskEndEvt) {
    const QString &prodFolderOutPath = GetTaskOutputPathFromEvt(ctx, outPrdCreatorTaskEndEvt) + "/" + PRODUCT_FORMATTER_OUT_PROPS_FILE;
    QStringList fileLines = ProcessorHandlerHelper::GetTextFileLines(prodFolderOutPath);
    if(fileLines.size() > 0) {
        return fileLines[0].trimmed();
    }
    return "";
}

void ProcessorHandler::WriteOutputProductSourceProductIds(TaskToSubmit &prdCreatorTask, const QList<int> &parentIds) {
    const auto &outPropsPath = prdCreatorTask.GetFilePath(PRODUCT_FORMATTER_IN_PRD_IDS_FILE);
    std::ofstream executionInfosFile;
    try {
        executionInfosFile.open(outPropsPath.toStdString().c_str(), std::ofstream::out);
        for(int id : parentIds) {
            if (id > 0) {
                executionInfosFile << id << std::endl;
            }
        }
        executionInfosFile.close();
    } catch (...) {
    }
}

ProductIdsList ProcessorHandler::GetOutputProductParentProductIds(EventProcessingContext &ctx,
                                               const TaskFinishedEvent &outPrdCreatorTaskEndEvt) {
    const QString &prodFolderOutPath = GetTaskOutputPathFromEvt(ctx, outPrdCreatorTaskEndEvt) + "/" + PRODUCT_FORMATTER_IN_PRD_IDS_FILE;
    const QStringList &fileLines = ProcessorHandlerHelper::GetTextFileLines(prodFolderOutPath);
    ProductIdsList ret;
    for(QString line: fileLines) {
        int val = line.trimmed().toInt();
        if (val > 0) {
            ret.append(val);
        }
    }
    return ret;
}

QString ProcessorHandler::GetOutputProductName(EventProcessingContext &ctx,
                                                        const TaskFinishedEvent &event) {
    const QString &productPath = GetOutputProductPath(ctx, event);
    if(productPath.length() > 0) {
        QString name = ProcessorHandlerHelper::GetFileNameFromPath(productPath);
        if(name.trimmed() != "") {
            return name;
        }
    }
    return "";
}

QString ProcessorHandler::GetProductFormatterQuicklook(EventProcessingContext &ctx,
                                                        const TaskFinishedEvent &prdFrmtTaskEndEvt) {
    const QString &mainFolderName = GetOutputProductPath(ctx, prdFrmtTaskEndEvt);
    QString quickLookName("");
    if(mainFolderName.size() > 0) {
        QString legacyFolder = mainFolderName;      // + "/LEGACY_DATA/";

        QDirIterator it(legacyFolder, QStringList() << "*.jpg", QDir::Files);
        // get the last shape file found
        QString quickLookFullName;
        while (it.hasNext()) {
            quickLookFullName = it.next();
            QFileInfo quickLookFileInfo(quickLookFullName);
            QString quickLookTmpName = quickLookFileInfo.fileName();
            if(quickLookTmpName.indexOf("_PVI_")) {
                return quickLookTmpName;
            }

        }
    }
    return quickLookName;
}

QString ProcessorHandler::GetProductFormatterFootprint(EventProcessingContext &ctx,
                                                        const TaskFinishedEvent &prdFrmtTaskEndEvt) {
    const QString &mainFolderName = GetOutputProductPath(ctx, prdFrmtTaskEndEvt);
    if(mainFolderName.size() > 0) {
        QString legacyFolder = mainFolderName + "/LEGACY_DATA/";

        QDirIterator it(legacyFolder, QStringList() << "*.xml", QDir::Files);
        // get the last shape file found
        QString footprintFullName;
        while (it.hasNext()) {
            footprintFullName = it.next();
            // check only the name if it contains MTD
            QFileInfo footprintFileInfo(footprintFullName);
            if(footprintFileInfo.fileName().indexOf("_MTD_") > 0) {
                // parse the XML file
                QFile inputFile(footprintFullName);
                if (inputFile.open(QIODevice::ReadOnly))
                {
                   QTextStream in(&inputFile);
                   while (!in.atEnd()) {
                        QString curLine = in.readLine();
                        // we assume we have only one line
                        QString startTag("<EXT_POS_LIST>");
                        int extposlistStartIdx = curLine.indexOf(startTag);
                        if(extposlistStartIdx >= 0) {
                            int extposlistEndIdxIdx = curLine.indexOf("</EXT_POS_LIST>");
                            if(extposlistEndIdxIdx >= 0) {
                                int startIdx = extposlistStartIdx + startTag.length();
                                QString extensionPointsStr = curLine.mid(startIdx,extposlistEndIdxIdx-startIdx);
                                QStringList extPointsList = extensionPointsStr.split(" ");
                                if((extPointsList.size() > 8) && (extPointsList.size() % 2) == 0) {
                                    QString footprint = "POLYGON((";
                                    for(int i = 0; i<extPointsList.size(); i++) {
                                        if(i > 0)
                                            footprint.append(", ");
                                        footprint.append(extPointsList[i]);

                                        footprint.append(" ");
                                        footprint.append(extPointsList[++i]);
                                    }
                                    footprint += "))";
                                    inputFile.close();
                                    return footprint;
                                }
                            }
                        }
                   }
                   inputFile.close();
                }
            }

        }
    }
    return "POLYGON((0.0 0.0, 0.0 0.0, 0.0 0.0, 0.0 0.0, 0.0 0.0))";
}

QStringList ProcessorHandler::GetDefaultProductFormatterArgs(EventProcessingContext &ctx, TaskToSubmit &productFormatterTask,
                                                             int jobId, int siteId, const QString &level, const QString &timePeriod,
                                                             const QString &processor, const QStringList &additionalParameters,
                                                             bool isVectPrd, const QString &gipp, bool compress) {
    const auto &targetFolder = GetFinalProductFolder(ctx, jobId, siteId);
    const auto &outPropsPath = productFormatterTask.GetFilePath(PRODUCT_FORMATTER_OUT_PROPS_FILE);
    const QString &gippTmp = (gipp.size() == 0 ? productFormatterTask.GetFilePath("executionInfos.txt") : gipp);
    QStringList productFormatterArgs =  {
                                            "ProductFormatter",
                                             "-destroot", targetFolder,
                                             "-fileclass", "OPER",
                                             "-level", level,
                                             "-baseline", "01.00",
                                             "-siteid", QString::number(siteId),
                                             "-processor", processor,
                                             "-compress", compress ? "1" : "0",
                                             "-vectprd", isVectPrd ? "1" : "0",
                                             "-gipp", gippTmp,
                                             "-outprops", outPropsPath
                                        };
    if (timePeriod.size() > 0) {
        productFormatterArgs += {"-timeperiod", timePeriod};
    }

    if (!isVectPrd) {
        const std::map<QString, QString> &zarrEnabled = ctx.GetJobConfigurationParameters(jobId, "processor.zarr.enabled");
        if (ProcessorHandlerHelper::GetBoolConfigValue(QJsonObject(), zarrEnabled,  "processor.zarr.enabled", "")) {
            productFormatterArgs += {"-zarr", "1"};
        }
    }

    productFormatterArgs += additionalParameters;
    return productFormatterArgs;
}

bool IsInSeason(const QDate &startSeasonDate, const QDate &endSeasonDate, const QDate &currentDate, QDateTime &startTime, QDateTime &endTime)
{
    if(startSeasonDate.isValid() && endSeasonDate.isValid()) {
        // normally this should not happen for summer season but can happen for winter season
        QDate sSeasonDate = startSeasonDate;
        if(endSeasonDate < startSeasonDate) {
            sSeasonDate = startSeasonDate.addYears(-1);
        }
        // we allow maximum 1 month after the end of season (in case of composite, for example,
        // we have scheduled date after end of season)
        if(currentDate >= sSeasonDate && currentDate <= endSeasonDate.addMonths(1)) {
            startTime = QDateTime(sSeasonDate);
            endTime = QDateTime(endSeasonDate);
            return true;
        } else {
            Logger::debug(QStringLiteral("IsInSeason: Date not in season (start = %1, end = %2, current=%3)")
                          .arg(sSeasonDate.toString())
                          .arg(endSeasonDate.toString())
                          .arg(currentDate.toString()));
        }
    } else {
        Logger::error(QStringLiteral("IsInSeason: Invalid season start or end date (start = %1, end = %2)")
                      .arg(startSeasonDate.toString())
                      .arg(endSeasonDate.toString()));
    }
    return false;
}

Season ProcessorHandler::GetSeason(ExecutionContextBase &ctx, int siteId, const QDateTime &executionDate) {
    const SeasonList &seasons = ctx.GetSiteSeasons(siteId);
    const QDate &date = executionDate.date();
    for (const Season &season: seasons) {
        if (date >= season.startDate && date < season.endDate.addDays(1)) {
            return season;
        }
    }
    return Season();
}

bool ProcessorHandler::GetSeasonStartEndDates(const SeasonList &seasons, QDateTime &startTime, QDateTime &endTime,
                                              const QDateTime &executionDate,
                                              const ConfigurationParameterValueMap &requestOverrideCfgValues) {
    const QDate &currentDate = executionDate.date();
    if(requestOverrideCfgValues.contains("site_season_id")) {
        const QString &seasonIdStr = requestOverrideCfgValues["site_season_id"].value;
        bool convOk = false;
        int siteSeasonId = seasonIdStr.toInt(&convOk);
        if (convOk && siteSeasonId >= 0) {
            for (const Season &season: seasons) {
                if (season.enabled && (season.seasonId == siteSeasonId) &&
                    IsInSeason(season.startDate, season.endDate, currentDate, startTime, endTime)) {
                    return true;
                }
            }
        } else {
            Logger::error(QStringLiteral("GetSeasonStartEndDates: Invalid site_season_id = %1").arg(seasonIdStr));
        }
    }
    // If somehow the site season id is not set, then get the first season that contains the scheduled time
    for (const Season &season: seasons) {
        if (season.enabled && IsInSeason(season.startDate, season.endDate, currentDate, startTime, endTime)) {
            return true;
        }
    }

    return false;
}

bool ProcessorHandler::GetSeasonStartEndDates(ExecutionContextBase &ctx, int siteId,
                                              QDateTime &startTime, QDateTime &endTime,
                                              const QDateTime &executionDate,
                                              const ConfigurationParameterValueMap &requestOverrideCfgValues) {
    return GetSeasonStartEndDates(ctx.GetSiteSeasons(siteId), startTime, endTime, executionDate, requestOverrideCfgValues);
}

bool ProcessorHandler::GetBestSeasonToMatchDate(ExecutionContextBase &ctx, int siteId,
                                              QDateTime &startTime, QDateTime &endTime,
                                              const QDateTime &executionDate,
                                              const ConfigurationParameterValueMap &requestOverrideCfgValues) {
    const SeasonList &seasons = ctx.GetSiteSeasons(siteId);
    const QDate &currentDate = executionDate.date();
    if(requestOverrideCfgValues.contains("site_season_id")) {
        const QString &seasonIdStr = requestOverrideCfgValues["site_season_id"].value;
        bool convOk = false;
        int siteSeasonId = seasonIdStr.toInt(&convOk);
        if (convOk && siteSeasonId >= 0) {
            for (const Season &season: seasons) {
                if (season.seasonId == siteSeasonId) {
                    if (season.enabled) {
                        startTime = QDateTime(season.startDate);
                        endTime = QDateTime(season.endDate);
                        return true;
                    } else {
                        Logger::error(QStringLiteral("GetSeasonStartEndDates: Season with site_season_id = %1 is disabled!").arg(seasonIdStr));
                        return false;
                    }
                }
            }
        } else {
            Logger::error(QStringLiteral("GetSeasonStartEndDates: Invalid site_season_id = %1").arg(seasonIdStr));
        }
    }

    // We are searching either the first season that contains this date or the last season whose end
    // date is before the provided execution date
    // TODO: Maybe we should look also for overlapping seasons, in which case, we should take the
    //       minimum start date and the maximum end date
    Season bestSeason;
    bool bestSeasonSet = false;
    for (const Season &season: seasons) {
        if (season.enabled) {
            if (IsInSeason(season.startDate, season.endDate, currentDate, startTime, endTime)) {
                return true;
            } else {
                if (currentDate >= season.endDate) {
                    if (!bestSeasonSet || bestSeason.endDate < season.endDate) {
                        bestSeason = season;
                        bestSeasonSet = true;
                    }
                }
            }
        }
    }
    if (bestSeasonSet) {
        startTime = QDateTime(bestSeason.startDate);
        endTime = QDateTime(bestSeason.endDate);
        return true;
    }

    return false;
}

QStringList ProcessorHandler::FilterProducts(const QStringList &products, const ProductType &prdType) {
    QStringList retPrds;
    for (const auto &prd : products) {
        if (prdType == ProductHelperFactory::GetProductHelper(prd)->GetProductType()) {
            retPrds.append(prd);
        }
    }
    return retPrds;
}

QStringList ProcessorHandler::GetInputProductNames(const QJsonObject &parameters, const QString &paramsCfgKey) {
    QStringList listProducts;
    const auto &inputProducts = parameters[paramsCfgKey].toArray();
    for (const auto &inputProduct : inputProducts) {
        listProducts.append(inputProduct.toString());
    }
    return listProducts;
}

QStringList ProcessorHandler::GetInputProductNames(const QJsonObject &parameters, const ProductType &prdType) {
    QString prdTypeStr;
    if (prdType != ProductType::InvalidProductTypeId && prdType != ProductType::L2AProductTypeId &&
            prdType != ProductType::MaskedL2AProductTypeId) {
        // Try to extract the products from the key specific for this product type
        prdTypeStr = ProductHelper::GetProductTypeShortName(prdType);
    }
    if (prdTypeStr.size() > 0) {
        const QStringList &listProducts = GetInputProductNames(parameters, "input_" + prdTypeStr);
        if (listProducts.size() > 0) {
            return listProducts;
        }
    }
    const QStringList &listProducts = GetInputProductNames(parameters, "input_products");
    return prdTypeStr.size() > 0 ? FilterProducts(listProducts, prdType) : listProducts;
}

ProductList ProcessorHandler::GetInputProducts(EventProcessingContext &ctx,
                                                          const QJsonObject &parameters, int siteId,
                                                          const ProductType &prdType,
                                                          QDateTime *pMinDate, QDateTime *pMaxDate) {
    ProductList retPrds;
    bool isL2AMskEnabled = IsL2AValidityMaskEnabled(ctx, parameters, siteId);
    ProductType prodType = (isL2AMskEnabled && prdType == ProductType::L2AProductTypeId) ?
                ProductType::MaskedL2AProductTypeId : prdType;
    const QStringList &prdNames = GetInputProductNames(parameters, prodType);

    // get the products from the input_products or based on date_start or date_end
    if(prdNames.size() == 0) {
        const auto &startDate = QDateTime::fromString(parameters["start_date"].toString(), "yyyyMMdd");
        const auto &endDateStart = QDateTime::fromString(parameters["end_date"].toString(), "yyyyMMdd");
        if(startDate.isValid() && endDateStart.isValid()) {
            // update min/max dates, if requested
            if (pMinDate && (!pMinDate->isValid() || *pMinDate > startDate)) {
                *pMinDate = startDate;
            }
            // we consider the end of the end date day
            const auto endDate = endDateStart.addSecs(SECONDS_IN_DAY-1);
            if (pMaxDate && (!pMaxDate->isValid() || *pMaxDate < endDate)) {
                *pMaxDate = endDate;
            }

            return ctx.GetProducts(siteId, (int)prodType, startDate, endDate);
        }
    } else {
        const ProductList &prds = ctx.GetProducts(siteId, prdNames);
        for (const auto &prd : prds) {
            if (prdNames.contains(prd.name)) {
                retPrds.append(prd);
                if (pMinDate || pMaxDate) {
                    const QDateTime &prdDate = prd.created;
                    // update min/max dates, if requested
                    if (pMinDate && (!pMinDate->isValid() || *pMinDate > prdDate)) {
                        *pMinDate = prdDate;
                    }
                    if (pMaxDate && (!pMaxDate->isValid() || *pMaxDate < prdDate)) {
                        *pMaxDate = prdDate;
                    }
                }
            } else {
                Logger::error(QStringLiteral("The product path does not exists %1.").arg(prd.name));
                return ProductList();
            }
        }
    }

    return retPrds;
}

TilesTimeSeries ProcessorHandler::GroupL2ATiles(EventProcessingContext &ctx, const QList<ProductDetails> &productDetails)
{
    // perform a first iteration to see the satellites IDs in all tiles
    TilesTimeSeries timeSeries(productDetails);

    // Get the primary satellite id
    Satellite primarySatId = timeSeries.GetPrimarySatellite();
    QMap<QString, QStringList> mapMtdFilesForPrimaryTile;
    const QMap<QString, TileTimeSeriesInfo> &mapTiles = timeSeries.GetTileTimeseriesInfos();

    // first iterate and fill the map for the primary satellite tiles
    for(const TileTimeSeriesInfo &info : mapTiles.values())
    {
        bool isPrimarySatIdInfo = ((info.uniqueSatteliteIds.size() == 1) &&
                                   (primarySatId == info.uniqueSatteliteIds[0]));
        if (isPrimarySatIdInfo) {
            QStringList prdsTilesMetaFiles;
            for (const TileMetadataDetails &mtdDetails : info.GetTileTimeSeriesInfoFiles())
            {
                prdsTilesMetaFiles.append(mtdDetails.tileMetaFile);

            }
            const TileList &tiles = ctx.GetIntersectingTiles(primarySatId, info.tileId);
            for(const TileTimeSeriesInfo &secInfo : mapTiles.values())
            {
                bool isSecSatIdInfo = !((secInfo.uniqueSatteliteIds.size() == 1) &&
                                           (primarySatId == secInfo.uniqueSatteliteIds[0]));
                if (isSecSatIdInfo) {
                    for (Tile tile : tiles) {
                        if (tile.tileId == secInfo.tileId) {
                            for (const TileMetadataDetails &mtdDetails2 : secInfo.GetTileTimeSeriesInfoFiles())
                            {
                                prdsTilesMetaFiles.append(mtdDetails2.tileMetaFile);
                            }
                            break;
                        }
                    }
                }
            }
            mapMtdFilesForPrimaryTile[info.tileId] = prdsTilesMetaFiles;
        }
    }
    // rebuild the time series according to the new files
    TilesTimeSeries timeSeries2;
    timeSeries2.InitializeFrom(timeSeries, mapMtdFilesForPrimaryTile);

    return timeSeries2;
}

QString ProcessorHandler::GetProductFormatterTile(const QString &tile) {
    if(tile.indexOf("TILE_") == 0)
        return tile;
    return ("TILE_" + tile);
}

void ProcessorHandler::SubmitTasks(EventProcessingContext &ctx,
                                   int jobId,
                                   const QList<std::reference_wrapper<TaskToSubmit>> &tasks) {
    ctx.SubmitTasks(jobId, tasks, processorDescr.shortName);
}

QMap<Satellite, TileList> ProcessorHandler::GetSiteTiles(EventProcessingContext &ctx, int siteId)
{
    QMap<Satellite, TileList> retMap;
    retMap[Satellite::Sentinel2] = ctx.GetSiteTiles(siteId, static_cast<int>(Satellite::Sentinel2));
    retMap[Satellite::Landsat8] = ctx.GetSiteTiles(siteId, static_cast<int>(Satellite::Landsat8));

    return retMap;
}

bool ProcessorHandler::IsCloudOptimizedGeotiff(const std::map<QString, QString> &configParameters) {
    const QStringList &tokens = processorDescr.shortName.split('_');
    const QString &strKey = "processor." + tokens[0] + ".cloud_optimized_geotiff_output";
    auto isCog = ProcessorHandlerHelper::GetMapValue(configParameters, strKey);
    bool bIsCog = false;
    if(isCog == "1") bIsCog = true;
    return bIsCog;
}

NewStep ProcessorHandler::CreateTaskStep(TaskToSubmit &task, const QString &stepName, const QStringList &stepArgs)
{
    return StepExecutionDecorator::GetInstance()->CreateTaskStep(this->processorDescr.shortName, task, stepName, stepArgs);
}

bool ProcessorHandler::IsL2AValidityMaskEnabled(EventProcessingContext &ctx, const QJsonObject &parameters, int siteId) {
    return ProcessorHandlerHelper::GetBoolConfigValue(parameters,
                       ctx.GetConfigurationParameters("processor.l2a_msk.enabled", siteId),
                       "processor.l2a_msk.enabled", "");
}

bool ProcessorHandler::CheckAllAncestorProductCreation(ExecutionContextBase &ctx, int siteId, const ProductType &prdType,
                                                       const QDateTime &startDate, const QDateTime &endDate) {
    QJsonObject parameters;
    bool checkAncestorsDisabled = ProcessorHandlerHelper::GetBoolConfigValue(parameters,
                           ctx.GetConfigurationParameters("orchestrator.check_ancestors.disabled", siteId),
                           "orchestrator.check_ancestors.disabled", "");
    Logger::info(QStringLiteral("orchestrator.check_ancestors.disabled = %1")
                 .arg(checkAncestorsDisabled));
    bool ret = true;
    if (!checkAncestorsDisabled)
    {
        // TODO: We should load these mappings from a file
        QList<ProductType> ancestorProductTypes;
        switch (prdType) {
            case ProductType::MaskedL2AProductTypeId :
                ancestorProductTypes = {ProductType::L2AProductTypeId, ProductType::FMaskProductTypeId};
                break;
            case ProductType::L3AProductTypeId:
            case ProductType::L3BProductTypeId:
            case ProductType::L3EProductTypeId:
            case ProductType::L4AProductTypeId:
            case ProductType::L4BProductTypeId:
            case ProductType::L3CProductTypeId:
            case ProductType::L3DProductTypeId:
                ancestorProductTypes = {ProductType::L2AProductTypeId, ProductType::FMaskProductTypeId,
                                        ProductType::MaskedL2AProductTypeId};
                break;
            case ProductType::S4CL4AProductTypeId:
            case ProductType::S4CL4BProductTypeId:
            case ProductType::S4CL4CProductTypeId:
            case ProductType::S4MDB1ProductTypeId:
            case ProductType::S4MDB2ProductTypeId:
            case ProductType::S4MDB3ProductTypeId:
            case ProductType::S4MDBL4AOptMainProductTypeId:
            case ProductType::S4MDBL4AOptReProductTypeId:
            case ProductType::S4MDBL4ASarMainProductTypeId:
            case ProductType::S4MDBL4ASarTempProductTypeId:
                ancestorProductTypes = {ProductType::S4CS1L2AmpProductTypeId, ProductType::S4CS1L2CoheProductTypeId,
                                        ProductType::L2AProductTypeId, ProductType::FMaskProductTypeId,
                                        ProductType::MaskedL2AProductTypeId};
                break;
            case ProductType::S4SPermCropsProductTypeId :
            case ProductType::S4SYieldFeatProductTypeId :
            case ProductType::S4SCropTypeMappingProductTypeId :
                break;
            case ProductType::S1CompositeProductTypeId :
                ancestorProductTypes = {ProductType::S4CS1L2AmpProductTypeId};
                break;
            case ProductType::L3IndicatorsCompositeProductTypeId:
                ancestorProductTypes = {ProductType::L2AProductTypeId, ProductType::FMaskProductTypeId,
                                        ProductType::MaskedL2AProductTypeId, ProductType::L3BProductTypeId};
                break;
            default:
                break;
        }
        for (ProductType ancPrdType : ancestorProductTypes) {
            QList<int> satIds;
            if (ancPrdType == ProductType::L2AProductTypeId) {
                satIds = {(int)Satellite::Sentinel2, (int)Satellite::Landsat8};
            } else if (ancPrdType == ProductType::S4CS1L2AmpProductTypeId ||
                       ancPrdType == ProductType::S4CS1L2CoheProductTypeId) {
                satIds = {(int)Satellite::Sentinel1};
            }
            if (!ctx.IsProcessingDone(ancPrdType, siteId, startDate, endDate, satIds)) {
                Logger::info(QStringLiteral("Processing is not yet finished for site = %1, product type = %2 in order to "
                                            "be able to generate product with type id %3 for start date %4 and end date %5")
                             .arg(siteId).arg((int)ancPrdType).arg((int)prdType)
                             .arg(startDate.toString("yyyyMMdd")).arg(endDate.toString("yyyyMMdd")));
                ret = false;
                break;

            }
        }
    }
    return ret;
}

