#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "logger.hpp"
#include "processorhandlerhelper.h"
#include "s4c_croptypehandler.hpp"

#include "products/generichighlevelproducthelper.h"
using namespace orchestrator::products;

S4CCropTypeHandler::S4CCropTypeHandler()
{
    m_cfgKeys = QStringList() << "lc"
                              << "min-s2-pix"
                              << "min-s1-pix"
                              << "best-s2-pix"
                              << "pa-min"
                              << "pa-train-h"
                              << "pa-train-l"
                              << "sample-ratio-h"
                              << "sample-ratio-l"
                              << "smote-target"
                              << "smote-k"
                              << "num-trees"
                              << "min-node-size"
                              << "mode";
}

QList<std::reference_wrapper<TaskToSubmit>>
S4CCropTypeHandler::CreateTasks(QList<TaskToSubmit> &outAllTasksList)
{
    outAllTasksList.append(TaskToSubmit{ "s4c-l4a-extract-parcels", {} });
    outAllTasksList.append(TaskToSubmit{ "s4c-crop-type", { outAllTasksList[0] } });
    outAllTasksList.append(TaskToSubmit{ "product-formatter", {outAllTasksList[1]} });
    outAllTasksList.append(TaskToSubmit{ "export-product-launcher", { outAllTasksList[2] } });

    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    for (TaskToSubmit &task : outAllTasksList) {
        allTasksListRef.append(task);
    }
    return allTasksListRef;
}

NewStepList S4CCropTypeHandler::CreateSteps(EventProcessingContext &ctx,
                                            const JobSubmittedEvent &event,
                                            QList<TaskToSubmit> &allTasksList,
                                            const CropTypeJobConfig &cfg)
{
    int curTaskIdx = 0;
    NewStepList allSteps;
    TaskToSubmit &extractParcelsTask = allTasksList[curTaskIdx++];
    TaskToSubmit &cropTypeTask = allTasksList[curTaskIdx++];
    TaskToSubmit &prdFormatterTask = allTasksList[curTaskIdx++];

    const QString &parcelsPath = extractParcelsTask.GetFilePath("parcels.csv");
    const QString &lutPath = extractParcelsTask.GetFilePath("lut.csv");
    const QString &tilesPath = extractParcelsTask.GetFilePath("tiles.csv");
    const QString &opticalPath = extractParcelsTask.GetFilePath("optical.csv");
    const QString &radarPath = extractParcelsTask.GetFilePath("radar.csv");
    const QString &lpisPath = extractParcelsTask.GetFilePath("lpis.txt");

    const QString &workingPath = cropTypeTask.GetFilePath("");
    const QString &outMarkerFilesInfos = cropTypeTask.GetFilePath("markers_product_infos.csv");
    const QString &prdFinalFilesDir = prdFormatterTask.GetFilePath("");

    const QStringList &extractParcelsArgs = GetExtractParcelsTaskArgs(cfg, parcelsPath, lutPath, tilesPath,
                                                                      opticalPath, radarPath, lpisPath);
    allSteps.append(CreateTaskStep(extractParcelsTask, "S4CCropTypeExtractParcels", extractParcelsArgs));

    const QStringList &cropTypeArgs = GetCropTypeTaskArgs(cfg, prdFinalFilesDir, workingPath, outMarkerFilesInfos,
                                                          parcelsPath, lutPath, tilesPath,
                                                          opticalPath, radarPath, lpisPath);
    allSteps.append(CreateTaskStep(cropTypeTask, "S4CCropType", cropTypeArgs));

    const QStringList &productFormatterArgs = GetProductFormatterArgs(
        prdFormatterTask, ctx, event, prdFinalFilesDir, cfg.startDate, cfg.endDate);
    allSteps.append(CreateTaskStep(prdFormatterTask, "ProductFormatter", productFormatterArgs));

    const auto &productFormatterPrdFileIdFile = prdFormatterTask.GetFilePath("prd_infos.txt");
    TaskToSubmit &exportCsvToShpProductTask = allTasksList[curTaskIdx++];
    const QStringList &exportCsvToShpProductArgs = { "-f", productFormatterPrdFileIdFile, "-o",
                                                     "CropType.gpkg" };
    allSteps.append(
        CreateTaskStep(exportCsvToShpProductTask, "export-product-launcher", exportCsvToShpProductArgs));

    return allSteps;
}

QStringList S4CCropTypeHandler::GetExtractParcelsTaskArgs(const CropTypeJobConfig &cfg,
                                                    const QString &parcelsPath, const QString &lutPath,
                                                    const QString &tilesPath, const QString &opticalPath,
                                                    const QString &radarPath,  const QString &lpisPath)
{
    QStringList extractParcelsArgs = { "-s",
                                 QString::number(cfg.event.siteId),
                                 "--season-start",
                                 cfg.startDate.toString("yyyy-MM-dd"),
                                 "--season-end",
                                 cfg.endDate.toString("yyyy-MM-dd")};
    if (cfg.tileIds.size() > 0) {
        extractParcelsArgs += "--tiles";
        extractParcelsArgs.append(cfg.tileIds);
    }

    if (cfg.filterProductNames.size() > 0) {
        extractParcelsArgs += "--products";
        extractParcelsArgs.append(cfg.filterProductNames);
    }

    extractParcelsArgs.append("--");
    extractParcelsArgs.append(parcelsPath);
    extractParcelsArgs.append(lutPath);
    extractParcelsArgs.append(tilesPath);
    extractParcelsArgs.append(opticalPath);
    extractParcelsArgs.append(radarPath);
    extractParcelsArgs.append(lpisPath);

    return extractParcelsArgs;
}

QStringList S4CCropTypeHandler::GetCropTypeTaskArgs(const CropTypeJobConfig &cfg,
                                                    const QString &prdTargetDir,
                                                    const QString &workingPath,const QString &outMarkerFilesInfos,
                                                    const QString &parcelsPath, const QString &lutPath,
                                                    const QString &tilesPath, const QString &opticalPath,
                                                    const QString &radarPath,  const QString &lpisPath)
{

    QStringList cropTypeArgs = { "-s",
                                 QString::number(cfg.event.siteId),
                                 "--season-start",
                                 cfg.startDate.toString("yyyy-MM-dd"),
                                 "--season-end",
                                 cfg.endDate.toString("yyyy-MM-dd"),
                                 "--working-path",
                                 workingPath,
                                 "--out-path",
                                 prdTargetDir,
                                 "--parcels",
                                 parcelsPath,
                                 "--lut",
                                 lutPath,
                                 "--tile-footprints",
                                 tilesPath,
                                 "--optical-products",
                                 opticalPath,
                                 "--radar-products",
                                 radarPath,
                                 "--lpis",
                                 lpisPath,
                                 "--target-path",
                                 GetFinalProductFolder(*(cfg.pCtx), cfg.event.jobId, cfg.event.siteId),
                                 "--outputs",
                                 outMarkerFilesInfos};

    for (const QString &cfgKey : m_cfgKeys) {
        auto it = cfg.mapCfgValues.find(cfgKey);
        if (it != cfg.mapCfgValues.end()) {
            const QString &value = it.value();
            if (value.size() > 0) {
                cropTypeArgs += QString("--").append(cfgKey);
                cropTypeArgs += value;
            }
        }
    }

    return cropTypeArgs;
}

QStringList S4CCropTypeHandler::GetProductFormatterArgs(TaskToSubmit &productFormatterTask,
                                                        EventProcessingContext &ctx,
                                                        const JobSubmittedEvent &event,
                                                        const QString &tmpPrdDir,
                                                        const QDateTime &minDate,
                                                        const QDateTime &maxDate)
{
    // ProductFormatter /home/cudroiu/sen2agri-processors-build
    //    -vectprd 1 -destroot
    //    /mnt/archive_new/test/Sen4CAP_L4C_Tests/NLD_Validation_TSA/OutPrdFormatter -fileclass OPER
    //    -level S4C_L4C -baseline 01.00 -siteid 4 -timeperiod 20180101_20181231 -processor generic
    //    -processor.generic.files <dir_with_list>

    const auto &targetFolder = GetFinalProductFolder(ctx, event.jobId, event.siteId);
    const auto &outPropsPath = productFormatterTask.GetFilePath(PRODUCT_FORMATTER_OUT_PROPS_FILE);
    const auto &executionInfosPath = productFormatterTask.GetFilePath("executionInfos.txt");
    QString strTimePeriod =
        minDate.toString("yyyyMMddTHHmmss").append("_").append(maxDate.toString("yyyyMMddTHHmmss"));
    QStringList productFormatterArgs = { "ProductFormatter",
                                         "-destroot",
                                         targetFolder,
                                         "-fileclass",
                                         "OPER",
                                         "-level",
                                         "S4C_L4A",
                                         "-vectprd",
                                         "1",
                                         "-baseline",
                                         "01.00",
                                         "-siteid",
                                         QString::number(event.siteId),
                                         "-timeperiod",
                                         strTimePeriod,
                                         "-processor",
                                         "generic",
                                         "-outprops",
                                         outPropsPath,
                                         "-gipp",
                                         executionInfosPath };
    productFormatterArgs += "-processor.generic.files";
    productFormatterArgs += tmpPrdDir;

    return productFormatterArgs;
}

bool S4CCropTypeHandler::GetStartEndDatesFromProducts(EventProcessingContext &ctx,
                                                      const JobSubmittedEvent &event,
                                                      QDateTime &startDate,
                                                      QDateTime &endDate,
                                                      QList<ProductDetails> &productDetails)
{
    const auto &parameters = QJsonDocument::fromJson(event.parametersJson.toUtf8()).object();
    const ProductList &prds = GetInputProducts(ctx, parameters, event.siteId);
    productDetails = ProcessorHandlerHelper::GetProductDetails(prds, ctx);

    return ProcessorHandlerHelper::GetIntevalFromProducts(prds, startDate, endDate);
}

void S4CCropTypeHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                                const JobSubmittedEvent &event)
{
    CropTypeJobConfig cfg(&ctx, event, m_cfgKeys);
    UpdateJobConfigParameters(cfg);

    QList<TaskToSubmit> allTasksList;
    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef = CreateTasks(allTasksList);
    SubmitTasks(ctx, cfg.event.jobId, allTasksListRef);
    NewStepList allSteps = CreateSteps(ctx, event, allTasksList, cfg);
    ctx.SubmitSteps(allSteps);
}

void S4CCropTypeHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                                const TaskFinishedEvent &event)
{
    if (event.module == "s4c-crop-type") {
        HandleMarkerProductsAvailable(ctx, event);

    } else if (event.module == "product-formatter") {
        const QString &prodName = GetOutputProductName(ctx, event);
        const QString &productFolder =
            GetFinalProductFolder(ctx, event.jobId, event.siteId) + "/" + prodName;
        if (prodName != "") {
            const QString &quicklook = GetProductFormatterQuicklook(ctx, event);
            const QString &footPrint = GetProductFormatterFootprint(ctx, event);
            // Insert the product into the database
            GenericHighLevelProductHelper prdHelper(productFolder);
            int prdId = ctx.InsertProduct({ ProductType::S4CL4AProductTypeId, event.processorId,
                                            event.siteId, event.jobId, productFolder, prdHelper.GetAcqDate(),
                                            prodName, quicklook, footPrint,
                                            std::experimental::nullopt, TileIdList(), ProductIdsList() });
            const QString &prodFolderOutPath =
                ctx.GetOutputPath(event.jobId, event.taskId, event.module,
                                  processorDescr.shortName) +
                "/" + "prd_infos.txt";

            QFile file(prodFolderOutPath);
            if (file.open(QIODevice::ReadWrite)) {
                QTextStream stream(&file);
                stream << prdId << ";" << productFolder << endl;
            }
        } else {
            ctx.MarkJobFailed(event.jobId);
            Logger::error(
                QStringLiteral("Cannot insert into database the product with name %1 and folder %2")
                    .arg(prodName)
                    .arg(productFolder));
        }
    } else if (event.module == "export-product-launcher") {
        ctx.MarkJobFinished(event.jobId);
        // Now remove the job folder containing temporary files
        RemoveJobFolder(ctx, event.jobId, processorDescr.shortName);
    }
}

ProcessorJobDefinitionParams S4CCropTypeHandler::GetProcessingDefinitionImpl(
    SchedulingContext &ctx,
    int siteId,
    int scheduledDate,
    const ConfigurationParameterValueMap &requestOverrideCfgValues)
{
    ProcessorJobDefinitionParams params;
    params.isValid = false;

    QDateTime seasonStartDate;
    QDateTime seasonEndDate;
    // extract the scheduled date
    QDateTime qScheduledDate = QDateTime::fromTime_t(scheduledDate);
    bool success = GetSeasonStartEndDates(ctx, siteId, seasonStartDate, seasonEndDate,
                                          qScheduledDate, requestOverrideCfgValues);
    // if cannot get the season dates
    if (!success) {
        Logger::debug(QStringLiteral("Scheduler CropType: Error getting season start dates for "
                                     "site %1 for scheduled date %2!")
                          .arg(siteId)
                          .arg(qScheduledDate.toString()));
        return params;
    }

    QDateTime limitDate = seasonEndDate.addMonths(2);
    if (qScheduledDate > limitDate) {
        Logger::debug(QStringLiteral("Scheduler CropType: Error scheduled date %1 greater than the "
                                     "limit date %2 for site %3!")
                          .arg(qScheduledDate.toString())
                          .arg(limitDate.toString())
                          .arg(siteId));
        return params;
    }

    ConfigurationParameterValueMap cfgValues =
        ctx.GetConfigurationParameters("processor.s4c_l4a.", siteId, requestOverrideCfgValues);
    // we might have an offset in days from starting the downloading products to start the S4C L4A
    // production
    int startSeasonOffset = cfgValues["processor.s4c_l4a.start_season_offset"].value.toInt();
    seasonStartDate = seasonStartDate.addDays(startSeasonOffset);

    QDateTime startDate = seasonStartDate;
    QDateTime endDate = qScheduledDate;
    // do not pass anymore the product list but the dates
    params.jsonParameters.append("{ \"scheduled_job\": \"1\", \"start_date\": \"" + startDate.toString("yyyyMMdd") + "\", " +
                                 "\"end_date\": \"" + endDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_start_date\": \"" + seasonStartDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_end_date\": \"" + seasonEndDate.toString("yyyyMMdd") + "\"}");

    // Normally, we need at least 1 product available, the crop mask and the shapefile in order to
    // be able to create a S4C L4A product but if we do not return here, the schedule block waiting
    // for products (that might never happen)
    bool waitForAvailProcInputs =
        (cfgValues["processor.s4c_l4a.sched_wait_proc_inputs"].value.toInt() != 0);
    if ((waitForAvailProcInputs == false) || ((params.productList.size() > 0))) {
        params.isValid = true;
        Logger::debug(
            QStringLiteral("Executing scheduled job. Scheduler extracted for S4C L4A a number "
                           "of %1 products for site ID %2 with start date %3 and end date %4!")
                .arg(params.productList.size())
                .arg(siteId)
                .arg(startDate.toString())
                .arg(endDate.toString()));
    } else {
        Logger::debug(QStringLiteral("Scheduled job for S4C L4A and site ID %1 with start date %2 "
                                     "and end date %3 will not be executed "
                                     "(productsNo = %4)!")
                          .arg(siteId)
                          .arg(startDate.toString())
                          .arg(endDate.toString())
                          .arg(params.productList.size()));
    }

    return params;
}


void S4CCropTypeHandler::UpdateJobConfigParameters(CropTypeJobConfig &cfgToUpdate)
{
    if(IsScheduledJobRequest(cfgToUpdate.parameters)) {
        QString strStartDate, strEndDate;
        if (ProcessorHandlerHelper::GetParameterValueAsString(cfgToUpdate.parameters, "start_date", strStartDate) &&
            ProcessorHandlerHelper::GetParameterValueAsString(cfgToUpdate.parameters, "end_date", strEndDate) &&
            cfgToUpdate.parameters.contains("input_products") && cfgToUpdate.parameters["input_products"].toArray().size() == 0) {
            cfgToUpdate.isScheduled = true;
            cfgToUpdate.startDate = ProcessorHandlerHelper::GetDateTimeFromString(strStartDate);
            cfgToUpdate.endDate = ProcessorHandlerHelper::GetDateTimeFromString(strEndDate);
        }
    } else {
        const QStringList &filterProductNames = GetInputProductNames(cfgToUpdate.parameters);
        cfgToUpdate.SetFilteringProducts(filterProductNames);

        QList<ProductDetails> productDetails;
        bool ret = GetStartEndDatesFromProducts(*(cfgToUpdate.pCtx), cfgToUpdate.event, cfgToUpdate.startDate, cfgToUpdate.endDate, productDetails);
        if (!ret || productDetails.size() == 0) {
            // try to get the start and end date if they are given
            cfgToUpdate.pCtx->MarkJobFailed(cfgToUpdate.event.jobId);
            throw std::runtime_error(
                QStringLiteral(
                    "No products provided at input or no products available in the specified interval")
                    .toStdString());
        }

        cfgToUpdate.tileIds = GetTileIdsFromProducts(*(cfgToUpdate.pCtx), cfgToUpdate.event, productDetails);

    }
}


QStringList S4CCropTypeHandler::GetTileIdsFromProducts(EventProcessingContext &ctx,
                                                       const JobSubmittedEvent &event,
                                                       const QList<ProductDetails> &productDetails)
{

    const TilesTimeSeries &mapTiles = ProcessorHandlerHelper::GroupTiles(ctx, event.siteId, productDetails, ProductType::L2AProductTypeId);

    // normally, we can use only one list by we want (not necessary) to have the
    // secondary satellite tiles after the main satellite tiles
    QStringList tilesList;
    for (const auto &tileId : mapTiles.GetTileIds()) {
        tilesList.append(tileId);
    }
    return tilesList;
}

bool S4CCropTypeHandler::IsScheduledJobRequest(const QJsonObject &parameters) {
    int jobVal;
    return ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "scheduled_job", jobVal) && (jobVal == 1);
}

void S4CCropTypeHandler::HandleMarkerProductsAvailable(EventProcessingContext &ctx,
                                                    const TaskFinishedEvent &event) {
    const QString &outMarkerInfos = ctx.GetOutputPath(event.jobId, event.taskId, event.module, this->processorDescr.shortName) + "markers_product_infos.csv";
    QFile file( outMarkerInfos );
    if (file.open(QIODevice::ReadOnly))
    {
        Logger::info(QStringLiteral("S4C L4A: Extracting the marker files for site id = %1 from %2")
                          .arg(event.siteId)
                          .arg(outMarkerInfos));
        QTextStream in(&file);
        QString defFootprint("POLYGON((0.0 0.0, 0.0 0.0, 0.0 0.0, 0.0 0.0, 0.0 0.0))");
        int i = 0;
        std::map<QString, int> mapHeader = {{"product_type_id", -1}, {"name", -1}, {"path", -1}, {"created_timestamp", -1}, {"tiles", -1}};
        while (!in.atEnd())
        {
            const QString &curLine = in.readLine();
            const QStringList &items = curLine.split(',');
            // we expect to have product_type_id, name, full_path, created_timestamp, tiles
            if (i == 0) {
                for(int j = 0; j<items.size(); j++) {
                    mapHeader[items[j]] = j;
                }
                i++;
            } else {
                int productTypeIdx = mapHeader["product_type_id"];
                int nameIdx = mapHeader["name"];
                int pathIdx = mapHeader["path"];
                int creationTimeIdx = mapHeader["created_timestamp"];
                int tilesIdx = mapHeader["tiles"];
                if (productTypeIdx >= 0 && productTypeIdx < items.size() &&
                        nameIdx >= 0 && nameIdx < items.size() &&
                        pathIdx >= 0 && pathIdx < items.size() &&
                        creationTimeIdx >= 0 && creationTimeIdx < items.size() &&
                        tilesIdx >= 0 && tilesIdx < items.size()) {

                    // extract the tiles
                    QString tilesStr = items[tilesIdx].trimmed();
                    tilesStr.remove('[').remove(']');
                    const QStringList &tiles = tilesStr.split(',');
                    TileIdList tileIds;
                    for (QString tile: tiles) {
                        tileIds.append(tile.remove('\''));
                    }

                    Logger::info(QStringLiteral("S4C L4A: Inserting markers product with name = %1, full path = %2")
                                      .arg(items[nameIdx])
                                      .arg(items[pathIdx]));

                    ctx.InsertProduct({ (ProductType)items[productTypeIdx].toInt(), event.processorId,
                                                event.siteId, event.jobId, items[pathIdx], QDateTime::fromString(items[creationTimeIdx], "yyyy-MM-dd HH:mm:ss"),
                                                items[nameIdx], QString(), defFootprint,
                                                std::experimental::nullopt, TileIdList(), ProductIdsList() });
                } else {
                    Logger::error(QStringLiteral("S4C L4A: One of required headers was not found in the markers products file with name = %1!").arg(outMarkerInfos));
                }
            }
        }
        file.close();
    } else {
        Logger::error(QStringLiteral("S4C L4A: Markers products file with name = %1 does not exist!").arg(outMarkerInfos));
    }
}
