#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "logger.hpp"
#include "processorhandlerhelper.h"
#include "s4s_permanent_crop_handler.hpp"
#include "s4c_utils.hpp"
#include "stepexecutiondecorator.h"

#include "products/generichighlevelproducthelper.h"
using namespace orchestrator::products;

#define ANNUAL_CROP_EXP     "(im1b2 > 5  && im1b2 > im1b3 && im1b2 > im1b4) ? 1 : 0"
#define PERENIAL_CROP_EXP   "(im1b3 > 15 && im1b3 > im1b2 && im1b3 > im1b4) ? 2 : 0"
#define NO_CROPLAND_EXP     "(im1b4 > 0  && im1b4 > im1b2 && im1b4 > im1b3) ? 3 : 0"

#define ANNUAL_PERMANENT_CROP_EXP     "im1b1 + im2b1 + im3b1"

// TODO: These defines shoule be extracted from config
#define FIELD_NAME          "pr_61_nb"
#define SAMPLES_VECTOR_VAL_TO_REPLACE 0
#define SAMPLES_VECTOR_REPLACING_VAL  3

QList<std::reference_wrapper<TaskToSubmit>>
S4SPermanentCropHandler::CreateTasks(QList<TaskToSubmit> &outAllTasksList)
{
    int curIdx = 0;
    outAllTasksList.append(TaskToSubmit{ "s4s-perm-crops-extract-inputs", {} });
    outAllTasksList.append(TaskToSubmit{ "s4s-perm-crops-build-refl-stack-tif", { outAllTasksList[curIdx++] } });

    outAllTasksList.append(TaskToSubmit{ "s4s-perm-crops-polygon-class-statistics", {outAllTasksList[curIdx++]} });
    outAllTasksList.append(TaskToSubmit{ "s4s-perm-crops-samples-selection", {outAllTasksList[curIdx++]} });
    outAllTasksList.append(TaskToSubmit{ "s4s-perm-crops-samples-extraction", {outAllTasksList[curIdx++]} });
    outAllTasksList.append(TaskToSubmit{ "s4s-perm-crops-samples-rasterization", {outAllTasksList[curIdx++]} });

    outAllTasksList.append(TaskToSubmit{ "s4s-perm-crops-run-broceliande", { outAllTasksList[curIdx++] } });

    int broceliandeIdx = curIdx;
    // launch the next 3 in parallel
    outAllTasksList.append(TaskToSubmit{ "s4s-perm-crops-annual-crop-extraction", { outAllTasksList[broceliandeIdx] } });
    outAllTasksList.append(TaskToSubmit{ "s4s-perm-crops-perenial-crop-extraction", { outAllTasksList[broceliandeIdx] } });
    outAllTasksList.append(TaskToSubmit{ "s4s-perm-crops-no-cropland-extraction", { outAllTasksList[broceliandeIdx] } });

    // the next 3 are launched in parallel but each one waits for the corresponding extraction task above
    curIdx += 3;
    int annualSiegeTaskIdx = curIdx+1;
    outAllTasksList.append(TaskToSubmit{ "s4s-perm-crops-sieve", { outAllTasksList[broceliandeIdx+1] } });
    outAllTasksList.append(TaskToSubmit{ "s4s-perm-crops-sieve", { outAllTasksList[broceliandeIdx+2] } });
    outAllTasksList.append(TaskToSubmit{ "s4s-perm-crops-sieve", { outAllTasksList[broceliandeIdx+3] } });

    // increment with 3 steps above
    curIdx += 3;
    outAllTasksList.append(TaskToSubmit{ "s4s-annual-perm-crop-extraction", { outAllTasksList[annualSiegeTaskIdx],
                                                                             outAllTasksList[annualSiegeTaskIdx+1],
                                                                             outAllTasksList[annualSiegeTaskIdx+2] } });
    curIdx++;
    int annualPermCropExtrIdx = curIdx++;
    outAllTasksList.append(TaskToSubmit{ "product-formatter", {outAllTasksList[annualPermCropExtrIdx]} });

    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    for (TaskToSubmit &task : outAllTasksList) {
        allTasksListRef.append(task);
    }
    return allTasksListRef;
}

NewStepList S4SPermanentCropHandler::CreateSteps(QList<TaskToSubmit> &allTasksList,
                                            const S4SPermanentCropJobConfig &cfg)
{
    int curTaskIdx = 0;
    NewStepList allSteps;
    TaskToSubmit &extractInputsTask = allTasksList[curTaskIdx++];
    TaskToSubmit &buildReflStackTifTask = allTasksList[curTaskIdx++];
    TaskToSubmit &polyClassStatsTask = allTasksList[curTaskIdx++];
    TaskToSubmit &samplesSelectionTask = allTasksList[curTaskIdx++];
    TaskToSubmit &samplesExtractionTask = allTasksList[curTaskIdx++];
    TaskToSubmit &samplesRasterizationTask = allTasksList[curTaskIdx++];
    TaskToSubmit &broceliandeTask = allTasksList[curTaskIdx++];

    TaskToSubmit &annualCropExtractionTask = allTasksList[curTaskIdx++];
    TaskToSubmit &perenialCropExtractionTask = allTasksList[curTaskIdx++];
    TaskToSubmit &noCroplandExtractionTask = allTasksList[curTaskIdx++];

    TaskToSubmit &annualCropSieveTask = allTasksList[curTaskIdx++];
    TaskToSubmit &perenialCropSieveTask = allTasksList[curTaskIdx++];
    TaskToSubmit &noCroplandSieveTask = allTasksList[curTaskIdx++];

    TaskToSubmit &annualPermCropExtractionTask = allTasksList[curTaskIdx++];
    TaskToSubmit &productFormatterTask = allTasksList[curTaskIdx++];

    // Resulting files from tasks
    const QString &extractedInputsListPath = extractInputsTask.GetFilePath("input_rasters_list.csv");
    const QString &stackBuildWorkingDirPath = buildReflStackTifTask.GetFilePath("");
    const QString &fullStackTifPath = buildReflStackTifTask.GetFilePath("reflectance_full_stack.tif");
    const QString &sampleStats = polyClassStatsTask.GetFilePath("sample_stats.xml");
    const QString &selectedUpdateSamples = samplesSelectionTask.GetFilePath("selected_update_samples.shp");
    const QString &outRates = samplesSelectionTask.GetFilePath("out_rates.csv");
    const QString &finalUpdateSamples = samplesExtractionTask.GetFilePath("resulted_update_samples.shp");
    const QString &rasterizedSamples = samplesRasterizationTask.GetFilePath("conversion_output.tif");
    const QString &broceliandeOutput = broceliandeTask.GetFilePath("broceliande_output.tif");

    const QString &annualCropExtrResult = annualCropExtractionTask.GetFilePath("annual_crop_extraction.tif");
    const QString &perenialCropExtrResult = perenialCropExtractionTask.GetFilePath("perenial_crop_extraction.tif");
    const QString &noCroplandExtrResult = noCroplandExtractionTask.GetFilePath("no_cropland_extraction.tif");

    const QString &annualCropSieveResult = annualCropSieveTask.GetFilePath("annual_crop_sieve.tif");
    const QString &perenialCropSieveResult = perenialCropSieveTask.GetFilePath("perenial_crop_sieve.tif");
    const QString &noCroplandSieveResult = noCroplandSieveTask.GetFilePath("no_cropland_sieve.tif");

    const QString &annualPermCropExtrResult = annualPermCropExtractionTask.GetFilePath("annual_permanent_crop.tif");

    // Inputs extraction and reflectances stack tif creation
    const QStringList &extractInputsArgs = GetExtractInputsTaskArgs(cfg, extractedInputsListPath);
    allSteps.append(CreateTaskStep(extractInputsTask, "ExtractInputs", extractInputsArgs));
    const QStringList &buildReflStackTifArgs = GetBuildFullStackTifTaskArgs(extractedInputsListPath, fullStackTifPath, stackBuildWorkingDirPath);
    allSteps.append(CreateTaskStep(buildReflStackTifTask, "BuildReflStackTif", buildReflStackTifArgs));

    QString fieldName = ProcessorHandlerHelper::GetStringConfigValue(cfg.parameters, cfg.configParameters, "vec_field", S4S_PERM_CROPS_CFG_PREFIX);
    if (fieldName.size() == 0) {
        fieldName = FIELD_NAME;
    }

    // Sample section
    const QStringList &polygonClassStatisticsArgs = GetPolygonClassStatisticsTaskArgs(fullStackTifPath, cfg.samplesShapePath, fieldName, sampleStats);
    allSteps.append(CreateTaskStep(polyClassStatsTask, "PolygonClassStatistics", polygonClassStatisticsArgs));

    const QStringList &sampleSelectionArgs = GetSampleSelectionTaskArgs(fullStackTifPath, cfg.samplesShapePath, fieldName, sampleStats, outRates, selectedUpdateSamples);
    allSteps.append(CreateTaskStep(samplesSelectionTask, "SampleSelection", sampleSelectionArgs));

    const QStringList &sampleExtractionArgs = GetSampleExtractionTaskArgs(fullStackTifPath, selectedUpdateSamples, fieldName, finalUpdateSamples);
    allSteps.append(CreateTaskStep(samplesExtractionTask, "SampleExtraction", sampleExtractionArgs));

    const QStringList &samplesRasterizationArgs = GetSamplesRasterizationTaskArgs(fullStackTifPath, finalUpdateSamples, fieldName,
                                                                                  SAMPLES_VECTOR_VAL_TO_REPLACE, SAMPLES_VECTOR_REPLACING_VAL,
                                                                                  rasterizedSamples);
    allSteps.append(CreateTaskStep(samplesRasterizationTask, "SamplesRasterization", samplesRasterizationArgs));

    // Broceliande
    const QStringList &broceliandeArgs = GetBroceliandeTaskArgs(cfg, broceliandeTask, fullStackTifPath, rasterizedSamples, broceliandeOutput);
    allSteps.append(CreateTaskStep(broceliandeTask, "Broceliande", broceliandeArgs));

    // Crop extractions
    const QStringList &extrInputsList = {broceliandeOutput};
    const QStringList &annualCropExtrArgs = GetCropInfosExtractionTaskArgs(extrInputsList, ANNUAL_CROP_EXP, annualCropExtrResult);
    allSteps.append(CreateTaskStep(annualCropExtractionTask, "AnnualCropExtraction", annualCropExtrArgs));
    const QStringList &perenialCropExtrArgs = GetCropInfosExtractionTaskArgs(extrInputsList, PERENIAL_CROP_EXP, perenialCropExtrResult);
    allSteps.append(CreateTaskStep(perenialCropExtractionTask, "PerenialCropExtraction", perenialCropExtrArgs));
    const QStringList &noCroplandExtrArgs = GetCropInfosExtractionTaskArgs(extrInputsList, NO_CROPLAND_EXP, noCroplandExtrResult);
    allSteps.append(CreateTaskStep(noCroplandExtractionTask, "NoCroplandExtraction", noCroplandExtrArgs));

    // Crop sieve
    const QStringList &annualCropSieveArgs = GetCropSieveTaskArgs(annualCropExtrResult, annualCropSieveResult);
    allSteps.append(CreateTaskStep(annualCropSieveTask, "AnnualCropSieve", annualCropSieveArgs));
    const QStringList &perenialCropSieveArgs = GetCropSieveTaskArgs(perenialCropExtrResult, perenialCropSieveResult);
    allSteps.append(CreateTaskStep(perenialCropSieveTask, "PerenialCropSieve", perenialCropSieveArgs));
    const QStringList &noCroplandSieveArgs = GetCropSieveTaskArgs(noCroplandExtrResult, noCroplandSieveResult);
    allSteps.append(CreateTaskStep(noCroplandSieveTask, "NoCroplandSieve", noCroplandSieveArgs));

    // Annual permanent crop extraction
    const QStringList &permCropExtrInputs = {annualCropExtrResult, perenialCropExtrResult, noCroplandExtrResult};
    const QStringList &annualPermCropExtrArgs = GetCropInfosExtractionTaskArgs(permCropExtrInputs, ANNUAL_PERMANENT_CROP_EXP, annualPermCropExtrResult);
    allSteps.append(CreateTaskStep(annualPermCropExtractionTask, "AnnualPermanentCropExtraction", annualPermCropExtrArgs));

    const QStringList &productFormatterArgs = GetProductFormatterArgs(productFormatterTask, cfg, {annualPermCropExtrResult});
    allSteps.append(CreateTaskStep(productFormatterTask, "ProductFormatter", productFormatterArgs));

    return allSteps;
}

QStringList S4SPermanentCropHandler::GetExtractInputsTaskArgs(const S4SPermanentCropJobConfig &cfg, const QString &outFile)
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

    extractParcelsArgs.append("-o");
    extractParcelsArgs.append(outFile);

    return extractParcelsArgs;
}

QStringList S4SPermanentCropHandler::GetBuildVrtTaskArgs(const QString &inputsListFile,
                                                         const QString &fullStackVrtPath,
                                                         const QString &workingDir)
{
    return {    "-i", inputsListFile,
                "-o", fullStackVrtPath,
                "-w", workingDir};
}

QStringList S4SPermanentCropHandler::GetBuildFullStackTifTaskArgs(const QString &inputFilesListPath,
                                                                  const QString &fullStackTifPath,
                                                                  const QString &workingDir)
{
    return {    "-i", inputFilesListPath,
                "-o", fullStackTifPath,
                "-w", workingDir};
}

QStringList S4SPermanentCropHandler::GetPolygonClassStatisticsTaskArgs(const QString &image, const QString &samples,
                                                                       const QString &fieldName, const QString &sampleStats)
{
    // cmd = '%s -in %s -vec %s -field pr_61_nb -out %s' % (PolygonClassStatistics, image, sample, samplestats)
    return {
        "PolygonClassStatistics",
        "-in", image,
        "-vec", samples,
        "-field", fieldName,
        "-out", sampleStats};
}

QStringList S4SPermanentCropHandler::GetSampleSelectionTaskArgs(const QString &image, const QString &samples,
                                                                const QString &fieldName, const QString &sampleStats,
                                                                const QString &outRates, const QString &selectedUpdateSamples)
{
    // cmd = '%s -in %s -vec %s -instats %s -field pr_61_nb -strategy constant -strategy.constant.nb 1000 -sampler random -outrates %s -out %s -ram 10000'
    // %(SampleSelectionCmd, image, sample, samplestats, outrates, select_updatesamples)
    return {    "SampleSelection",
                "-in", image,
                "-vec", samples,
                "-instats", sampleStats,
                "-field", fieldName,
                "-strategy", "constant",
                "-strategy.constant.nb", "1000",
                "-sampler", "random",
                "-outrates", outRates,
                "-out", selectedUpdateSamples,
                "-ram", "10000"};
}

QStringList S4SPermanentCropHandler::GetSampleExtractionTaskArgs(const QString &image, const QString &selectedUpdateSamples,
                                                                 const QString &fieldName, const QString &finalUpdateSamples)
{
    // cmd = '%s -in %s -vec %s -field pr_61_nb -out %s -ram 10000' %(SampleExtraction, image, select_updatesamples, final_updatesamples)
    return {    "SampleExtraction",
                "-in", image,
                "-vec", selectedUpdateSamples,
                "-field", fieldName,
                "-out", finalUpdateSamples,
                "-ram", "10000"
    };
}

QStringList S4SPermanentCropHandler::GetSamplesRasterizationTaskArgs(const QString &reflStackTif,
                                                                     const QString &finalUpdateSamples,
                                                                     const QString &fieldName, int valToReplace,
                                                                     int replacingValue, const QString &outputFile)
{
    return {    "--image", reflStackTif,
                "--vec", finalUpdateSamples,
                "--output", outputFile,
                "--field", fieldName,
                "--val-to-replace", QString::number(valToReplace),
                "--replacing-val", QString::number(replacingValue)
    };
}

QStringList S4SPermanentCropHandler::GetBroceliandeTaskArgs(const S4SPermanentCropJobConfig &cfg,
                                                            const TaskToSubmit &task,
                                                            const QString &image,
                                                            const QString &samples,
                                                            const QString &output)
{
    QStringList args = {    "-i", image,
                            "-s", samples,
                            "-o", output,
                            "--docker-mounts",
    };
    const QStringList &dockerMounts = StepExecutionDecorator::GetInstance()->GetDockerMounts(this->processorDescr.shortName, task.moduleName);
    for (const QString &mount: dockerMounts) {
        args.append(mount);
    }
    const QString &dockerImg = ProcessorHandlerHelper::GetStringConfigValue(cfg.parameters, cfg.configParameters,
                                                                            "broceliande-docker-image", S4S_PERM_CROPS_CFG_PREFIX);
    if (dockerImg.size() == 0) {
        throw std::runtime_error(
            QStringLiteral("Broceliande image not configured in the database for key %1%2")
                    .arg(S4S_PERM_CROPS_CFG_PREFIX).arg("broceliande-docker-img").toStdString());
    }
    args.append("--docker-image");
    args.append(dockerImg);

    return args;
}

QStringList S4SPermanentCropHandler::GetCropInfosExtractionTaskArgs(const QStringList &imgs, const QString &exp, const QString &out)
{
    QStringList args = { "BandMathX",  "-out", out,
                         "-ram", "5000",
                        "-exp", "\"" + exp + "\"",
                        "-il"
    };
    for(const QString &img: imgs) {
        args.append(img);
    }
    return args;
}

QStringList S4SPermanentCropHandler::GetCropSieveTaskArgs(const QString &annualCrop,
                                                         const QString &annualSieve)
{
    return {    "-st", QString::number(25), "-8",
                annualCrop,
                "-of", "GTiff",
                annualSieve
    };
}

bool S4SPermanentCropHandler::GetStartEndDatesFromProducts(EventProcessingContext &ctx,
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

void S4SPermanentCropHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                                const JobSubmittedEvent &event)
{
    S4SPermanentCropJobConfig cfg(&ctx, event);
    UpdateJobConfigParameters(cfg);

    QList<TaskToSubmit> allTasksList;
    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef = CreateTasks(allTasksList);
    SubmitTasks(ctx, cfg.event.jobId, allTasksListRef);
    NewStepList allSteps = CreateSteps(allTasksList, cfg);
    ctx.SubmitSteps(allSteps);
}

void S4SPermanentCropHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                                const TaskFinishedEvent &event)
{
    if (event.module == "product-formatter") {
        const QString &prodName = GetOutputProductName(ctx, event);
        const QString &productFolder =
            GetFinalProductFolder(ctx, event.jobId, event.siteId) + "/" + prodName;
        if (prodName != "") {
            const QString &quicklook = GetProductFormatterQuicklook(ctx, event);
            const QString &footPrint = GetProductFormatterFootprint(ctx, event);
            // Insert the product into the database
            GenericHighLevelProductHelper prdHelper(productFolder);
            int prdId = ctx.InsertProduct({ ProductType::S4SPermCropsProductTypeId, event.processorId,
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
            ctx.MarkJobFinished(event.jobId);
            // Now remove the job folder containing temporary files
            RemoveJobFolder(ctx, event.jobId, processorDescr.shortName);
        } else {
            ctx.MarkJobFailed(event.jobId);
            Logger::error(
                QStringLiteral("Cannot insert into database the product with name %1 and folder %2")
                    .arg(prodName)
                    .arg(productFolder));
        }
    }
}

ProcessorJobDefinitionParams S4SPermanentCropHandler::GetProcessingDefinitionImpl(
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
        Logger::debug(QStringLiteral("Scheduler Permanent Crops: Error getting season start dates for "
                                     "site %1 for scheduled date %2!")
                          .arg(siteId)
                          .arg(qScheduledDate.toString()));
        return params;
    }

    QDateTime limitDate = seasonEndDate.addMonths(2);
    if (qScheduledDate > limitDate) {
        Logger::debug(QStringLiteral("Scheduler Permanent Crops: Error scheduled date %1 greater than the "
                                     "limit date %2 for site %3!")
                          .arg(qScheduledDate.toString())
                          .arg(limitDate.toString())
                          .arg(siteId));
        return params;
    }

    ConfigurationParameterValueMap cfgValues =
        ctx.GetConfigurationParameters("processor.s4s_perm_crop.", siteId, requestOverrideCfgValues);
    // we might have an offset in days from starting the downloading products to start the S4C L4A
    // production
    int startSeasonOffset = cfgValues["processor.s4s_perm_crop.start_season_offset"].value.toInt();
    seasonStartDate = seasonStartDate.addDays(startSeasonOffset);

    QDateTime startDate = seasonStartDate;
    QDateTime endDate = qScheduledDate;
    // do not pass anymore the product list but the dates
    params.jsonParameters.append("{ \"scheduled_job\": \"1\", \"start_date\": \"" + startDate.toString("yyyyMMdd") + "\", " +
                                 "\"end_date\": \"" + endDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_start_date\": \"" + seasonStartDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_end_date\": \"" + seasonEndDate.toString("yyyyMMdd") + "\"}");

    // Normally, we need at least 1 product available, the crop mask and the shapefile in order to
    // be able to create a S4C Permanent Crops product but if we do not return here, the schedule block waiting
    // for products (that might never happen)
    bool waitForAvailProcInputs =
        (cfgValues["processor.s4s_perm_crop.sched_wait_proc_inputs"].value.toInt() != 0);
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
        Logger::debug(QStringLiteral("Scheduled job for S4S Permanent Crops and site ID %1 with start date %2 "
                                     "and end date %3 will not be executed "
                                     "(productsNo = %4)!")
                          .arg(siteId)
                          .arg(startDate.toString())
                          .arg(endDate.toString())
                          .arg(params.productList.size()));
    }

    return params;
}


void S4SPermanentCropHandler::UpdateJobConfigParameters(S4SPermanentCropJobConfig &cfgToUpdate)
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
    cfgToUpdate.year = cfgToUpdate.endDate.date().year();           // TODO: see if this is valid
    const QString &samplesFile = ExtractSamplesInfos(cfgToUpdate);
    if (samplesFile.size() == 0) {
        // try to get the start and end date if they are given
        cfgToUpdate.pCtx->MarkJobFailed(cfgToUpdate.event.jobId);
        throw std::runtime_error(
            QStringLiteral(
                "Cannot extract the gpkg file for the samples infos")
                .toStdString());
    }

    cfgToUpdate.SetSamplesInfosProducts(samplesFile);
}

//TODO: We should return here only one LPIS product according to the year
QString S4SPermanentCropHandler::ExtractSamplesInfos(const S4SPermanentCropJobConfig &cfg) {
    // We take it the last LPIS product for this site.
    const ProductList &samplesPrds = S4CUtils::GetLpisProduct(cfg.pCtx, cfg.event.siteId);
    if (samplesPrds.size() == 0) {
        cfg.pCtx->MarkJobFailed(cfg.event.jobId);
        throw std::runtime_error(QStringLiteral("No LPIS product found in database for the permanent crops execution for site %1.").
                                 arg(cfg.siteShortName).toStdString());
    }

    QDateTime lastPrdInsertedDate;
    QString retSampleFile;
    for(const Product &samplesPrd: samplesPrds) {
        int insituPrdYear = samplesPrd.created.date().year();
        if (insituPrdYear != cfg.year) {
            continue;
        }
        // ignore LPIS products from a year where we already added an LPIS product newer
        if (lastPrdInsertedDate.isValid() && samplesPrd.inserted < lastPrdInsertedDate) {
            continue;
        }

        QDir directory(samplesPrd.fullPath);
        QRegularExpression reGpkg("in_?situ_.*_\\d{4}.gpkg");
        const QStringList &dirFiles = directory.entryList(QStringList() << "*.gpkg",QDir::Files);
        foreach(const QString &fileName, dirFiles) {
            if (reGpkg.match(fileName).hasMatch())  {
                retSampleFile = directory.filePath(fileName);
                break;
            }
        }
    }

    return retSampleFile;
}


QStringList S4SPermanentCropHandler::GetTileIdsFromProducts(EventProcessingContext &ctx,
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

bool S4SPermanentCropHandler::IsScheduledJobRequest(const QJsonObject &parameters) {
    int jobVal;
    return ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "scheduled_job", jobVal) && (jobVal == 1);
}

QStringList S4SPermanentCropHandler::GetProductFormatterArgs(TaskToSubmit &productFormatterTask,
                                                            const S4SPermanentCropJobConfig &cfg, const QStringList &listFiles) {
    // ProductFormatter /home/cudroiu/sen2agri-processors-build
    //    -vectprd 1 -destroot /mnt/archive_new/test/Sen4CAP_L4B_Tests/NLD_Validation_TSA/OutPrdFormatter
    //    -fileclass OPER -level S4C_L4B -baseline 01.00 -siteid 4 -timeperiod 20180101_20181231 -processor generic
    //    -processor.generic.files <files_list>

    const auto &targetFolder = GetFinalProductFolder(*(cfg.pCtx), cfg.event.jobId, cfg.event.siteId);
    const auto &outPropsPath = productFormatterTask.GetFilePath(PRODUCT_FORMATTER_OUT_PROPS_FILE);
    const auto &executionInfosPath = productFormatterTask.GetFilePath("executionInfos.txt");
    QString strTimePeriod = cfg.startDate.toString("yyyyMMddTHHmmss").append("_").append(cfg.endDate.toString("yyyyMMddTHHmmss"));
    QStringList productFormatterArgs = { "ProductFormatter",
                                         "-destroot", targetFolder,
                                         "-fileclass", "OPER",
                                         "-level", "S4S_PERMC",
                                         "-vectprd", "0",
                                         "-baseline", "01.00",
                                         "-siteid", QString::number(cfg.event.siteId),
                                         "-timeperiod", strTimePeriod,
                                         "-processor", "generic",
                                         "-outprops", outPropsPath,
                                         "-gipp", executionInfosPath
                                       };
    productFormatterArgs += "-processor.generic.files";
    productFormatterArgs += listFiles;

    return productFormatterArgs;
}

