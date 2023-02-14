#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "compositehandler.hpp"
#include "processorhandlerhelper.h"
#include "json_conversions.hpp"
#include "logger.hpp"

#include "products/generichighlevelproducthelper.h"
#include "products/producthelperfactory.h"

using namespace orchestrator::products;

#define DEFAULT_HALF_SYNTHESIS      15

void CompositeHandler::CreateTasksForNewProducts(const CompositeJobConfig &cfg, QList<TaskToSubmit> &outAllTasksList,
                                                QList<std::reference_wrapper<const TaskToSubmit>> &outProdFormatterParentsList,
                                                const TileTimeSeriesInfo &tileTemporalFilesInfo)
{
    int nbProducts = tileTemporalFilesInfo.temporalTilesFileInfos.size();
    // just create the tasks but with no information so far
    for (int i = 0; i < nbProducts; i++) {
        outAllTasksList.append(TaskToSubmit{ "composite-mask-handler", {} });
        outAllTasksList.append(TaskToSubmit{ "composite-preprocessing", {} });

        outAllTasksList.append(TaskToSubmit{ "composite-weight-aot", {} });
        outAllTasksList.append(TaskToSubmit{ "composite-weight-on-clouds", {} });
        outAllTasksList.append(TaskToSubmit{ "composite-total-weight", {} });
        outAllTasksList.append(TaskToSubmit{ "composite-update-synthesis", {} });
        outAllTasksList.append(TaskToSubmit{ "composite-splitter", {} });
        if(!cfg.keepJobFiles) {
            outAllTasksList.append(TaskToSubmit{ "files-remover", {} });
        }
    }

    // now fill the tasks hierarchy infos
    int i;
    int nCurTaskIdx = 0;

    for (i = 0; i < nbProducts; i++) {
        if (i > 0) {
            // update the mask handler with the reference of the previous composite splitter
            outAllTasksList[nCurTaskIdx].parentTasks.append(outAllTasksList[nCurTaskIdx-1]);
        }
        nCurTaskIdx++;
        // the others comme naturally updated
        // composite-preprocessing -> mask-handler
        outAllTasksList[nCurTaskIdx].parentTasks.append(outAllTasksList[nCurTaskIdx-1]);
        nCurTaskIdx++;

        // weigh-aot -> composite-preprocessing
        outAllTasksList[nCurTaskIdx].parentTasks.append(outAllTasksList[nCurTaskIdx-1]);
        nCurTaskIdx++;
        // weigh-on-clouds -> composite-preprocessing
        outAllTasksList[nCurTaskIdx].parentTasks.append(outAllTasksList[nCurTaskIdx-2]);
        nCurTaskIdx++;
        // total-weight -> weigh-aot and weigh-on-clouds
        outAllTasksList[nCurTaskIdx].parentTasks.append(outAllTasksList[nCurTaskIdx-1]);
        outAllTasksList[nCurTaskIdx].parentTasks.append(outAllTasksList[nCurTaskIdx-2]);
        nCurTaskIdx++;
        // update-synthesis -> total-weight
        outAllTasksList[nCurTaskIdx].parentTasks.append(outAllTasksList[nCurTaskIdx-1]);
        nCurTaskIdx++;

        // composite-splitter -> update-synthesis
        outAllTasksList[nCurTaskIdx].parentTasks.append(outAllTasksList[nCurTaskIdx-1]);
        nCurTaskIdx++;
        if(!cfg.keepJobFiles) {
            // cleanup-intermediate-files -> composite-splitter
            outAllTasksList[nCurTaskIdx].parentTasks.append(outAllTasksList[nCurTaskIdx-1]);
            nCurTaskIdx++;
        }

    }
    // product-formatter -> the last composite-splitter
    outProdFormatterParentsList.append(outAllTasksList[outAllTasksList.size() - 1]);
}

void CompositeHandler::HandleNewTilesList(EventProcessingContext &ctx,
                                          const CompositeJobConfig &cfg,
                                          const TileTimeSeriesInfo &tileTemporalFilesInfo,
                                          CompositeGlobalExecutionInfos &globalExecInfos,
                                          int resolution)
{
    const QList<TileMetadataDetails> &listProducts = tileTemporalFilesInfo.GetTileTimeSeriesInfoFiles();
    QList<ProductDetails> prdDetails;
    std::for_each(listProducts.begin(), listProducts.end(), [&prdDetails](const TileMetadataDetails &prd) {
        prdDetails.append(prd.srcPrdDetails);
    });

    QString bandsMapping = DeductBandsMappingFile(prdDetails, cfg.bandsMapping, resolution);
    const auto &resolutionStr = QString::number(resolution);
    QString scatCoeffs = ((resolution == 10) ? cfg.scatCoeffs10M : cfg.scatCoeffs20M);

    QList<TaskToSubmit> &allTasksList = globalExecInfos.allTasksList;
    QList<std::reference_wrapper<const TaskToSubmit>> &prodFormParTsksList = globalExecInfos.prodFormatParams.parentsTasksRef;
    CreateTasksForNewProducts(cfg, allTasksList, prodFormParTsksList, tileTemporalFilesInfo);

    NewStepList &steps = globalExecInfos.allStepsList;
    int nCurTaskIdx = 0;

    QString primaryTileMetadata;
    for(int i = 0; i<tileTemporalFilesInfo.temporalTilesFileInfos.size(); i++) {
        const InfoTileFile &tempFileInfo = tileTemporalFilesInfo.temporalTilesFileInfos[i];
        if(tempFileInfo.satId == tileTemporalFilesInfo.primarySatelliteId) {
            primaryTileMetadata = tempFileInfo.metaFile;
            break;
        }
    }
    QString prevL3AProdRefls;
    QString prevL3AProdWeights;
    QString prevL3AProdFlags;
    QString prevL3AProdDates;
    QString prevL3ARgbFile;
    for (int i = 0; i < listProducts.size(); i++) {
        QStringList cleanupTemporaryFilesList;// = {"-f"};
        const auto &inputProduct = listProducts[i];
        // Mask Handler Step
        TaskToSubmit &maskHandler = allTasksList[nCurTaskIdx++];
        SubmitTasks(ctx, cfg.jobId, {maskHandler});
        const auto &masksFile = maskHandler.GetFilePath("all_masks_file.tif");
        QStringList maskHandlerArgs = { "MaskHandler", "-xml",         inputProduct.tileMetaFile, "-out",
                                        masksFile,     "-sentinelres", resolutionStr };
        if (inputProduct.srcPrdDetails.GetProduct().productTypeId == ProductType::MaskedL2AProductTypeId) {
            const auto &inExtMask = GetL2AExternalMask(inputProduct.srcPrdDetails);
            if (inExtMask != "") {
                maskHandlerArgs.append("-extmsk");
                maskHandlerArgs.append(inExtMask);
            }
        }
        steps.append(CreateTaskStep(maskHandler, "MaskHandler", maskHandlerArgs));
        cleanupTemporaryFilesList.append(masksFile);

        TaskToSubmit &compositePreprocessing = allTasksList[nCurTaskIdx++];
        SubmitTasks(ctx, cfg.jobId, {compositePreprocessing});
        // Composite preprocessing Step
        auto outResImgBands = compositePreprocessing.GetFilePath("img_res_bands.tif");
        auto cldResImg = compositePreprocessing.GetFilePath("cld_res.tif");
        auto waterResImg = compositePreprocessing.GetFilePath("water_res.tif");
        auto snowResImg = compositePreprocessing.GetFilePath("snow_res.tif");
        auto aotResImg = compositePreprocessing.GetFilePath("aot_res.tif");
        // TODO: Provide here also via a parameter also the validity masks (if available instead of MaskHandler mask)
        QStringList compositePreprocessingArgs = { "CompositePreprocessing", "-xml", inputProduct.tileMetaFile,
                                                   "-bmap", bandsMapping, "-res", resolutionStr,
                                                   "-msk", masksFile, "-outres", outResImgBands,
                                                   "-outcmres", cldResImg, "-outwmres", waterResImg,
                                                   "-outsmres", snowResImg, "-outaotres", aotResImg };

        cleanupTemporaryFilesList.append(outResImgBands);
        cleanupTemporaryFilesList.append(cldResImg);
        cleanupTemporaryFilesList.append(waterResImg);
        cleanupTemporaryFilesList.append(snowResImg);
        cleanupTemporaryFilesList.append(aotResImg);

        if(scatCoeffs.length() > 0) {
            compositePreprocessingArgs.append("-scatcoef");
            compositePreprocessingArgs.append(scatCoeffs);
        }
        if((primaryTileMetadata != "") &&
           (tileTemporalFilesInfo.temporalTilesFileInfos[i].satId != tileTemporalFilesInfo.primarySatelliteId)) {
            compositePreprocessingArgs.append("-pmxml");
            compositePreprocessingArgs.append(primaryTileMetadata);
        }
        steps.append(CreateTaskStep(compositePreprocessing, "CompositePreprocessing", compositePreprocessingArgs));

        TaskToSubmit &weightAot = allTasksList[nCurTaskIdx++];
        SubmitTasks(ctx, cfg.jobId, {weightAot});
        TaskToSubmit &weightOnClouds = allTasksList[nCurTaskIdx++];
        SubmitTasks(ctx, cfg.jobId, {weightOnClouds});
        TaskToSubmit &totalWeight = allTasksList[nCurTaskIdx++];
        SubmitTasks(ctx, cfg.jobId, {totalWeight});
        TaskToSubmit &updateSynthesis = allTasksList[nCurTaskIdx++];
        SubmitTasks(ctx, cfg.jobId, {updateSynthesis});
        TaskToSubmit &compositeSplitter = allTasksList[nCurTaskIdx++];
        SubmitTasks(ctx, cfg.jobId, {compositeSplitter});

        // Weight AOT Step
        const auto &outWeightAotFile = weightAot.GetFilePath("weight_aot.tif");
        QStringList weightAotArgs = { "WeightAOT",     "-xml",     inputProduct.tileMetaFile, "-in",
                                      aotResImg,       "-waotmin", cfg.weightAOTMin, "-waotmax",
                                      cfg.weightAOTMax,    "-aotmax",  cfg.AOTMax,       "-out",
                                      outWeightAotFile };
        steps.append(CreateTaskStep(weightAot, "WeightAOT", weightAotArgs));
        cleanupTemporaryFilesList.append(outWeightAotFile);

        // Weight on clouds Step
        const auto &outWeightCldFile = weightOnClouds.GetFilePath("weight_cloud.tif");
        QStringList weightOnCloudArgs = { "WeightOnClouds", "-inxml",         inputProduct.tileMetaFile,
                                          "-incldmsk",      cldResImg,        "-coarseres",
                                          cfg.coarseRes,        "-sigmasmallcld", cfg.sigmaSmallCloud,
                                          "-sigmalargecld", cfg.sigmaLargeCloud,  "-out", outWeightCldFile };
        steps.append(CreateTaskStep(weightOnClouds, "WeightOnClouds", weightOnCloudArgs));
        cleanupTemporaryFilesList.append(outWeightCldFile);

        // Total weight Step
        const auto &outTotalWeighFile = totalWeight.GetFilePath("weight_total.tif");
        QStringList totalWeightArgs = { "TotalWeight",    "-xml",           inputProduct.tileMetaFile,
                                        "-waotfile",      outWeightAotFile, "-wcldfile",
                                        outWeightCldFile, "-l3adate",       cfg.l3aSynthesisDate,
                                        "-halfsynthesis", cfg.synthalf,         "-wdatemin",
                                        cfg.weightDateMin,    "-out",           outTotalWeighFile };
        steps.append(CreateTaskStep(totalWeight, "TotalWeight", totalWeightArgs));
        cleanupTemporaryFilesList.append(outTotalWeighFile);

        // Update Synthesis Step
        const auto &outL3AResultFile = updateSynthesis.GetFilePath("L3AResult.tif");
        QStringList updateSynthesisArgs = { "UpdateSynthesis", "-in",   outResImgBands,    "-bmap",
                                            bandsMapping,      "-xml",  inputProduct.tileMetaFile,
                                            "-csm", cldResImg,         "-wm",   waterResImg,
                                            "-sm", snowResImg,        "-wl2a", outTotalWeighFile,
                                            "-out", outL3AResultFile };
        if (i > 0) {
            updateSynthesisArgs.append("-prevl3aw");
            updateSynthesisArgs.append(prevL3AProdWeights);
            updateSynthesisArgs.append("-prevl3ad");
            updateSynthesisArgs.append(prevL3AProdDates);
            updateSynthesisArgs.append("-prevl3ar");
            updateSynthesisArgs.append(prevL3AProdRefls);
            updateSynthesisArgs.append("-prevl3af");
            updateSynthesisArgs.append(prevL3AProdFlags);
            // remove at this step the previous files
            cleanupTemporaryFilesList.append(prevL3AProdWeights);
            cleanupTemporaryFilesList.append(prevL3AProdDates);
            cleanupTemporaryFilesList.append(prevL3AProdRefls);
            cleanupTemporaryFilesList.append(prevL3AProdFlags);
            // normally this will not be created in intermedaiate steps
            //cleanupTemporaryFilesList.append(prevL3ARgbFile);
        }
        steps.append(CreateTaskStep(updateSynthesis, "UpdateSynthesis", updateSynthesisArgs));
        cleanupTemporaryFilesList.append(outL3AResultFile);

        // Composite Splitter Step
        const auto &outL3AResultReflsFile = compositeSplitter.GetFilePath("L3AResult_refls.tif");
        const auto &outL3AResultWeightsFile = compositeSplitter.GetFilePath("L3AResult_weights.tif");
        const auto &outL3AResultFlagsFile = compositeSplitter.GetFilePath("L3AResult_flags.tif");
        const auto &outL3AResultDatesFile = compositeSplitter.GetFilePath("L3AResult_dates.tif");
        const auto &outL3AResultRgbFile = compositeSplitter.GetFilePath("L3AResult_rgb.tif");
        bool isLastProduct = (i == (listProducts.size() - 1));
        QStringList compositeSplitterArgs = { "CompositeSplitter2",
                                              "-in", outL3AResultFile, "-xml", inputProduct.tileMetaFile, "-bmap", bandsMapping,
                                              "-outweights", outL3AResultWeightsFile,
                                              "-outdates", outL3AResultDatesFile,
                                              "-outrefls", outL3AResultReflsFile,
                                              "-outflags", outL3AResultFlagsFile,
                                              "-isfinal", (isLastProduct ? "1" : "0")
                                            };
        // we need to create the rgb file only if the last product
        if(isLastProduct) {
            compositeSplitterArgs.append("-outrgb");
            compositeSplitterArgs.append(outL3AResultRgbFile);
        }
        steps.append(CreateTaskStep(compositeSplitter, "CompositeSplitter", compositeSplitterArgs));

        // save the created L3A product file for the next product creation
        prevL3AProdRefls = outL3AResultReflsFile;
        prevL3AProdWeights = outL3AResultWeightsFile;
        prevL3AProdFlags = outL3AResultFlagsFile;
        prevL3AProdDates = outL3AResultDatesFile;
        prevL3ARgbFile = outL3AResultRgbFile;

        if(!cfg.keepJobFiles) {
            TaskToSubmit &cleanupTemporaryFiles = allTasksList[nCurTaskIdx++];
            SubmitTasks(ctx, cfg.jobId, {cleanupTemporaryFiles});
            steps.append(CreateTaskStep(cleanupTemporaryFiles, "CleanupTemporaryFiles", cleanupTemporaryFilesList));
        }
    }

    CompositeProductFormatterParams &productFormatterParams = globalExecInfos.prodFormatParams;
    productFormatterParams.prevL3AProdRefls = prevL3AProdRefls;
    productFormatterParams.prevL3AProdWeights = prevL3AProdWeights;
    productFormatterParams.prevL3AProdFlags = prevL3AProdFlags;
    productFormatterParams.prevL3AProdDates = prevL3AProdDates;
    productFormatterParams.prevL3ARgbFile = prevL3ARgbFile;
}

void CompositeHandler::WriteExecutionInfosFile(const QString &executionInfosPath,
                                               const CompositeJobConfig &cfg,
                                               const QList<ProductDetails> &productDetails)
{
    std::ofstream executionInfosFile;
    try {
        executionInfosFile.open(executionInfosPath.toStdString().c_str(), std::ofstream::out);
        executionInfosFile << "<?xml version=\"1.0\" ?>" << std::endl;
        executionInfosFile << "<metadata>" << std::endl;
        executionInfosFile << "  <General>" << std::endl;
        int resolution = 10;

        QString bandsMapping = DeductBandsMappingFile(productDetails, cfg.bandsMapping, resolution);
        executionInfosFile << "    <bands_mapping_file>" << bandsMapping.toStdString()
                           << "</bands_mapping_file>" << std::endl;
        executionInfosFile << "    <scattering_coefficients_10M_file>" << cfg.scatCoeffs10M.toStdString()
                           << "</scattering_coefficients_10M_file>" << std::endl;
        executionInfosFile << "    <scattering_coefficients_20M_file>" << cfg.scatCoeffs20M.toStdString()
                           << "</scattering_coefficients_20M_file>" << std::endl;
        executionInfosFile << "  </General>" << std::endl;

        executionInfosFile << "  <Weight_AOT>" << std::endl;
        executionInfosFile << "    <weight_aot_min>" << cfg.weightAOTMin.toStdString()
                           << "</weight_aot_min>" << std::endl;
        executionInfosFile << "    <weight_aot_max>" << cfg.weightAOTMax.toStdString()
                           << "</weight_aot_max>" << std::endl;
        executionInfosFile << "    <aot_max>" << cfg.AOTMax.toStdString() << "</aot_max>" << std::endl;
        executionInfosFile << "  </Weight_AOT>" << std::endl;

        executionInfosFile << "  <Weight_On_Clouds>" << std::endl;
        executionInfosFile << "    <coarse_res>" << cfg.coarseRes.toStdString() << "</coarse_res>"
                           << std::endl;
        executionInfosFile << "    <sigma_small_cloud>" << cfg.sigmaSmallCloud.toStdString()
                           << "</sigma_small_cloud>" << std::endl;
        executionInfosFile << "    <sigma_large_cloud>" << cfg.sigmaLargeCloud.toStdString()
                           << "</sigma_large_cloud>" << std::endl;
        executionInfosFile << "  </Weight_On_Clouds>" << std::endl;

        executionInfosFile << "  <Weight_On_Date>" << std::endl;
        executionInfosFile << "    <weight_date_min>" << cfg.weightDateMin.toStdString()
                           << "</weight_date_min>" << std::endl;
        executionInfosFile << "    <l3a_product_date>" << cfg.l3aSynthesisDate.toStdString()
                           << "</l3a_product_date>" << std::endl;
        executionInfosFile << "    <half_synthesis>" << cfg.synthalf.toStdString()
                           << "</half_synthesis>" << std::endl;
        executionInfosFile << "  </Weight_On_Date>" << std::endl;

        executionInfosFile << "  <Dates_information>" << std::endl;
        // TODO: We should get these infos somehow but without parsing here anything
        // executionInfosFile << "    <start_date>" << 2013031 << "</start_date>" << std::endl;
        // executionInfosFile << "    <end_date>" << 20130422 << "</end_date>" << std::endl;
        executionInfosFile << "    <synthesis_date>" << cfg.l3aSynthesisDate.toStdString()
                           << "</synthesis_date>" << std::endl;
        executionInfosFile << "    <synthesis_half>" << cfg.synthalf.toStdString()
                           << "</synthesis_half>" << std::endl;
        executionInfosFile << "  </Dates_information>" << std::endl;

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

// This function removes the files from the list that are outside the synthesis interval
// and that should not be used in the composition
void CompositeHandler::FilterInputProducts(QStringList &listFiles,
                                           int productDate,
                                           int halfSynthesis)
{
    // TODO: we should extract here the date of the product to compare it with tha synthesis
    // interval
    Q_UNUSED(listFiles);
    Q_UNUSED(productDate);
    Q_UNUSED(halfSynthesis);
}

void CompositeHandler::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                              const JobSubmittedEvent &event)
{
    const auto &parameters = QJsonDocument::fromJson(event.parametersJson.toUtf8()).object();
    const ProductList &prds = GetInputProducts(ctx, parameters, event.siteId);
    const QList<ProductDetails> &prdDetails = ProcessorHandlerHelper::GetProductDetails(prds, ctx);
    if(prdDetails.size() == 0) {
        ctx.MarkJobFailed(event.jobId);
        throw std::runtime_error(
            QStringLiteral("No products provided at input or no products available in the specified interval").
                    toStdString());
    }

    CompositeJobConfig cfg;
    GetJobConfig(ctx, event, cfg);

    int resolution = cfg.resolution;
    const TilesTimeSeries &mapTiles = GroupL2ATiles(ctx, prdDetails);

    QList<CompositeProductFormatterParams> listParams;

    TaskToSubmit productFormatterTask{"product-formatter", {}};
    NewStepList allSteps;
    QList<CompositeGlobalExecutionInfos> listCompositeInfos;
    for(const auto &tileId : mapTiles.GetTileIds())
    {
        const TileTimeSeriesInfo &listTemporalTiles = mapTiles.GetTileTimeSeriesInfo(tileId);
        int curRes = resolution;
        bool bHasS2 = listTemporalTiles.uniqueSatteliteIds.contains(Satellite::Sentinel2);
        // if we have S2 maybe we want to create products only for 20m resolution

        for(int i = 0; i<2; i++)  {
            if(i == 0) {
                if(bHasS2 && resolution != 20) { curRes = 10;}  // if S2 and resolution not 20m, force it to 10m
            } else {
                // if we are at the second iteration, check if we have S2, if we should generate for 20m and if we had previously 10m
                if(cfg.bGenerate20MS2Res && bHasS2 && resolution != 20) { curRes = 20;}
                else { break;}  // exit the loop and do not execute anymore the second step
            }
            listCompositeInfos.append(CompositeGlobalExecutionInfos());
            CompositeGlobalExecutionInfos &infos = listCompositeInfos[listCompositeInfos.size()-1];
            infos.prodFormatParams.tileId = GetProductFormatterTile(tileId);
            HandleNewTilesList(ctx, cfg, listTemporalTiles, infos, curRes);
            listParams.append(infos.prodFormatParams);
            productFormatterTask.parentTasks += infos.prodFormatParams.parentsTasksRef;
            allSteps.append(infos.allStepsList);
        }
    }

    SubmitTasks(ctx, event.jobId, {productFormatterTask});

    // finally format the product
    const QStringList &productFormatterArgs = GetProductFormatterArgs(productFormatterTask, ctx, cfg, prdDetails, listParams);

    // add these steps to the steps list to be submitted
    allSteps.append(CreateTaskStep(productFormatterTask, "ProductFormatter", productFormatterArgs));
    ctx.SubmitSteps(allSteps);
}

void CompositeHandler::HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                              const TaskFinishedEvent &event)
{
    if (event.module == "product-formatter") {
        const QString &prodName = GetOutputProductName(ctx, event);
        const QString &productFolder = GetFinalProductFolder(ctx, event.jobId, event.siteId) + "/" + prodName;
        if(prodName != "" && GenericHighLevelProductHelper(productFolder).HasValidStructure()) {
            // mark the job as finished
            ctx.MarkJobFinished(event.jobId);

            const QString &quicklook = GetProductFormatterQuicklook(ctx, event);
            const QString &footPrint = GetProductFormatterFootprint(ctx, event);
            // Insert the product into the database
            GenericHighLevelProductHelper prdHelper(productFolder);
            const QStringList &prodTiles = prdHelper.GetTileIdsFromProduct();
            ctx.InsertProduct({ ProductType::L3AProductTypeId, event.processorId, event.siteId,
                                event.jobId, productFolder, prdHelper.GetAcqDate(), prodName, quicklook,
                                footPrint, std::experimental::nullopt, prodTiles, ProductIdsList() });
        } else {
            // mark the job as failed
            ctx.MarkJobFailed(event.jobId);
            Logger::error(QStringLiteral("Cannot insert into database the product with name %1 and folder %2").arg(prodName).arg(productFolder));
        }
        // Now remove the job folder containing temporary files
        RemoveJobFolder(ctx, event.jobId, processorDescr.shortName);
    }
}

void CompositeHandler::GetJobConfig(EventProcessingContext &ctx,const JobSubmittedEvent &event,CompositeJobConfig &cfg) {
    cfg.allCfgMap = ctx.GetJobConfigurationParameters(event.jobId, "processor.l3a.");
    auto execProcConfigParameters = ctx.GetJobConfigurationParameters(event.jobId, "executor.processor.l3a.keep_job_folders");
    //auto resourceParameters = ctx.GetJobConfigurationParameters(event.jobId, "resources.working-mem");
    const auto &parameters = QJsonDocument::fromJson(event.parametersJson.toUtf8()).object();

    cfg.jobId = event.jobId;
    cfg.siteId = event.siteId;
    cfg.resolution = 0;
    if(!ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "resolution", cfg.resolution) ||
            cfg.resolution == 0) {
        cfg.resolution = 10;
    }

    cfg.bGenerate20MS2Res = ProcessorHandlerHelper::GetBoolConfigValue(parameters, cfg.allCfgMap, "generate_20m_s2_resolution", "processor.l3a.");
    cfg.l3aSynthesisDate = ProcessorHandlerHelper::GetStringConfigValue(parameters, cfg.allCfgMap, "synthesis_date", "processor.l3a.");
    cfg.synthalf = ProcessorHandlerHelper::GetStringConfigValue(parameters, cfg.allCfgMap, "half_synthesis", "processor.l3a.");

    // Get the parameters from the configuration
    // Get the Half Synthesis interval value if it was not specified by the user
    if(cfg.synthalf.length() == 0) {
        cfg.synthalf = cfg.allCfgMap["processor.l3a.half_synthesis"];
        if(cfg.synthalf.length() == 0) {
            cfg.synthalf = "25";
        }
    }
    cfg.lutPath = cfg.allCfgMap["processor.l3a.lut_path"];

    cfg.bandsMapping = cfg.allCfgMap["processor.l3a.bandsmapping"];
    cfg.scatCoeffs10M = cfg.allCfgMap["processor.l3a.preproc.scatcoeffs_10m"];
    cfg.scatCoeffs20M = cfg.allCfgMap["processor.l3a.preproc.scatcoeffs_20m"];
    cfg.weightAOTMin = cfg.allCfgMap["processor.l3a.weight.aot.minweight"];
    cfg.weightAOTMax = cfg.allCfgMap["processor.l3a.weight.aot.maxweight"];
    cfg.AOTMax = cfg.allCfgMap["processor.l3a.weight.aot.maxaot"];
    cfg.coarseRes = cfg.allCfgMap["processor.l3a.weight.cloud.coarseresolution"];
    cfg.sigmaSmallCloud = cfg.allCfgMap["processor.l3a.weight.cloud.sigmasmall"];
    cfg.sigmaLargeCloud = cfg.allCfgMap["processor.l3a.weight.cloud.sigmalarge"];
    cfg.weightDateMin = cfg.allCfgMap["processor.l3a.weight.total.weightdatemin"];

    // by default, do not keep job files
    cfg.keepJobFiles = false;
    auto keepStr = execProcConfigParameters["executor.processor.l3a.keep_job_folders"];
    if(keepStr == "1") cfg.keepJobFiles = true;
}

QStringList CompositeHandler::GetProductFormatterArgs(TaskToSubmit &productFormatterTask, EventProcessingContext &ctx, const CompositeJobConfig &cfg,
                                    const QList<ProductDetails> &productDetails, const QList<CompositeProductFormatterParams> &productParams) {

    const auto &executionInfosPath = productFormatterTask.GetFilePath("executionInfos.xml");
    WriteExecutionInfosFile(executionInfosPath, cfg, productDetails);

    QDateTime dtStartDate, dtEndDate;
    QString timePeriod = cfg.l3aSynthesisDate + "_" + cfg.l3aSynthesisDate;
    if(ProcessorHandlerHelper::GetIntevalFromProducts(productDetails, dtStartDate, dtEndDate)) {
        timePeriod = dtStartDate.toString("yyyyMMdd") + "_" + dtEndDate.toString("yyyyMMdd");
    }
    QStringList additionalArgs;
    const std::unique_ptr<ProductHelper> &l2aPrdHelper = ProductHelperFactory::GetProductHelper(productDetails[productDetails.size() - 1]);
    const QStringList &l2aPrdMetaFiles = l2aPrdHelper->GetProductMetadataFiles();
    if (l2aPrdMetaFiles.size() > 0) {
        additionalArgs += "-il";
        additionalArgs += l2aPrdMetaFiles[0];
    }

    if(cfg.lutPath.size() > 0) {
        additionalArgs += "-lut";
        additionalArgs += cfg.lutPath;
    }

    additionalArgs += "-processor.composite.refls";
    for(const CompositeProductFormatterParams &params: productParams) {
        additionalArgs += GetProductFormatterTile(params.tileId);
        additionalArgs += params.prevL3AProdRefls;
    }
    additionalArgs += "-processor.composite.weights";
    for(const CompositeProductFormatterParams &params: productParams) {
        additionalArgs += GetProductFormatterTile(params.tileId);
        additionalArgs += params.prevL3AProdWeights;
    }
    additionalArgs += "-processor.composite.flags";
    for(const CompositeProductFormatterParams &params: productParams) {
        additionalArgs += GetProductFormatterTile(params.tileId);
        additionalArgs += params.prevL3AProdFlags;
    }
    additionalArgs += "-processor.composite.dates";
    for(const CompositeProductFormatterParams &params: productParams) {
        additionalArgs += GetProductFormatterTile(params.tileId);
        additionalArgs += params.prevL3AProdDates;
    }
    additionalArgs += "-processor.composite.rgb";
    for(const CompositeProductFormatterParams &params: productParams) {
        additionalArgs += GetProductFormatterTile(params.tileId);
        additionalArgs += params.prevL3ARgbFile;
    }
    if (IsCloudOptimizedGeotiff(cfg.allCfgMap)) {
        additionalArgs += "-cog";
        additionalArgs += "1";
    }

    return GetDefaultProductFormatterArgs(ctx, productFormatterTask, cfg.jobId, cfg.siteId,
                                          "L3A", timePeriod, "composite", additionalArgs, false, executionInfosPath);
}


bool CompositeHandler::IsProductAcceptableForJob(int jobId, const ProductAvailableEvent &event)
{
    Q_UNUSED(jobId);
    Q_UNUSED(event);

    return false;
}

QStringList CompositeHandler::GetMissionsFromBandsMapping(const QString &bandsMappingFile) {
    // Normally, this is a small file
    QStringList listLines = ProcessorHandlerHelper::GetTextFileLines(bandsMappingFile);
    if(listLines.size() > 0) {
        // we get the first line only
        QString firstLine = listLines[0];
        return firstLine.split(",");
    }
    return QStringList();
}

QString CompositeHandler::DeductBandsMappingFile(const QList<ProductDetails> &prdDetails,
                                                 const QString &bandsMappingFile, int &resolution) {
    if (prdDetails.size() == 0) {
        return bandsMappingFile;
    }
    QFileInfo fileInfo(bandsMappingFile);

    // by default, we consider this is a dir
    QString curBandsMappingPath = bandsMappingFile;
    if(!fileInfo.isDir())
        curBandsMappingPath = fileInfo.dir().absolutePath();
    QList<Satellite> listUniqueSatTypes;
    for (int i = 0; i < prdDetails.size(); i++) {
        Satellite satType = (Satellite)prdDetails[i].GetProduct().satId;
        if(!listUniqueSatTypes.contains(satType)) {
            listUniqueSatTypes.append(satType);
        }
    }
    int cntUniqueSatTypes = listUniqueSatTypes.size();
    if(cntUniqueSatTypes < 1 || cntUniqueSatTypes > 2 ) {
        return bandsMappingFile;
    }
    if(cntUniqueSatTypes == 1) {
        if(listUniqueSatTypes[0] == Satellite::Sentinel2) {
            return (curBandsMappingPath + "/bands_mapping_s2.txt");
        }
        if(listUniqueSatTypes[0] == Satellite::Landsat8) {
            resolution = 30;
            return (curBandsMappingPath + "/bands_mapping_L8.txt");
        }
//        if(listUniqueSatTypes[0] == Satellite::Spot4) {
//            resolution = 20;
//            return (curBandsMappingPath + "/bands_mapping_spot4.txt");
//        }
//        if(listUniqueSatTypes[0] == Satellite::Spot5) {
//            resolution = 10;
//            return (curBandsMappingPath + "/bands_mapping_spot5.txt");
//        }
    } else {
        if(listUniqueSatTypes.contains(Satellite::Landsat8)) {
            if(listUniqueSatTypes.contains(Satellite::Sentinel2)) {
                if((resolution != 10) && (resolution != 20))
                    resolution = 10;
                return (curBandsMappingPath + "/bands_mapping_s2_L8.txt");
            }
//            else if(listUniqueSatTypes.contains(Satellite::Spot4)) {
//                resolution = 10;
//                return (curBandsMappingPath + "/bands_mapping_Spot4_L8.txt");
//            } else if(listUniqueSatTypes.contains(Satellite::Spot5)) {
//                resolution = 10;
//                return (curBandsMappingPath + "/bands_mapping_Spot5_L8.txt");
//            }
        }
    }
    return bandsMappingFile;
}


ProcessorJobDefinitionParams CompositeHandler::GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                          const ConfigurationParameterValueMap &requestOverrideCfgValues)
{
    ProcessorJobDefinitionParams params;

    QDateTime seasonStartDate;
    QDateTime seasonEndDate;
    // extract the scheduled date
    QDateTime qScheduledDate = QDateTime::fromTime_t(scheduledDate);
    Logger::debug(QStringLiteral("Scheduler L3A: Getting season dates for site %1 for scheduled date %2!")
                  .arg(siteId)
                  .arg(qScheduledDate.toString()));
    bool success = GetSeasonStartEndDates(ctx, siteId, seasonStartDate, seasonEndDate, qScheduledDate, requestOverrideCfgValues);
    // if cannot get the season dates
    if(!success) {
//        Logger::debug(QStringLiteral("Scheduler L3A: Error getting season start dates for site %1 for scheduled date %2!")
//                      .arg(siteId)
//                      .arg(qScheduledDate.toString()));
        return params;
    }
    Logger::debug(QStringLiteral("Scheduler L3A: Extracted season dates: Start: %1, End: %2!")
                  .arg(seasonStartDate.toString())
                  .arg(seasonEndDate.toString()));
    QDateTime limitDate = seasonEndDate.addMonths(2);
    if(qScheduledDate > limitDate) {
        Logger::debug(QStringLiteral("Scheduler L3A: Error scheduled date %1 greater than the limit date %2 for site %3!")
                      .arg(qScheduledDate.toString())
                      .arg(limitDate.toString())
                      .arg(siteId));
        return params;
    }

    ConfigurationParameterValueMap mapCfg = ctx.GetConfigurationParameters(QString("processor.l3a."), siteId, requestOverrideCfgValues);

    // we might have an offset in days from starting the downloading products to start the L3A production
    int startSeasonOffset = mapCfg["processor.l3a.start_season_offset"].value.toInt();
    seasonStartDate = seasonStartDate.addDays(startSeasonOffset);

    int halfSynthesis = mapCfg["processor.l3a.half_synthesis"].value.toInt();
    if(halfSynthesis == 0 || halfSynthesis < DEFAULT_HALF_SYNTHESIS)
        halfSynthesis = DEFAULT_HALF_SYNTHESIS;
    int synthDateOffset = mapCfg["processor.l3a.synth_date_sched_offset"].value.toInt();
    if(synthDateOffset == 0)
        synthDateOffset = 30;

    // compute the half synthesis date
    QDateTime halfSynthesisDate = qScheduledDate.addDays(-synthDateOffset);
    // compute the start and date time
    QDateTime startDate = halfSynthesisDate.addDays(-halfSynthesis);
    if(startDate < seasonStartDate) {
        startDate = seasonStartDate;
    }
    QDateTime endDate = halfSynthesisDate.addDays(halfSynthesis);
    // make sure that for the first month we have at least the default half synthesis
    if (seasonStartDate.addDays(halfSynthesis) > endDate) {
        endDate = seasonStartDate.addDays(halfSynthesis);
    }

    params.isValid = true;
    if (!CheckAllAncestorProductCreation(ctx, siteId, ProductType::L3AProductTypeId, startDate, endDate)) {
        // do not trigger yet the schedule.
        params.schedulingFlags = SchedulingFlags::SCH_FLG_RETRY_LATER;
        Logger::debug(QStringLiteral("Scheduled job for L3A and site ID %1 with start date %2 and end date %3 will "
                                     "not be executed retried later due to no products or not all inputs pre-processed!")
                              .arg(siteId)
                              .arg(startDate.toString())
                              .arg(endDate.toString()));
    } else {
        params.productList = ctx.GetProducts(siteId, (int)ProductType::L2AProductTypeId, startDate, endDate);
        params.jsonParameters = "{ \"synthesis_date\": \"" + halfSynthesisDate.toString("yyyyMMdd") + "\"}";
        Logger::debug(QStringLiteral("Executing scheduled job. Scheduler extracted for L3A a number "
                                     "of %1 products for site ID %2 with start date %3 and end date %4!")
                      .arg(params.productList.size())
                      .arg(siteId)
                      .arg(startDate.toString())
                      .arg(endDate.toString()));
    }
    return params;
}

QString CompositeHandler::GetL2AExternalMask(const ProductDetails &prdDetails)
{
    std::unique_ptr<ProductHelper> helper = ProductHelperFactory::GetProductHelper(prdDetails);
    const QStringList &extMasks = helper->GetProductMasks();
    if (extMasks.size() > 0 ) {
        return extMasks[0];
    }
    return "";
}

