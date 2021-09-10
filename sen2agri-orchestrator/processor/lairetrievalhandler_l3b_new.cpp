#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>

#include <fstream>

#include "lairetrievalhandler_l3b_new.hpp"
#include "processorhandlerhelper.h"
#include "json_conversions.hpp"
#include "logger.hpp"
#include "maccshdrmeananglesreader.hpp"
#include <unordered_map>

#include <QHash>
#include <QString>
#include <functional>

#include "products/generichighlevelproducthelper.h"
#include "processor/products/producthelper.h"
#include "processor/products/producthelperfactory.h"
using namespace orchestrator::products;

// For unordered map and QString as key
namespace std {
  template<> struct hash<QString> {
    std::size_t operator()(const QString& s) const noexcept {
      return (size_t) qHash(s);
    }
  };
}

#define CURRENT_PROC_PRDS_FILE_NAME     "current_processing_l3b_new.txt"

void LaiRetrievalHandlerL3BNew::CreateTasksForNewProduct(const L3BJobContext &jobCtx,
                                                         QList<TaskToSubmit> &outAllTasksList,
                                                    const QList<TileInfos> &tileInfosList) {

    // in allTasksList we might have tasks from other products. We start from the first task of the current product
    int initialTasksNo = outAllTasksList.size();
    int nbLaiMonoProducts = tileInfosList.size();
    for(int i = 0; i<nbLaiMonoProducts; i++) {
        outAllTasksList.append(TaskToSubmit{"lai-processor-mask-flags", {}});
        if (jobCtx.bGenNdvi) {
            outAllTasksList.append(TaskToSubmit{"lai-processor-ndvi-extractor", {}});
        }
        if (jobCtx.bGenLai || jobCtx.bGenFapar || jobCtx.bGenFCover) {
            outAllTasksList.append(TaskToSubmit{"lai-create-angles", {}});
            outAllTasksList.append(TaskToSubmit{"gdal_translate", {}});
            outAllTasksList.append(TaskToSubmit{"gdalbuildvrt", {}});
            outAllTasksList.append(TaskToSubmit{"gdal_translate", {}});
            if (jobCtx.bGenLai) {
                outAllTasksList.append(TaskToSubmit{"lai-processor", {}});
                outAllTasksList.append(TaskToSubmit{"lai-quantify-image", {}});
                outAllTasksList.append(TaskToSubmit{"gen-domain-flags", {}});
            }
            if (jobCtx.bGenFapar) {
                outAllTasksList.append(TaskToSubmit{"fapar-processor", {}});
                outAllTasksList.append(TaskToSubmit{"fapar-quantify-image", {}});
                outAllTasksList.append(TaskToSubmit{"gen-domain-flags", {}});
            }
            if (jobCtx.bGenFCover) {
                outAllTasksList.append(TaskToSubmit{"fcover-processor", {}});
                outAllTasksList.append(TaskToSubmit{"fcover-quantify-image", {}});
                outAllTasksList.append(TaskToSubmit{"gen-domain-flags", {}});
            }
        }
        if (jobCtx.bGenInDomainFlags) {
            // add the task for generating domain input flags
            outAllTasksList.append(TaskToSubmit{"gen-domain-flags", {}});
        }
    }
    outAllTasksList.append({"product-formatter", {}});
    if(jobCtx.bRemoveTempFiles) {
        outAllTasksList.append(TaskToSubmit{ "files-remover", {} });
    }

    //
    // NOTE: In this moment, the products in loop are not executed in parallel. To do this, the if(i > 0) below
    //      should be removed but in this case, the time-series-builders should wait for all the monodate images
    int i;
    QList<std::reference_wrapper<const TaskToSubmit>> productFormatterParentsRefs;

    // we execute in parallel and launch at once all processing chains for each product
    // for example, if we have genModels, we launch all bv-input-variable-generation for all products
    // if we do not have genModels, we launch all NDVIRVIExtraction in the same time for all products
    int nCurTaskIdx = initialTasksNo;

    // Specifies if the products creation should be chained or not.
    // TODO: This should be taken from the configuration
    bool bChainProducts = true;

    for(i = 0; i<nbLaiMonoProducts; i++) {
        // if we want chaining products and we have a previous product executed
        if(bChainProducts && initialTasksNo > 0) {
            // we create a dependency to the last task of the previous product
            outAllTasksList[nCurTaskIdx].parentTasks.append(outAllTasksList[nCurTaskIdx-1]);
        }   // else  skip over the lai-processor-mask-flags as we run it with no previous dependency,
            // allowing running several products in parallel
        // increment the current index for ndvi-rvi-extraction
        nCurTaskIdx++;

        // lai-processor-ndvi-extraction, lai-processor, fapar-processor, fcover-processor -> lai-processor-mask-flags
        // all these are run in parallel
        int flagsTaskIdx = nCurTaskIdx-1;
        if (jobCtx.bGenNdvi) {
            int ndviRviExtrIdx = nCurTaskIdx++;
            outAllTasksList[ndviRviExtrIdx].parentTasks.append(outAllTasksList[flagsTaskIdx]);
            // add the ndvi task to the list of the product formatter corresponding to this product
            productFormatterParentsRefs.append(outAllTasksList[ndviRviExtrIdx]);
        }
        int nAnglesTaskId = flagsTaskIdx;
        if (jobCtx.bGenLai || jobCtx.bGenFapar || jobCtx.bGenFCover) {
            nCurTaskIdx = CreateAnglesTasks(flagsTaskIdx, outAllTasksList, nCurTaskIdx, nAnglesTaskId);

            if (jobCtx.bGenLai) {
                nCurTaskIdx = CreateBiophysicalIndicatorTasks(nAnglesTaskId, outAllTasksList, productFormatterParentsRefs, nCurTaskIdx);
            }

            if (jobCtx.bGenFapar) {
                nCurTaskIdx = CreateBiophysicalIndicatorTasks(nAnglesTaskId, outAllTasksList, productFormatterParentsRefs, nCurTaskIdx);
            }

            if (jobCtx.bGenFCover) {
                nCurTaskIdx = CreateBiophysicalIndicatorTasks(nAnglesTaskId, outAllTasksList, productFormatterParentsRefs, nCurTaskIdx);
            }
        }
        if (jobCtx.bGenInDomainFlags) {
            int nInputDomainIdx = nCurTaskIdx++;
            outAllTasksList[nInputDomainIdx].parentTasks.append(outAllTasksList[flagsTaskIdx]);
            // add the input domain task to the list of the product formatter corresponding to this product
            productFormatterParentsRefs.append(outAllTasksList[nInputDomainIdx]);
        }
    }
    int productFormatterIdx = nCurTaskIdx++;
    outAllTasksList[productFormatterIdx].parentTasks.append(productFormatterParentsRefs);
    if(jobCtx.bRemoveTempFiles) {
        // cleanup-intermediate-files -> product formatter
        outAllTasksList[nCurTaskIdx].parentTasks.append(outAllTasksList[nCurTaskIdx-1]);
    }
}

int LaiRetrievalHandlerL3BNew::CreateAnglesTasks(int parentTaskId, QList<TaskToSubmit> &outAllTasksList,
                                     int nCurTaskIdx, int & nAnglesTaskId)
{
    int createAnglesIdx = nCurTaskIdx++;
    outAllTasksList[createAnglesIdx].parentTasks.append(outAllTasksList[parentTaskId]);
    int anglesGdalTranslateNoDataIdx = nCurTaskIdx++;
    outAllTasksList[anglesGdalTranslateNoDataIdx].parentTasks.append(outAllTasksList[createAnglesIdx]);
    int gdalBuildVrtIdx = nCurTaskIdx++;
    outAllTasksList[gdalBuildVrtIdx].parentTasks.append(outAllTasksList[anglesGdalTranslateNoDataIdx]);
    int anglesResamleIdx = nCurTaskIdx++;
    outAllTasksList[anglesResamleIdx].parentTasks.append(outAllTasksList[gdalBuildVrtIdx]);
    nAnglesTaskId = anglesResamleIdx;

    return nCurTaskIdx;
}

int LaiRetrievalHandlerL3BNew::CreateBiophysicalIndicatorTasks(int parentTaskId, QList<TaskToSubmit> &outAllTasksList,
                                     QList<std::reference_wrapper<const TaskToSubmit>> &productFormatterParentsRefs,
                                     int nCurTaskIdx)
{
    int nBIProcessorIdx = nCurTaskIdx++;
    outAllTasksList[nBIProcessorIdx].parentTasks.append(outAllTasksList[parentTaskId]);

    // domain-flags-image -> BI-processor
    int nBIDomainFlagsImageIdx = nCurTaskIdx++;
    outAllTasksList[nBIDomainFlagsImageIdx].parentTasks.append(outAllTasksList[nBIProcessorIdx]);

    // BI-quantify-image -> domain-flags-image
    int nBIQuantifyImageIdx = nCurTaskIdx++;
    outAllTasksList[nBIQuantifyImageIdx].parentTasks.append(outAllTasksList[nBIDomainFlagsImageIdx]);
    // add the quantified task to the list of the product formatter corresponding to this product
    productFormatterParentsRefs.append(outAllTasksList[nBIQuantifyImageIdx]);

    return nCurTaskIdx;
}

NewStepList LaiRetrievalHandlerL3BNew::GetStepsForMonodateLai(const L3BJobContext &jobCtx, const QList<TileInfos> &prdTilesInfosList,
                                                              QList<TaskToSubmit> &allTasksList, int tasksStartIdx)
{
    NewStepList steps;

    // in allTasksList we might have tasks from other products. We start from the first task of the current product
    int curTaskIdx = tasksStartIdx;

    QList<TileResultFiles> tileResultFileInfos;
    QStringList cleanupTemporaryFilesList;

    for (int i = 0; i<prdTilesInfosList.size(); i++) {
        TileResultFiles tileResultFileInfo;
        const auto &prdTileInfo = prdTilesInfosList[i];
        InitTileResultFiles(prdTileInfo, tileResultFileInfo);

        curTaskIdx = GetStepsForStatusFlags(jobCtx, allTasksList, curTaskIdx, tileResultFileInfo, steps,
                                            cleanupTemporaryFilesList);
        if (jobCtx.bGenNdvi) {
            curTaskIdx = GetStepsForNdvi(jobCtx, allTasksList, curTaskIdx, tileResultFileInfo,
                                         steps, cleanupTemporaryFilesList);
        }
        if (jobCtx.bGenLai || jobCtx.bGenFapar || jobCtx.bGenFCover) {
            curTaskIdx = GetStepsForAnglesCreation(allTasksList, curTaskIdx, tileResultFileInfo, steps,
                                                   cleanupTemporaryFilesList);
            if (jobCtx.bGenLai) {
                curTaskIdx = GetStepsForMonoDateBI(jobCtx, allTasksList, "lai", curTaskIdx,
                                                   tileResultFileInfo, steps, cleanupTemporaryFilesList);
            }
            if (jobCtx.bGenFapar) {
                curTaskIdx = GetStepsForMonoDateBI(jobCtx, allTasksList, "fapar", curTaskIdx,
                                                   tileResultFileInfo, steps, cleanupTemporaryFilesList);
            }
            if (jobCtx.bGenFCover) {
                curTaskIdx = GetStepsForMonoDateBI(jobCtx, allTasksList, "fcover", curTaskIdx,
                                                   tileResultFileInfo, steps, cleanupTemporaryFilesList);
            }
        }
        if (jobCtx.bGenInDomainFlags) {
            curTaskIdx = GetStepsForInDomainFlags(jobCtx, allTasksList, curTaskIdx, tileResultFileInfo, steps, cleanupTemporaryFilesList);
        }

        tileResultFileInfos.append(tileResultFileInfo);
    }
    TaskToSubmit &laiMonoProductFormatterTask = allTasksList[curTaskIdx++];
    const QStringList &productFormatterArgs = GetLaiMonoProductFormatterArgs(laiMonoProductFormatterTask, jobCtx,
                                                                             prdTilesInfosList, tileResultFileInfos);
    steps.append(CreateTaskStep(laiMonoProductFormatterTask, "ProductFormatter", productFormatterArgs));

    if(jobCtx.bRemoveTempFiles) {
        TaskToSubmit &cleanupTemporaryFilesTask = allTasksList[curTaskIdx++];
        // add also the cleanup step
        steps.append(CreateTaskStep(cleanupTemporaryFilesTask, "CleanupTemporaryFiles", cleanupTemporaryFilesList));
    }

    return steps;
}

int LaiRetrievalHandlerL3BNew::GetStepsForStatusFlags(const L3BJobContext &jobCtx, QList<TaskToSubmit> &allTasksList, int curTaskIdx,
                            TileResultFiles &tileResultFileInfo, NewStepList &steps, QStringList &cleanupTemporaryFilesList) {

    TaskToSubmit &genMonoDateMskFagsTask = allTasksList[curTaskIdx++];
    tileResultFileInfo.statusFlagsFile = genMonoDateMskFagsTask.GetFilePath("LAI_mono_date_msk_flgs_img.tif");
    tileResultFileInfo.statusFlagsFileResampled = genMonoDateMskFagsTask.GetFilePath("LAI_mono_date_msk_flgs_img_resampled.tif");
    const QStringList &genMonoDateMskFagsArgs = GetMonoDateMskFlagsArgs(tileResultFileInfo.tileFile,
                                                                 tileResultFileInfo.inPrdExtMsk,
                                                                 tileResultFileInfo.statusFlagsFile,
                                                                 tileResultFileInfo.statusFlagsFileResampled,
                                                                 jobCtx.resolutionStr);
    // add these steps to the steps list to be submitted
    steps.append(CreateTaskStep(genMonoDateMskFagsTask, "GenerateLaiMonoDateMaskFlags", genMonoDateMskFagsArgs));
    cleanupTemporaryFilesList.append(tileResultFileInfo.statusFlagsFile);
    cleanupTemporaryFilesList.append(tileResultFileInfo.statusFlagsFileResampled);

    return curTaskIdx;
}

int LaiRetrievalHandlerL3BNew::GetStepsForNdvi(const L3BJobContext &jobCtx, QList<TaskToSubmit> &allTasksList, int curTaskIdx,
                            TileResultFiles &tileResultFileInfo,  NewStepList &steps, QStringList &cleanupTemporaryFilesList) {

    TaskToSubmit &ndviRviExtractorTask = allTasksList[curTaskIdx++];
    tileResultFileInfo.ndviFile = ndviRviExtractorTask.GetFilePath("single_ndvi.tif");
    const QStringList &ndviRviExtractionArgs = GetNdviRviExtractionNewArgs(tileResultFileInfo.tileFile,
                                                                 tileResultFileInfo.statusFlagsFile,
                                                                 tileResultFileInfo.ndviFile,
                                                                 jobCtx.resolutionStr, jobCtx.laiCfgFile);
    steps.append(CreateTaskStep(ndviRviExtractorTask, "NdviRviExtractionNew", ndviRviExtractionArgs));
    // save the file to be sent to product formatter
    cleanupTemporaryFilesList.append(tileResultFileInfo.ndviFile);

    return curTaskIdx;
}

int LaiRetrievalHandlerL3BNew::GetStepsForAnglesCreation(QList<TaskToSubmit> &allTasksList, int curTaskIdx,
                            TileResultFiles &tileResultFileInfo, NewStepList &steps, QStringList &cleanupTemporaryFilesList) {
    TaskToSubmit &createAnglesTask = allTasksList[curTaskIdx++];
    TaskToSubmit &gdalTranslateNoDataTask = allTasksList[curTaskIdx++];
    TaskToSubmit &anglesCreateVrtTask = allTasksList[curTaskIdx++];
    TaskToSubmit &anglesResampleTask = allTasksList[curTaskIdx++];

    const auto & anglesSmallResFileName = createAnglesTask.GetFilePath("angles_small_res.tif");
    const auto & anglesSmallResNoDataFileName = gdalTranslateNoDataTask.GetFilePath("angles_small_res_no_data.tif");
    const auto & anglesVrtFileName = anglesCreateVrtTask.GetFilePath("angles.vrt");
    tileResultFileInfo.anglesFile = anglesResampleTask.GetFilePath("angles_resampled.tif");

    const QStringList &createAnglesArgs = GetCreateAnglesArgs(tileResultFileInfo.tileFile, anglesSmallResFileName);
    const QStringList &gdalSetAnglesNoDataArgs = GetGdalTranslateAnglesNoDataArgs(anglesSmallResFileName, anglesSmallResNoDataFileName);
    const QStringList &gdalBuildAnglesVrtArgs = GetGdalBuildAnglesVrtArgs(anglesSmallResNoDataFileName, anglesVrtFileName);
    const QStringList &gdalResampleAnglesArgs = GetGdalTranslateResampleAnglesArgs(anglesVrtFileName, tileResultFileInfo.anglesFile);

    steps.append(CreateTaskStep(createAnglesTask, "CreateAnglesRaster", createAnglesArgs));
    steps.append(CreateTaskStep(gdalTranslateNoDataTask, "gdal_translate", gdalSetAnglesNoDataArgs));
    steps.append(CreateTaskStep(anglesCreateVrtTask, "gdalbuildvrt", gdalBuildAnglesVrtArgs));
    steps.append(CreateTaskStep(anglesResampleTask, "gdal_translate", gdalResampleAnglesArgs));
    cleanupTemporaryFilesList.append(anglesSmallResFileName);
    cleanupTemporaryFilesList.append(anglesSmallResNoDataFileName);
    cleanupTemporaryFilesList.append(anglesVrtFileName);
    cleanupTemporaryFilesList.append(tileResultFileInfo.anglesFile);

    return curTaskIdx;
}

int LaiRetrievalHandlerL3BNew::GetStepsForMonoDateBI(const L3BJobContext &jobCtx, QList<TaskToSubmit> &allTasksList,
                           const QString &indexName, int curTaskIdx, TileResultFiles &tileResultFileInfo, NewStepList &steps,
                           QStringList &cleanupTemporaryFilesList) {
    const QString &indexNameCaps = indexName.toUpper();
    TaskToSubmit &biProcessorTask = allTasksList[curTaskIdx++];
    TaskToSubmit &biDomainFlagsTask = allTasksList[curTaskIdx++];
    TaskToSubmit &quantifyBIImageTask = allTasksList[curTaskIdx++];
    const auto & BIFileName = biProcessorTask.GetFilePath(indexNameCaps + "_mono_date_img.tif");
    const auto & quantifiedBIFileName = quantifyBIImageTask.GetFilePath(indexNameCaps + "_mono_date_img_16.tif");
    const QStringList &BIProcessorArgs = GetLaiProcessorArgs(tileResultFileInfo.tileFile, tileResultFileInfo.anglesFile,
                                                                    jobCtx.resolutionStr, jobCtx.laiCfgFile,
                                                                    BIFileName, indexName);
    steps.append(CreateTaskStep(biProcessorTask, "BVLaiNewProcessor" + indexNameCaps, BIProcessorArgs));

    const auto & domainFlagsFileName = biDomainFlagsTask.GetFilePath(indexNameCaps + "_out_domain_flags.tif");
    const auto & correctedBIFileName = biDomainFlagsTask.GetFilePath(indexNameCaps + "_corrected_mono_date.tif");
    const QStringList &outDomainFlagsArgs = GetGenerateOutputDomainFlagsArgs(tileResultFileInfo.tileFile, BIFileName,
                                                                jobCtx.laiCfgFile, indexName,
                                                                domainFlagsFileName,  correctedBIFileName,
                                                                jobCtx.resolutionStr);
    steps.append(CreateTaskStep(biDomainFlagsTask, "Generate" + indexNameCaps + "InDomainQualityFlags", outDomainFlagsArgs));

    const QStringList &quantifyFcoverImageArgs = GetQuantifyImageArgs(correctedBIFileName, quantifiedBIFileName);
    steps.append(CreateTaskStep(quantifyBIImageTask, "Quantify"+indexNameCaps + "Image", quantifyFcoverImageArgs));
    // save the file to be sent to product formatter
    if (indexName == "fapar") {
        tileResultFileInfo.faparDomainFlagsFile = domainFlagsFileName;
        tileResultFileInfo.faparFile = quantifiedBIFileName;
    } else if (indexName == "fcover") {
        tileResultFileInfo.fcoverDomainFlagsFile = domainFlagsFileName;
        tileResultFileInfo.fcoverFile = quantifiedBIFileName;
    } else {
        tileResultFileInfo.laiDomainFlagsFile = domainFlagsFileName;
        tileResultFileInfo.laiFile = quantifiedBIFileName;
    }

    cleanupTemporaryFilesList.append(BIFileName);
    cleanupTemporaryFilesList.append(correctedBIFileName);
    cleanupTemporaryFilesList.append(quantifiedBIFileName);

    return curTaskIdx;
}

int LaiRetrievalHandlerL3BNew::GetStepsForInDomainFlags(const L3BJobContext &jobCtx, QList<TaskToSubmit> &allTasksList, int curTaskIdx,
                            TileResultFiles &tileResultFileInfo, NewStepList &steps, QStringList &) {
    TaskToSubmit &inputDomainTask = allTasksList[curTaskIdx++];
    tileResultFileInfo.inDomainFlagsFile = inputDomainTask.GetFilePath("Input_domain_flags.tif");
    const QStringList &inDomainFlagsArgs = GetGenerateInputDomainFlagsArgs(tileResultFileInfo.tileFile,
                                                                jobCtx.laiCfgFile, tileResultFileInfo.inDomainFlagsFile,
                                                                jobCtx.resolutionStr);
    steps.append(CreateTaskStep(inputDomainTask, "GenerateInDomainQualityFlags", inDomainFlagsArgs));

    return curTaskIdx;
}


void LaiRetrievalHandlerL3BNew::WriteExecutionInfosFile(const QString &executionInfosPath,
                                               const QList<TileResultFiles> &tileResultFilesList) {
    std::ofstream executionInfosFile;
    try
    {
        executionInfosFile.open(executionInfosPath.toStdString().c_str(), std::ofstream::out);
        executionInfosFile << "<?xml version=\"1.0\" ?>" << std::endl;
        executionInfosFile << "<metadata>" << std::endl;
        executionInfosFile << "  <General>" << std::endl;
        executionInfosFile << "  </General>" << std::endl;

        executionInfosFile << "  <XML_files>" << std::endl;
        for (int i = 0; i<tileResultFilesList.size(); i++) {
            executionInfosFile << "    <XML_" << std::to_string(i) << ">" << tileResultFilesList[i].tileFile.toStdString()
                               << "</XML_" << std::to_string(i) << ">" << std::endl;
        }
        executionInfosFile << "  </XML_files>" << std::endl;
        executionInfosFile << "</metadata>" << std::endl;
        executionInfosFile.close();
    }
    catch(...)
    {

    }
}

void LaiRetrievalHandlerL3BNew::WriteInputPrdIdsInfosFile(const QString &outFilePath,
                                               const QList<TileInfos> &prdTilesInfosList) {
    std::ofstream outFile;
    try
    {
        outFile.open(outFilePath.toStdString().c_str(), std::ofstream::out);
        for (int i = 0; i<prdTilesInfosList.size(); i++) {
            outFile << prdTilesInfosList[i].parentProductInfo.productId << std::endl;
        }
        outFile.close();
    }
    catch(...)
    {

    }
}

void LaiRetrievalHandlerL3BNew::HandleProduct(const L3BJobContext &jobCtx,
                                            const QList<TileInfos> &prdTilesInfosList, QList<TaskToSubmit> &allTasksList) {
    int tasksStartIdx = allTasksList.size();
    // create the tasks
    CreateTasksForNewProduct(jobCtx, allTasksList, prdTilesInfosList);

    QList<std::reference_wrapper<TaskToSubmit>> allTasksListRef;
    for(int i = tasksStartIdx; i < allTasksList.size(); i++) {
        const TaskToSubmit &task = allTasksList.at(i);
        allTasksListRef.append((TaskToSubmit&)task);
    }
    // submit all tasks
    SubmitTasks(*jobCtx.pCtx, jobCtx.event.jobId, allTasksListRef);

    NewStepList steps;

    steps += GetStepsForMonodateLai(jobCtx, prdTilesInfosList, allTasksList, tasksStartIdx);
    jobCtx.pCtx->SubmitSteps(steps);
}

void LaiRetrievalHandlerL3BNew::SubmitEndOfLaiTask(EventProcessingContext &ctx,
                                                const JobSubmittedEvent &event,
                                                const QList<TaskToSubmit> &allTasksList) {
    // add the end of lai job that will perform the cleanup
    QList<std::reference_wrapper<const TaskToSubmit>> endOfJobParents;
    for(const TaskToSubmit &task: allTasksList) {
        if(task.moduleName == "product-formatter" ||
                task.moduleName == "files-remover") {
            endOfJobParents.append(task);
        }
    }
    // we add a task in order to wait for all product formatter to finish.
    // This will allow us to mark the job as finished and to remove the job folder
    TaskToSubmit endOfJobDummyTask{"end-of-job", {}};
    endOfJobDummyTask.parentTasks.append(endOfJobParents);
    SubmitTasks(ctx, event.jobId, {endOfJobDummyTask});
    ctx.SubmitSteps({CreateTaskStep(endOfJobDummyTask, "EndOfJob", QStringList())});

}

void LaiRetrievalHandlerL3BNew::HandleJobSubmittedImpl(EventProcessingContext &ctx,
                                             const JobSubmittedEvent &evt)
{
    L3BJobContext jobCtx(this, &ctx, evt);

    if (!jobCtx.bGenNdvi && !jobCtx.bGenLai && !jobCtx.bGenFapar && !jobCtx.bGenFCover) {
        ctx.MarkJobFailed(evt.jobId);
        throw std::runtime_error(
            QStringLiteral("No vedgetation index (NDVI, LAI, FAPAR or FCOVER) was configured to be generated").toStdString());
    }


    // Moved this from the GetProcessingDefinitionImpl function as it might be time consuming and scheduler will
    // throw exception if timeout exceeded
    ProductList prdsToProcess;
    int ret = UpdateJobSubmittedParamsFromSchedReq(jobCtx, prdsToProcess);
    // no products available from the scheduling ... mark also the job as failed
    if (ret == 0) {
        ctx.MarkJobFailed(evt.jobId);
        throw std::runtime_error(
                    QStringLiteral("L3B Scheduled job with id %1 for site %2 marked as done as no products are available for now to process").
                                         arg(evt.jobId).arg(evt.siteId).toStdString());
    } else if (ret == -1) {
        // custom job
        auto parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
        const QStringList &prdNames = GetInputProductNames(parameters);
        prdsToProcess = ctx.GetProducts(evt.siteId, prdNames);
    }
    // extract the full paths to avoid extracting product info one by one
    std::unordered_map<QString, Product> mapInfoPrds;
    std::for_each(prdsToProcess.begin(), prdsToProcess.end(), [&mapInfoPrds](const Product &prd) {
        mapInfoPrds[prd.fullPath] = prd;
    });

    // create and submit the tasks for the received products
    const QList<ProductDetails> &productDetails = ProcessorHandlerHelper::GetProductDetails(prdsToProcess, ctx);
    if(productDetails.size() == 0) {
        ctx.MarkJobFailed(evt.jobId);
        throw std::runtime_error(
            QStringLiteral("No products provided at input or no products available in the specified interval").
                    toStdString());
    }
    // Group the products that belong to the same date
    // the tiles of products from secondary satellite are not included if they happen to be from the same date with tiles from
    // the same date
    const QMap<QDate, QList<ProductDetails>> &dateGroupedInputProductToTilesMap = ProcessorHandlerHelper::GroupByDate(productDetails);

    //container for all task
    QList<TaskToSubmit> allTasksList;
    const QSet<QString> &tilesFilter = GetTilesFilter(jobCtx);
    for(const auto &key : dateGroupedInputProductToTilesMap.keys()) {
        const QList<ProductDetails> &prdsDetails = dateGroupedInputProductToTilesMap[key];
        // create structures providing the models for each tile
        QList<TileInfos> tilesInfosList;
        for(const ProductDetails &prdDetails: prdsDetails) {
            if (FilterTile(tilesFilter, prdDetails)) {
                std::unique_ptr<ProductHelper> helper = ProductHelperFactory::GetProductHelper(prdDetails);
                const QStringList &metaFiles = helper->GetProductMetadataFiles();
                if (metaFiles.size() == 0) {
                    continue;
                }
                TileInfos tileInfo;
                tileInfo.tileFile = metaFiles[0];
                tileInfo.parentProductInfo = prdDetails.GetProduct();
                const QStringList &extMasks = helper->GetProductMasks();
                if (extMasks.size() > 0 ) {
                    tileInfo.prdExternalMskFile = extMasks[0];
                }
                tileInfo.tileIds = prdDetails.GetProduct().tiles;
                if (tileInfo.tileIds.size() == 0) {
                    Logger::error(QStringLiteral("InitTileResultFiles: the product %1 does not have any associated tiles!")
                                  .arg(metaFiles[0]));
                    continue;
                }
                tilesInfosList.append(tileInfo);
            }
        }
        // Handle product only if we have at least one tile (we might have all of them filtered)
        if (tilesInfosList.size() > 0) {
            HandleProduct(jobCtx, tilesInfosList, allTasksList);
        }
    }

    // we add a task in order to wait for all product formatter to finish.
    // This will allow us to mark the job as finished and to remove the job folder
    SubmitEndOfLaiTask(ctx, evt, allTasksList);
}

void LaiRetrievalHandlerL3BNew::HandleTaskFinishedImpl(EventProcessingContext &ctx,
                                             const TaskFinishedEvent &event)
{
    if (event.module == "end-of-job") {
        ctx.MarkJobFinished(event.jobId);
        // Now remove the job folder containing temporary files
        RemoveJobFolder(ctx, event.jobId, processorDescr.shortName);
    }
    if ((event.module == "product-formatter")) {
        const QString &prodName = GetOutputProductName(ctx, event);
        const QString &productFolder = GetOutputProductPath(ctx, event);
        GenericHighLevelProductHelper prdHelper(productFolder);
        if(prodName != "" && prdHelper.HasValidStructure()) {
            const QString &quicklook = GetProductFormatterQuicklook(ctx, event);
            const QString &footPrint = GetProductFormatterFootprint(ctx, event);
            const QStringList &prodTiles = prdHelper.GetTileIdsFromProduct();
            // get the satellite id for the product
            const QMap<Satellite, TileList> &siteTiles = GetSiteTiles(ctx, event.siteId);
            Satellite satId = Satellite::Invalid;
            for(const auto &tileId : prodTiles) {
                // we assume that all the tiles from the product are from the same satellite
                // in this case, we get only once the satellite Id for all tiles
                if(satId == Satellite::Invalid) {
                    satId = ProcessorHandlerHelper::GetSatIdForTile(siteTiles, tileId);
                    // ignore tiles for which the satellite id cannot be determined
                    if(satId != Satellite::Invalid) {
                        break;
                    }
                }
            }

            // Insert the product into the database
            const ProductIdsList &prdIds = GetOutputProductParentProductIds(ctx, event);
            int ret = ctx.InsertProduct({ ProductType::L3BProductTypeId, event.processorId, static_cast<int>(satId), event.siteId, event.jobId,
                                productFolder, prdHelper.GetAcqDate(), prodName,
                                quicklook, footPrint, std::experimental::nullopt, prodTiles, prdIds });
            Logger::debug(QStringLiteral("InsertProduct for %1 returned %2").arg(prodName).arg(ret));

            // Cleanup the currently processing products for the current job
            Logger::info(QStringLiteral("Cleaning up the file containing currently processing products for output product %1 and folder %2 and site id %3 and job id %4").
                         arg(prodName).arg(productFolder).arg(event.siteId).arg(event.jobId));
            const QString &curProcPrdsFilePath = GetSiteCurrentProcessingPrdsFile(ctx, event.jobId, event.siteId);
            QStringList prdStrIds;
            for(int id: prdIds) { prdStrIds.append(QString::number(id)); }
            ProcessorHandlerHelper::CleanupCurrentProductIdsForJob(curProcPrdsFilePath, event.jobId, prdStrIds);

        } else {
            Logger::error(QStringLiteral("Cannot insert into database the product with name %1 and folder %2").arg(prodName).arg(productFolder));
            // We might have several L3B products, we should not mark it at failed here as
            // this will stop also all other L3B processings that might be successful
            //ctx.MarkJobFailed(event.jobId);
        }
    }
}

QStringList LaiRetrievalHandlerL3BNew::GetCreateAnglesArgs(const QString &inputProduct, const QString &anglesFile) {
    return { "CreateAnglesRaster",
           "-xml", inputProduct,
           "-out", anglesFile
    };
}

QStringList LaiRetrievalHandlerL3BNew::GetGdalTranslateAnglesNoDataArgs(const QString &anglesFile,
                                                                        const QString &resultAnglesFile) {
    return {
            "-of", "GTiff", "-a_nodata", "-10000",
            anglesFile,
            resultAnglesFile
    };
}

QStringList LaiRetrievalHandlerL3BNew::GetGdalBuildAnglesVrtArgs(const QString &anglesFile,
                                                                 const QString &resultVrtFile) {
    return {
             "-tr", "10", "10", "-r", "bilinear", "-srcnodata", "-10000", "-vrtnodata", "-10000",
            resultVrtFile,
            anglesFile
    };
}

QStringList LaiRetrievalHandlerL3BNew::GetGdalTranslateResampleAnglesArgs(const QString &vrtFile,
                                                                        const QString &resultResampledAnglesFile) {
    return {
            vrtFile,
            resultResampledAnglesFile
    };
}

QStringList LaiRetrievalHandlerL3BNew::GetNdviRviExtractionNewArgs(const QString &inputProduct, const QString &msksFlagsFile,
                                                          const QString &ndviFile, const QString &resolution, const QString &laiBandsCfg) {
    return { "NdviRviExtractionNew",
           "-xml", inputProduct,
           "-msks", msksFlagsFile,
           "-ndvi", ndviFile,
           "-outres", resolution,
           "-laicfgs", laiBandsCfg
    };
}

QStringList LaiRetrievalHandlerL3BNew::GetLaiProcessorArgs(const QString &xmlFile, const QString &anglesFileName,
                                                           const QString &resolution, const QString &laiBandsCfg,
                                                           const QString &monoDateLaiFileName, const QString &indexName) {
    QString outParamName = QString("-out") + indexName;
    return { "BVLaiNewProcessor",
        "-xml", xmlFile,
        "-angles", anglesFileName,
        outParamName, monoDateLaiFileName,
        "-outres", resolution,
        "-laicfgs", laiBandsCfg
    };
}

QStringList LaiRetrievalHandlerL3BNew::GetGenerateInputDomainFlagsArgs(const QString &xmlFile,  const QString &laiBandsCfg,
                                                            const QString &outFlagsFileName, const QString &outRes) {
    return { "GenerateDomainQualityFlags",
        "-xml", xmlFile,
        "-laicfgs", laiBandsCfg,
        "-outf", outFlagsFileName,
        "-outres", outRes
    };
}

QStringList LaiRetrievalHandlerL3BNew::GetGenerateOutputDomainFlagsArgs(const QString &xmlFile, const QString &laiRasterFile,
                                                            const QString &laiBandsCfg, const QString &indexName,
                                                            const QString &outFlagsFileName,  const QString &outCorrectedLaiFile,
                                                            const QString &outRes)  {
    return { "GenerateDomainQualityFlags",
        "-xml", xmlFile,
        "-in", laiRasterFile,
        "-laicfgs", laiBandsCfg,
        "-indextype", indexName,
        "-outf", outFlagsFileName,
        "-out", outCorrectedLaiFile,
        "-outres", outRes,
    };
}

QStringList LaiRetrievalHandlerL3BNew::GetQuantifyImageArgs(const QString &inFileName, const QString &outFileName)  {
    return { "QuantifyImage",
        "-in", inFileName,
        "-out", outFileName
    };
}

QStringList LaiRetrievalHandlerL3BNew::GetMonoDateMskFlagsArgs(const QString &inputProduct,
                                                               const QString &extMsk,
                                                               const QString &monoDateMskFlgsFileName,
                                                               const QString &monoDateMskFlgsResFileName,
                                                               const QString &resStr) {
    QStringList args = { "GenerateLaiMonoDateMaskFlags",
      "-inxml", inputProduct,
      "-out", monoDateMskFlgsFileName,
      "-outres", resStr,
      "-outresampled", monoDateMskFlgsResFileName
    };
    if(extMsk.size() > 0) {
        args += "-extmsk";
        args += extMsk;
    }
    return args;
}

QStringList LaiRetrievalHandlerL3BNew::GetLaiMonoProductFormatterArgs(TaskToSubmit &productFormatterTask, const L3BJobContext &jobCtx,
                                                                      const QList<TileInfos> &prdTilesInfosList,
                                                                      const QList<TileResultFiles> &tileResultFilesList) {

    //const auto &targetFolder = productFormatterTask.GetFilePath("");
    const auto &targetFolder = GetFinalProductFolder(*jobCtx.pCtx, jobCtx.event.jobId, jobCtx.event.siteId);
    const auto &outPropsPath = productFormatterTask.GetFilePath(PRODUCT_FORMATTER_OUT_PROPS_FILE);
    const auto &executionInfosPath = productFormatterTask.GetFilePath("executionInfos.xml");
    const auto &inputPrdsIdsInfosPath = productFormatterTask.GetFilePath(PRODUCT_FORMATTER_IN_PRD_IDS_FILE);

    WriteExecutionInfosFile(executionInfosPath, tileResultFilesList);
    WriteInputPrdIdsInfosFile(inputPrdsIdsInfosPath, prdTilesInfosList);

    QStringList productFormatterArgs = { "ProductFormatter",
                            "-destroot", targetFolder,
                            "-fileclass", "OPER",
                            "-level", "L3B",
                            "-baseline", "01.00",
                            "-siteid", QString::number(jobCtx.event.siteId),
                            "-processor", "vegetation",
                            "-compress", "1",
                            "-gipp", executionInfosPath,
                            "-outprops", outPropsPath};
    productFormatterArgs += "-il";
    for(const TileResultFiles &tileInfo: tileResultFilesList) {
        productFormatterArgs.append(tileInfo.tileFile);
    }

    if(jobCtx.lutFile.size() > 0) {
        productFormatterArgs += "-lut";
        productFormatterArgs += jobCtx.lutFile;
    }

    productFormatterArgs += "-processor.vegetation.laistatusflgs";
    for(int i = 0; i<tileResultFilesList.size(); i++) {
        productFormatterArgs += GetProductFormatterTile(tileResultFilesList[i].tileId);
        productFormatterArgs += tileResultFilesList[i].statusFlagsFileResampled;
    }

    productFormatterArgs += "-processor.vegetation.indomainflgs";
    for(int i = 0; i<tileResultFilesList.size(); i++) {
        productFormatterArgs += GetProductFormatterTile(tileResultFilesList[i].tileId);
        productFormatterArgs += tileResultFilesList[i].inDomainFlagsFile;
    }

    if (jobCtx.bGenNdvi) {
        productFormatterArgs += "-processor.vegetation.laindvi";
        for(int i = 0; i<tileResultFilesList.size(); i++) {
            productFormatterArgs += GetProductFormatterTile(tileResultFilesList[i].tileId);
            productFormatterArgs += tileResultFilesList[i].ndviFile;
        }
    }
    if (jobCtx.bGenLai) {
        productFormatterArgs += "-processor.vegetation.laimonodate";
        for(int i = 0; i<tileResultFilesList.size(); i++) {
            productFormatterArgs += GetProductFormatterTile(tileResultFilesList[i].tileId);
            productFormatterArgs += tileResultFilesList[i].laiFile;
        }
        productFormatterArgs += "-processor.vegetation.laidomainflgs";
        for(int i = 0; i<tileResultFilesList.size(); i++) {
            productFormatterArgs += GetProductFormatterTile(tileResultFilesList[i].tileId);
            productFormatterArgs += tileResultFilesList[i].laiDomainFlagsFile;
        }
    }

    if (jobCtx.bGenFapar) {
        productFormatterArgs += "-processor.vegetation.faparmonodate";
        for(int i = 0; i<tileResultFilesList.size(); i++) {
            productFormatterArgs += GetProductFormatterTile(tileResultFilesList[i].tileId);
            productFormatterArgs += tileResultFilesList[i].faparFile;
        }
        productFormatterArgs += "-processor.vegetation.fapardomainflgs";
        for(int i = 0; i<tileResultFilesList.size(); i++) {
            productFormatterArgs += GetProductFormatterTile(tileResultFilesList[i].tileId);
            productFormatterArgs += tileResultFilesList[i].faparDomainFlagsFile;
        }
    }

    if (jobCtx.bGenFCover) {
        productFormatterArgs += "-processor.vegetation.fcovermonodate";
        for(int i = 0; i<tileResultFilesList.size(); i++) {
            productFormatterArgs += GetProductFormatterTile(tileResultFilesList[i].tileId);
            productFormatterArgs += tileResultFilesList[i].fcoverFile;
        }
        productFormatterArgs += "-processor.vegetation.fcoverdomaniflgs";
        for(int i = 0; i<tileResultFilesList.size(); i++) {
            productFormatterArgs += GetProductFormatterTile(tileResultFilesList[i].tileId);
            productFormatterArgs += tileResultFilesList[i].fcoverDomainFlagsFile;
        }
    }

    if (IsCloudOptimizedGeotiff(jobCtx.configParameters)) {
        productFormatterArgs += "-cog";
        productFormatterArgs += "1";
    }

    return productFormatterArgs;
}

const QString& LaiRetrievalHandlerL3BNew::GetDefaultCfgVal(std::map<QString, QString> &configParameters, const QString &key, const QString &defVal) {
    auto search = configParameters.find(key);
    if(search != configParameters.end()) {
        return search->second;
    }
    return defVal;
}

QSet<QString> LaiRetrievalHandlerL3BNew::GetTilesFilter(const L3BJobContext &jobCtx)
{
    QString strTilesFilter;
    if(jobCtx.parameters.contains("tiles_filter")) {
        const auto &value = jobCtx.parameters["tiles_filter"];
        if(value.isString()) {
            strTilesFilter = value.toString();
        }
    }
    if (strTilesFilter.isEmpty()) {
        auto it = jobCtx.configParameters.find("processor.l3b.lai.tiles_filter");
        if (it != jobCtx.configParameters.end()) {
            strTilesFilter = it->second;
        }
    }
    QSet<QString> retSet;
    // accept any of these separators
    const QStringList &tilesList = strTilesFilter.split(',');
    for (const QString &strTile: tilesList) {
        const QString &strTrimmedTile = strTile.trimmed();
        if(!strTrimmedTile.isEmpty()) {
            retSet.insert(strTrimmedTile);
        }
    }

    return retSet;
}

bool LaiRetrievalHandlerL3BNew::FilterTile(const QSet<QString> &tilesSet, const ProductDetails &prdDetails)
{
    const QStringList &tileIds = prdDetails.GetProduct().tiles;
    if (tileIds.size() == 0) {
        Logger::error(QStringLiteral("FilterTile: product %1 does not have any associated tiles!")
                      .arg(prdDetails.GetProduct().fullPath));
        return false;
    }
    return (tilesSet.empty() || tilesSet.contains(tileIds.at(0)));
}

void LaiRetrievalHandlerL3BNew::InitTileResultFiles(const TileInfos &tileInfo, TileResultFiles &tileResultFileInfo) {
    tileResultFileInfo.tileFile = tileInfo.tileFile;
    tileResultFileInfo.inPrdExtMsk = tileInfo.prdExternalMskFile;
    tileResultFileInfo.tileId = tileInfo.tileIds.at(0);
}

ProcessorJobDefinitionParams LaiRetrievalHandlerL3BNew::GetProcessingDefinitionImpl(SchedulingContext &ctx, int siteId, int scheduledDate,
                                                          const ConfigurationParameterValueMap &requestOverrideCfgValues)
{
    ProcessorJobDefinitionParams params;
    params.isValid = false;

    QDateTime seasonStartDate;
    QDateTime seasonEndDate;
    // extract the scheduled date
    QDateTime qScheduledDate = QDateTime::fromTime_t(scheduledDate);
    bool success = GetSeasonStartEndDates(ctx, siteId, seasonStartDate, seasonEndDate, qScheduledDate, requestOverrideCfgValues);
    // if cannot get the season dates
    if(!success) {
        success = GetBestSeasonToMatchDate(ctx, siteId, seasonStartDate, seasonEndDate, qScheduledDate, requestOverrideCfgValues);
        if(!success) {
            Logger::debug(QStringLiteral("Scheduler L3B: Error getting season start dates for site %1 for scheduled date %2!")
                          .arg(siteId)
                          .arg(qScheduledDate.toString()));
            return params;
        }
    }
    if(!seasonStartDate.isValid()) {
        Logger::error(QStringLiteral("Scheduler L3B: Season start date for site ID %1 is invalid in the database!")
                      .arg(siteId));
        return params;
    }

    ConfigurationParameterValueMap mapCfg = ctx.GetConfigurationParameters(QString("processor.l3b."), siteId, requestOverrideCfgValues);

    // we might have an offset in days from starting the downloading products to start the L3B production
    // TODO: Is this really needed
    int startSeasonOffset = mapCfg["processor.l3b.start_season_offset"].value.toInt();
    seasonStartDate = seasonStartDate.addDays(startSeasonOffset);

    params.jsonParameters = "{ \"scheduled_job\": \"1\"";

    // by default the start date is the season start date
    QDateTime startDate = seasonStartDate;
    QDateTime endDate = qScheduledDate;

    // Use only the products after the configured start season date
    if(startDate < seasonStartDate) {
        startDate = seasonStartDate;
    }

    params.jsonParameters.append(", \"start_date\": \"" + startDate.toString("yyyyMMdd") + "\", " +
                                 "\"end_date\": \"" + endDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_start_date\": \"" + seasonStartDate.toString("yyyyMMdd") + "\", " +
                                 "\"season_end_date\": \"" + seasonEndDate.toString("yyyyMMdd") + "\"}");
    params.isValid = true;

    return params;
}

int LaiRetrievalHandlerL3BNew::UpdateJobSubmittedParamsFromSchedReq(const L3BJobContext &jobCtx, ProductList &prdsToProcess) {
    int jobVal;
    QString strStartDate, strEndDate;
    if(ProcessorHandlerHelper::GetParameterValueAsInt(jobCtx.parameters, "scheduled_job", jobVal) && (jobVal == 1) &&
            ProcessorHandlerHelper::GetParameterValueAsString(jobCtx.parameters, "start_date", strStartDate) &&
            ProcessorHandlerHelper::GetParameterValueAsString(jobCtx.parameters, "end_date", strEndDate) &&
            jobCtx.parameters.contains("input_products")) {
        if (!jobCtx.parameters.contains("input_products") || jobCtx.parameters["input_products"].toArray().size() == 0) {
            auto startDate = ProcessorHandlerHelper::GetLocalDateTime(strStartDate);
            auto endDate = ProcessorHandlerHelper::GetLocalDateTime(strEndDate);

            QString strSeasonStartDate, strSeasonEndDate;
            QDateTime seasonStartDate, seasonEndDate;
            if (ProcessorHandlerHelper::GetParameterValueAsString(jobCtx.parameters, "season_start_date", strSeasonStartDate) &&
                    ProcessorHandlerHelper::GetParameterValueAsString(jobCtx.parameters, "season_end_date", strSeasonEndDate)) {
                seasonStartDate = ProcessorHandlerHelper::GetLocalDateTime(strSeasonStartDate);
                seasonEndDate = ProcessorHandlerHelper::GetLocalDateTime(strSeasonEndDate);
                // we consider only products in the current season
                if(startDate < seasonStartDate.addDays(-1)) {
                    startDate = seasonStartDate.addDays(-1);
                }
                if (endDate > seasonEndDate.addDays(1)) {
                    endDate = seasonEndDate.addDays(1);
                }
            }
            Logger::info(QStringLiteral("L3B Scheduled job received for siteId = %1, startDate=%2, endDate=%3").
                         arg(jobCtx.event.siteId).arg(startDate.toString("yyyyMMddTHHmmss")).arg(endDate.toString("yyyyMMddTHHmmss")));
            prdsToProcess  = GetL2AProductsNotProcessedProductProvenance(jobCtx, startDate, endDate);
            return prdsToProcess.size();
        }
    }
    return -1;
}

// TODO: This function should be updated to use the ProcessorHandlerHelper::EnsureMonoDateProductUniqueProc function
ProductList LaiRetrievalHandlerL3BNew::GetL2AProductsNotProcessedProductProvenance(const L3BJobContext &jobCtx,
                                                                  const QDateTime &startDate, const QDateTime &endDate) {

    // Get the list of L2A products already processed products as L3B
    const ProductList &existingPrds = jobCtx.pCtx->GetParentProductsInProvenance(jobCtx.event.siteId,
                                                {ProductType::L2AProductTypeId}, ProductType::L3BProductTypeId, startDate, endDate);
    // Get the list of L2A products NOT processed products as L3B
    const ProductList &missingPrds = jobCtx.pCtx->GetParentProductsNotInProvenance(jobCtx.event.siteId,
                                            {ProductType::L2AProductTypeId}, ProductType::L3BProductTypeId, startDate, endDate);

    std::unordered_map<int, int> mapPresence;
    std::for_each(existingPrds.begin(), existingPrds.end(), [&mapPresence](const Product &prd) {
        mapPresence[prd.productId] = 1;
    });

    ProductList newL2APrdsToProcess;

    Logger::info(QStringLiteral("Found a number of %1 L2A products not processed in L3B").arg(missingPrds.size()));
    if (missingPrds.size() == 0) {
        return newL2APrdsToProcess;
    }
    for (const Product &prd: missingPrds) {
        Logger::info(QStringLiteral("  ==> Missing L2A from L3B: %1").arg(prd.fullPath));
    }

    // Get the active jobs of this site
    const JobIdsList &activeJobIds = jobCtx.pCtx->GetActiveJobIds(this->processorDescr.processorId, jobCtx.event.siteId);

    // Get the file containing the product ids currently processing by all jobs of this site
    const QString &filePath = GetSiteCurrentProcessingPrdsFile(*jobCtx.pCtx, jobCtx.event.jobId, jobCtx.event.siteId);
    QDir().mkpath(QFileInfo(filePath).absolutePath());
    QFile file( filePath );
    // First read all the entries in the file to see what are the products that are currently processing
    QMap<int, int> curProcPrds;
    if (file.open(QIODevice::ReadOnly)) {
        QTextStream in(&file);
        while (!in.atEnd()) {
            const QString &line = in.readLine();
            const QStringList &pieces = line.split(';');
            if (pieces.size() == 2) {
                int prdId = pieces[0].toInt();
                int jobId = pieces[1].toInt();
                // add only product ids that were not yet processed or that are currently processing in some active jobs
                if (prdId != 0) {
                    // maybe the product was already created meanwhile by a custom job, in this case we should remove it
                    if (mapPresence.find(prdId) != mapPresence.end()) {
                        continue;
                    }
                    // if the job processing the product is still active, keep the product
                    if (activeJobIds.contains(jobId)) {
                        curProcPrds[prdId] = jobId;
                    }
                }
            }
        }
        file.close();
    }
    // add the products that will be processed next
    for (int i = 0; i<missingPrds.size(); i++) {
        if (curProcPrds.find(missingPrds[i].productId) == curProcPrds.end()) {
            curProcPrds[missingPrds[i].productId] = jobCtx.event.jobId;
            newL2APrdsToProcess.append(missingPrds[i]);
        }
        // else, if the product was already in this list, then it means it was already scheduled for processing
        // by another schedule operation
    }

    if ( file.open(QIODevice::ReadWrite | QFile::Truncate) )
    {
        QTextStream stream( &file );
        for(auto prdInfo : curProcPrds.keys()) {
            stream << prdInfo << ";" << curProcPrds.value(prdInfo) << endl;
        }
    }

    Logger::info(QStringLiteral("A number of %1 L2A products needs to be processed in L3B after checking already launched products").
                 arg(newL2APrdsToProcess.size()));
    return newL2APrdsToProcess;
}

QString LaiRetrievalHandlerL3BNew::GetSiteCurrentProcessingPrdsFile(EventProcessingContext &ctx, int jobId, int siteId) {
    return QDir::cleanPath(GetFinalProductFolder(ctx, jobId, siteId) + QDir::separator() + CURRENT_PROC_PRDS_FILE_NAME);
}

/*
ProductList LaiRetrievalHandlerL3BNew::GetL2AProductsNotProcessed(const L3BJobContext &jobCtx, const QDateTime &startDate, const QDateTime &endDate) {
    QStringList newRelL2APathsToProcess;
    ProductList newL2APrdsToProcess;

    ProductList l2aProducts;
    QStringList fullL2APaths;
    QStringList fullL2APathsFromL3Bs;
    Logger::info("Extracting L2A from DB...");
    const QStringList &l2aRelPathsFromDb = GetL2ARelPathsFromDB(*jobCtx.pCtx, jobCtx.event.siteId, startDate, endDate, fullL2APaths, l2aProducts);
    Logger::info("Extracting L2A from DB...DONE! ");
    for (const QString &relPath: l2aRelPathsFromDb) {
        Logger::info(QStringLiteral("  ==> DB L2A: %1").arg(relPath));
    }
    Logger::info(QStringLiteral("Extracted a number of %1 L2A products from DB. Extracting the L2A from L3B products ...").arg(l2aRelPathsFromDb.size()));
    // Get the relative paths of the products from the L3B products
    const QStringList &l2aRelPathsFromL3B = GetL2ARelPathsFromProcessedL3Bs(*jobCtx.pCtx, jobCtx.event.siteId, startDate, endDate, fullL2APathsFromL3Bs);
    Logger::info(QStringLiteral("Extracted a number of %1 L2A products from the L3B products").arg(l2aRelPathsFromL3B.size()));
    for (const QString &relPath: l2aRelPathsFromL3B) {
        Logger::info(QStringLiteral("  ==> L3B L2A: %1").arg(relPath));
    }

    QStringList missingPrdsPaths;
    ProductList missingPrds;
    for(int i = 0; i < l2aRelPathsFromDb.size(); i++) {
        if (!l2aRelPathsFromL3B.contains(l2aRelPathsFromDb[i])) {
            missingPrdsPaths.append(l2aRelPathsFromDb[i]);
            missingPrds.append(l2aProducts[i]);
        }
    }
    Logger::info(QStringLiteral("Found a number of %1 L2A products not processed in L3B").arg(missingPrds.size()));
    if (missingPrds.size() == 0) {
        return newL2APrdsToProcess;
    }
    for (const Product &prd: missingPrds) {
        Logger::info(QStringLiteral("  ==> Missing L2A from L3B: %1").arg(prd.fullPath));
    }

    const std::map<QString, QString> &mapCfg = jobCtx.pCtx->GetConfigurationParameters(PRODUCTS_LOCATION_CFG_KEY);
    std::map<QString, QString>::const_iterator it = mapCfg.find(PRODUCTS_LOCATION_CFG_KEY);
    QString fileParentPath;
    if (it != mapCfg.end()) {
        fileParentPath = it->second;
    } else {
        fileParentPath = "/mnt/archive/{site}/{processor}/";
    }
    fileParentPath = fileParentPath.replace("{site}", jobCtx.pCtx->GetSiteShortName(jobCtx.event.siteId));
    fileParentPath = fileParentPath.replace("{processor}", processorDescr.shortName);
    const QString &filePath = QDir::cleanPath(fileParentPath + QDir::separator() + "current_processing_l3b.txt");

    QDir().mkpath(QFileInfo(filePath).absolutePath());
    QFile file( filePath );
    // First read all the entries in the file to see what are the products that are currently processing

    QStringList curProcPrds;
    if (file.open(QIODevice::ReadOnly))
    {
       QTextStream in(&file);
       while (!in.atEnd())
       {
          curProcPrds.append(in.readLine());
       }
       file.close();
    }
    if (curProcPrds.size() > 0) {
        // remove already processed L2A products from this file
        for(const QString &l2aRelPath: l2aRelPathsFromL3B) {
            curProcPrds.removeAll(l2aRelPath);
        }
    }
    // add the products that will be processed next
    for (int i = 0; i<missingPrdsPaths.size(); i++) {
        const QString &l2aRelPath =  missingPrdsPaths[i];
        if (!curProcPrds.contains(l2aRelPath)) {
            curProcPrds.append(l2aRelPath);
            newRelL2APathsToProcess.append(l2aRelPath);
            newL2APrdsToProcess.append(missingPrds[i]);
        }
        // else, if the product was already in this list, then it means it was already scheduled for processing
        // by another schedule operation
    }

    if ( file.open(QIODevice::ReadWrite | QFile::Truncate) )
    {
        QTextStream stream( &file );
        for (const QString &prdPath: curProcPrds) {
            stream << prdPath << endl;
        }
    }

    Logger::info(QStringLiteral("A number of %1 L2A products needs to be processed in L3B after checking already launched products").
                 arg(newL2APrdsToProcess.size()));
    return newL2APrdsToProcess;
}

QStringList LaiRetrievalHandlerL3BNew::GetL2ARelPathsFromDB(EventProcessingContext &ctx, int siteId,
                                                            const QDateTime &startDate, const QDateTime &endDate,
                                                            QStringList &retFullPaths, ProductList &prdList) {
    QStringList listValidTilesMetaFiles;
    const ProductList &l2aPrds = ctx.GetProducts(siteId, (int)ProductType::L2AProductTypeId, startDate, endDate);
    for(const Product &prd: l2aPrds) {
        const QStringList &metaFiles = EventProcessingContext::findProductFiles(prd.fullPath);
        for(const QString &tileMetaFile: metaFiles) {
            if(ProcessorHandlerHelper::IsValidL2AMetadataFileName(tileMetaFile)) {
                // remove the full path from DB
                QString metaRelPath = tileMetaFile;
                metaRelPath.remove(0, prd.fullPath.size());
                // build the relative path
                const QString &relPath = QDir::cleanPath(QDir(prd.fullPath).dirName() + QDir::separator() + metaRelPath);
                if (!listValidTilesMetaFiles.contains(relPath)) {
                    listValidTilesMetaFiles.append(relPath);
                    retFullPaths.append(tileMetaFile);
                    prdList.append(prd);
                }
            }
        }
    }

    return listValidTilesMetaFiles;
}

QStringList LaiRetrievalHandlerL3BNew::GetL2ARelPathsFromProcessedL3Bs(EventProcessingContext &ctx, int siteId,
                                                                       const QDateTime &startDate, const QDateTime &endDate,
                                                                       QStringList &retFullPaths) {
    // Get the products from the L3B products
    Logger::info(QStringLiteral("Extracting L3B from DB... "));
    const ProductList &l3bPrds = ctx.GetProducts(siteId, (int)ProductType::L3BProductTypeId, startDate, endDate);
    Logger::info(QStringLiteral("Extracting L3B from DB...DONE"));
    // Now search in the IPP XML files from each L3B product the L2A products that genetated it
    QStringList l2aRelPathsFromL3B;
    Logger::info(QStringLiteral("Extracting L2A from L3B...!"));
    for(const Product &prd: l3bPrds) {
        Logger::info(QStringLiteral("  ==> L3B: %1").arg(prd.fullPath));
        const QStringList &l2aSrcPrds = GetL3BSourceL2APrdsPaths(prd.fullPath);
        // we can have the situation when the L2A products were moved so they are now in another location
        for(const QString &l2aSrcPrd: l2aSrcPrds) {
            const QStringList &pathComponents = l2aSrcPrd.split("/", QString::SkipEmptyParts);
            // get the product name and the metadata file (and intermediate subfolders, if needed)
            QString relPath;
            int numComps = 2;
            if (pathComponents.size() >= 3 && pathComponents[pathComponents.size() - 1].endsWith("_MTD_ALL.xml")) {
                numComps = 3;
            }
            int prdNameCompIdx = pathComponents.size() - numComps;
            for(int i = prdNameCompIdx; i<pathComponents.size(); i++) {
                relPath.append(pathComponents[i]);
                if (i < pathComponents.size()-1) {
                    relPath.append(QDir::separator());
                }
            }
            if (relPath.size() > 0 && !l2aRelPathsFromL3B.contains(relPath)) {
                l2aRelPathsFromL3B.append(relPath);
                retFullPaths.append(l2aSrcPrd);
            }
        }
    }
    Logger::info(QStringLiteral("Extracting L2A from L3B...DONE!"));
    return  l2aRelPathsFromL3B;
}

QStringList LaiRetrievalHandlerL3BNew::GetL3BSourceL2APrdsPaths(const QString &prdPath) {
    QStringList retPrdsList;

    QDir directory(QDir::cleanPath(prdPath + QDir::separator() + "AUX_DATA"));
    const QStringList &ippFiles = directory.entryList(QStringList() << "S2AGRI_L3B_IPP_A*.xml",QDir::Files);
    if (ippFiles.size() == 0) {
        return retPrdsList;
    }
    const QString &ippFilePath = directory.filePath(ippFiles[0]);
    QFile file(ippFilePath);
    if(!file.open(QFile::ReadOnly | QFile::Text)){
        qDebug() << "Cannot read file" << file.errorString();
        return retPrdsList;
    }

    QXmlStreamReader reader(&file);
    if (reader.readNextStartElement()) {
//        qDebug() << reader.name();
        if (reader.name() == "metadata") {
            while (reader.readNextStartElement()) {
//                qDebug() << reader.name();
                if (reader.name() == "XML_files") {
                    while(reader.readNextStartElement()){
//                        qDebug() << reader.name();
                        if (reader.name().startsWith("XML_")) {
                            const QString &s = reader.readElementText();
                            retPrdsList.append(s);
//                            qDebug() << "Product path: " << s;
                        }
                    }
                    // no need to continue
                    break;
                } else {
                    reader.skipCurrentElement();
                }
            }
        }
    }

    return retPrdsList;
}
*/
