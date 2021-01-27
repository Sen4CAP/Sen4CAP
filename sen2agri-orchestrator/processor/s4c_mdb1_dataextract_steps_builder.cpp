#include <QJsonDocument>
#include <QJsonObject>
#include <QRegularExpression>
#include <fstream>

#include "s4c_mdb1_dataextract_steps_builder.hpp"
#include "processorhandlerhelper.h"
#include "json_conversions.hpp"
#include "logger.hpp"
#include "s4c_utils.hpp"
#include "stepexecutiondecorator.h"

#define MDB1_DEF_TS_ROOT          "/mnt/archive/marker_database_files/mdb1/{site}/{year}/"
#define MDB1_DEF_DATA_EXTR_ROOT   "/mnt/archive/marker_database_files/mdb1/{site}/{year}/data_extraction/"
#define SECS_TILL_EOD               86399   // 24 hour x 3600 + 59 minutes x 60 + 59 seconds


#define L3B_REGEX       R"(S2AGRI_L3B_S(NDVI|LAIMONO|FAPARMONO|FCOVERMONO)_A(\d{8})T.*\.TIF)"
#define S1_REGEX        R"(SEN4CAP_L2A_.*_V(\d{8})T\d{6}_(\d{8})T\d{6}_(VH|VV)_(\d{3})_(?:.+)?(AMP|COHE)\.tif)"
#define L3B_REGEX_DATE_IDX          2
#define S1_REGEX_DATE_IDX           1
#define S1_REGEX_DATE2_IDX          2


S4CMarkersDB1DataExtractStepsBuilder::S4CMarkersDB1DataExtractStepsBuilder()
{
}


QList<MarkerType> S4CMarkersDB1DataExtractStepsBuilder::GetEnabledMarkers() const {
    return enabledMarkers;
}

void S4CMarkersDB1DataExtractStepsBuilder::GetDataExtractionInterval(QDateTime &minDate, QDateTime &maxDate) const {
    minDate = prdMinDate;
    maxDate = prdMaxDate;
}

QString S4CMarkersDB1DataExtractStepsBuilder::GetDataExtractionDir(const QString &markerName) const {
    return GetDataExtractionDir(prdMaxDate.date().year(), markerName);
}

QString S4CMarkersDB1DataExtractStepsBuilder::GetDataExtractionDir(int year, const QString &markerName) const {
    QString val = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, "data_extr_dir", MDB1_CFG_PREFIX);
    if (val.size() == 0) {
        val = MDB1_DEF_DATA_EXTR_ROOT;
    }
    val = val.replace("{site}", siteShortName);
    val = val.replace("{year}", QString::number(year));
    if (val.indexOf("{product_type}") == -1) {
        // force the product type to be added at the end of the directory
        return QDir(val).filePath(markerName);
    }
    return val.replace("{product_type}", markerName);
}

void S4CMarkersDB1DataExtractStepsBuilder::CreateTasks(const MarkerType &marker, QList<TaskToSubmit> &outAllTasksList, int &curTaskIdx) const
{
    Logger::info(QStringLiteral("MDB1 Create tasks: having a number of %1 tasks to add").arg(fileInfos.size()));

    // create for all products of this marker the data extraction tasks, if needed
    for(int i  = 0; i<fileInfos.size(); i++) {
        if (fileInfos[i].markerInfo.marker == marker.marker) {
            outAllTasksList.append(TaskToSubmit{ "mdb1-data-extraction", {} });
            curTaskIdx++;
        }
    }
}

void S4CMarkersDB1DataExtractStepsBuilder::CreateSteps(const MarkerType &marker, QList<TaskToSubmit> &allTasksList,
                                                       NewStepList &steps, int &curTaskIdx, QStringList &dataExtrDirs) const
{
    for(int i  = 0; i<fileInfos.size(); i++) {
        const PrdMarkerInfo &prdMarkerInfo = fileInfos[i];
        if (prdMarkerInfo.markerInfo.marker == marker.marker) {
            TaskToSubmit &dataExtractionTask = allTasksList[curTaskIdx++];
            QString dataExtrDirName;
            if (!isScheduledJob) {
                dataExtrDirName = dataExtractionTask.GetFilePath("");
            } else {
                dataExtrDirName = GetDataExtractionDir(prdMarkerInfo.prdFileInfo.prdTime.date().year(), marker.marker);
            }
            if (!dataExtrDirs.contains(dataExtrDirName)) {
                QDir().mkpath(dataExtrDirName);
                dataExtrDirs.append(dataExtrDirName);
            }
            const QStringList &dataExtractionArgs = GetDataExtractionArgs("NewID", prdMarkerInfo, dataExtrDirName);
            steps.append(StepExecutionDecorator::GetInstance()->CreateTaskStep(parentProcessorName, dataExtractionTask,
                                                                               "Markers1Extractor", dataExtractionArgs));
        }
    }
}

void S4CMarkersDB1DataExtractStepsBuilder::Initialize(const QString &parentProc, EventProcessingContext &ctx, const JobSubmittedEvent &evt,
                                                      const QStringList &markersEnabled)
{
    allMarkerFileTypes = {{"NDVI", ProductType::L3BProductTypeId, "NDVI"},
                            {"LAI", ProductType::L3BProductTypeId, "LAIMONO"},
                            {"FAPAR", ProductType::L3BProductTypeId, "FAPARMONO"},
                            {"FCOVER", ProductType::L3BProductTypeId, "FCOVERMONO"},
                            {"AMP", ProductType::S4CS1L2AmpProductTypeId, "AMP"},
                            {"COHE", ProductType::S4CS1L2CoheProductTypeId, "COHE"}};

    parentProcessorName = parentProc;
    pCtx = &ctx;
    event = evt;
    parameters = QJsonDocument::fromJson(evt.parametersJson.toUtf8()).object();
    configParameters = pCtx->GetJobConfigurationParameters(evt.jobId, MDB1_CFG_PREFIX);
    siteId = evt.siteId;
    siteShortName = pCtx->GetSiteShortName(evt.siteId);
    isScheduledJob = IsScheduledJobRequest(parameters);

    bHasL3BMarkers = false;
    // check the enabled markers
    for (const auto &marker: allMarkerFileTypes) {
        bool markerEnabled = false;
        if (markersEnabled.size() > 0) {
            if (markersEnabled.contains(marker.marker)) {
                markerEnabled = true;
            }
        } else {
            markerEnabled = ProcessorHandlerHelper::GetBoolConfigValue(parameters, configParameters,
                                                                       marker.marker.toLower() + "_enabled", MDB1_CFG_PREFIX);
        }
        if (markerEnabled) {
            enabledMarkers.push_back(marker);
            if (marker.prdType == ProductType::L3BProductTypeId) {
                bHasL3BMarkers = true;
            }
        }
    }

    // Here we check if there are files that do not have the data extraction performed within the
    //      given interval if the requested operation is Export IPC only
    //      In this case we should force a DataExtraction for the missing files
    UpdateJobSubmittedParamsFromSchedReq();

    ExtractProductFiles();

    // Get the list of most recent LPIS products per year, according to min and max dates extracted from files
    lpisInfos = ExtractLpisInfos();

    // Update file infos informations with lpis infos, if
    RemoveNoLpisProducts();
}



QDateTime S4CMarkersDB1DataExtractStepsBuilder::ExtractDateFromRegex(const QString &fileName, const QString &regex, int minDateGrpIdx, int maxDateGrpIdx) {
    QRegExp rx(regex);
    if (rx.indexIn(fileName) == -1) {
        return QDateTime();
    }
    const QStringList &list = rx.capturedTexts();
    if (minDateGrpIdx < list.size()) {
        const QString &minDateStr = list.at(minDateGrpIdx);
        const QDate &minDate = QDate::fromString(minDateStr, "yyyyMMdd");
        if (maxDateGrpIdx != -1 && maxDateGrpIdx < list.size()) {
            const QString &maxDateStr = list.at(minDateGrpIdx);
            const QDate &maxDate = QDate::fromString(maxDateStr, "yyyyMMdd");
            if (maxDate > minDate) {
                return QDateTime(maxDate);
            }
        }
        return QDateTime(minDate);
    }
    return QDateTime();
}

void S4CMarkersDB1DataExtractStepsBuilder::ExtractProductFiles()
{
    QDateTime prdMinDate;
    QDateTime prdMaxDate;

    if (enabledMarkers.size() > 0) {
        if (bHasL3BMarkers) {
            // Handle S3 Markers
            // This customization for L3B is made in order to avoid reading several times the input products and
            // also because one L3B contains several markers
            const QStringList &l3bPrds = S4CUtils::GetInputProducts(*(pCtx), event, ProductType::L3BProductTypeId,
                                                                    prdMinDate, prdMaxDate, isScheduledJob);
            Logger::info(QStringLiteral("Extracted a number of %1 L3B products").arg(l3bPrds.size()));
            if (l3bPrds.size() > 0) {
                for (const auto &marker: enabledMarkers) {
                    if (marker.prdType == ProductType::L3BProductTypeId) {
                        const QStringList &tiffFiles = S4CUtils::FindL3BProductTiffFiles(*(pCtx), event, l3bPrds, MDB1_CFG_PREFIX,
                                                                                         "S" + marker.markerSubstrInFileName);
                        for (const auto &tiffFile : tiffFiles) {
                            const QDateTime &prdDate = ExtractDateFromRegex(tiffFile, L3B_REGEX, L3B_REGEX_DATE_IDX);
                            if (prdDate.isValid()) {
                                fileInfos.push_back({marker, tiffFile, prdDate});
                            }
                        }
                    }
                }
            }
        }
        for (const auto &marker: enabledMarkers) {
            // Handle S1 markers
            if (marker.prdType != ProductType::L3BProductTypeId) {
                const QStringList &tiffFiles = S4CUtils::GetInputProducts(*(pCtx), event, marker.prdType,
                                                  prdMinDate, prdMaxDate, isScheduledJob);
                Logger::info(QStringLiteral("Extracted a number of %1 %2 products").arg(tiffFiles.size()).arg(marker.marker));
                for (const auto &tiffFile : tiffFiles) {
                    const QDateTime &prdDate = ExtractDateFromRegex(tiffFile, S1_REGEX, S1_REGEX_DATE_IDX, S1_REGEX_DATE2_IDX);
                    if (prdDate.isValid()) {
                        fileInfos.push_back({marker, tiffFile, prdDate});
                    }
                }
            }
        }
    } else {
        Logger::error(QStringLiteral("No enabled markers for job %1!!!").arg(event.jobId));
    }

    Logger::info(QStringLiteral("A total number of %1 tiff files will be processed").arg(fileInfos.size()));

    // Update the dates used by the product formatter
    if (!this->prdMinDate.isValid()) {
        this->prdMinDate = prdMinDate;
    }
    if (!this->prdMaxDate.isValid()) {
        this->prdMaxDate = prdMaxDate;
    }
}

QMap<int, LpisInfos> S4CMarkersDB1DataExtractStepsBuilder::ExtractLpisInfos() {
    // We take it the last LPIS product for this site.
    const ProductList &lpisPrds = GetLpisProduct(pCtx, siteId);
    if (lpisPrds.size() == 0) {
        pCtx->MarkJobFailed(event.jobId);
        throw std::runtime_error(QStringLiteral("No LPIS product found in database for the agricultural practices execution for site %1.").
                                 arg(siteShortName).toStdString());
    }

    QMap<int, LpisInfos> lpisInfos;
    QString allFilesRegex = "decl_" + siteShortName + "_\\d{4}.csv";
    QRegularExpression re(allFilesRegex);
    for(const Product &lpisPrd: lpisPrds) {
        // TODO: Here we should use the inserted date? Or maybe both?
        // ignore LPIS products from a year where we already added an LPIS product newer
        QMap<int, LpisInfos>::const_iterator i = lpisInfos.find(lpisPrd.created.date().year());
        if (i != lpisInfos.end()) {
            if (lpisPrd.inserted < i.value().insertedDate) {
                continue;
            }
        }

        // If the year is >= 2019, then use LAEA for AMP and COHE and no matter which other for NDVI
        //const QString &prdLpisPath = lpisPrds[lpisPrds.size()-1].fullPath;
        QDir directory(lpisPrd.fullPath);


        const QStringList &dirFiles = directory.entryList(QStringList() << "*.shp" << "*.csv",QDir::Files);
        LpisInfos lpisInfo;
        foreach(const QString &fileName, dirFiles) {
            // we don't want for NDVI the LAEA projection
            if (fileName.endsWith("_buf_5m.shp") && (!fileName.endsWith("_3035_buf_5m.shp")) && (lpisInfo.ndviIdsGeomShapePath.size() == 0)) {
                lpisInfo.ndviIdsGeomShapePath = directory.filePath(fileName);
            }
            // LAEA projection have priority for 10m buffer
            if (fileName.endsWith("_3035_buf_10m.shp") ||
                    (fileName.endsWith("_buf_10m.shp") && lpisInfo.ampCoheIdsGeomShapePath.size() == 0)) {
                lpisInfo.ampCoheIdsGeomShapePath = directory.filePath(fileName);
            }
            // we know its name but we want to be sure it is there
            if (re.match(fileName).hasMatch()) {
                lpisInfo.fullDeclsFilePath = directory.filePath(fileName);
            }
        }
        if (lpisInfo.ndviIdsGeomShapePath.size() != 0 &&
                lpisInfo.ampCoheIdsGeomShapePath.size() != 0 &&
                lpisInfo.fullDeclsFilePath.size() != 0) {
            lpisInfo.productDate = lpisPrd.created;
            lpisInfo.insertedDate = lpisPrd.inserted;
            lpisInfos[lpisInfo.productDate.date().year()] = lpisInfo;
            Logger::info(QStringLiteral("MDB1: Using LPIS %1 for year %2").arg(lpisPrd.fullPath).arg(lpisInfo.productDate.date().year()));
        }
    }

    return lpisInfos;
}

void S4CMarkersDB1DataExtractStepsBuilder::RemoveNoLpisProducts() {
    auto const pred = [this](const PrdMarkerInfo &prdInfo){
        QMap<int, LpisInfos>::const_iterator i = lpisInfos.find(prdInfo.prdFileInfo.prdTime.date().year());
        if (i == lpisInfos.end()) {
            Logger::info(QStringLiteral("MDB1: Product %1 not processed as there is no LPIS for product year %2").
                         arg(prdInfo.prdFileInfo.inFilePath).arg(prdInfo.prdFileInfo.prdTime.date().year()));
            return true;
        }
        return false;
    };
    int prevFileInfos = fileInfos.size();
    fileInfos.erase(std::remove_if(fileInfos.begin(), fileInfos.end(), pred), fileInfos.end());
    Logger::info(QStringLiteral("Removed a number of %1 products that do not have a year corresponding to an LPIS").arg(prevFileInfos - fileInfos.size()));
}

QStringList S4CMarkersDB1DataExtractStepsBuilder::GetDataExtractionArgs(const QString &uidField,
                                                         const PrdMarkerInfo &inputFileInfo, const QString &outDir) const
{
    QStringList retArgs = { "Markers1Extractor", "-field", uidField,
                            "-prdtype", inputFileInfo.markerInfo.marker,
                            "-outdir", outDir, "-il", inputFileInfo.prdFileInfo.inFilePath };
    const QString *idsGeomShapePath;
    QMap<int, LpisInfos>::const_iterator i = lpisInfos.find(inputFileInfo.prdFileInfo.prdTime.date().year());
    if (inputFileInfo.markerInfo.prdType == ProductType::L3BProductTypeId) {
        idsGeomShapePath = &(i.value().ndviIdsGeomShapePath);   // we can do that as we previously removed the products that do not have LPIS
    } else {
        idsGeomShapePath = &(i.value().ampCoheIdsGeomShapePath);
    }
    if (idsGeomShapePath->size() > 0) {
        retArgs += "-vec";
        retArgs += *idsGeomShapePath;
    }
    return retArgs;
}

ProductList S4CMarkersDB1DataExtractStepsBuilder::GetLpisProduct(ExecutionContextBase *pCtx, int siteId) {
    // We take it the last LPIS product for this site.
    QDate  startDate, endDate;
    startDate.setDate(1970, 1, 1);
    QDateTime startDateTime(startDate);
    endDate.setDate(2050, 12, 31);
    QDateTime endDateTime(endDate);
    return pCtx->GetProducts(siteId, (int)ProductType::S4CLPISProductTypeId, startDateTime, endDateTime);
}

QString S4CMarkersDB1DataExtractStepsBuilder::GetShortNameForProductType(const ProductType &prdType) {
    switch(prdType) {
        case ProductType::L3BProductTypeId:         return "L3B";
        case ProductType::S4CS1L2AmpProductTypeId:  return "AMP";
        case ProductType::S4CS1L2CoheProductTypeId: return "COHE";
        default:                                    return "";
    }
}

int S4CMarkersDB1DataExtractStepsBuilder::UpdateJobSubmittedParamsFromSchedReq() {
    QString strStartDate, strEndDate;
    if(isScheduledJob &&
            ProcessorHandlerHelper::GetParameterValueAsString(parameters, "start_date", strStartDate) &&
            ProcessorHandlerHelper::GetParameterValueAsString(parameters, "end_date", strEndDate)) {

        QString strSeasonStartDate, strSeasonEndDate;
        ProcessorHandlerHelper::GetParameterValueAsString(parameters, "season_start_date", strSeasonStartDate);
        ProcessorHandlerHelper::GetParameterValueAsString(parameters, "season_end_date", strSeasonEndDate);

        // TODO: Should we use here ProcessorHandlerHelper::GetLocalDateTime ???

        seasonStartDate = QDateTime::fromString(strSeasonStartDate, "yyyyMMdd");
        seasonEndDate = QDateTime::fromString(strSeasonEndDate, "yyyyMMdd").addSecs(SECS_TILL_EOD);

        const auto &startDate = QDateTime::fromString(strStartDate, "yyyyMMdd");
        const auto &endDate = QDateTime::fromString(strEndDate, "yyyyMMdd").addSecs(SECS_TILL_EOD);

        // set these values by default
        prdMinDate = startDate;
        prdMaxDate = endDate;

        Logger::info(QStringLiteral("Markers Extractor Scheduled job received for site name = %1, startDate=%2, endDate=%3").
                     arg(siteShortName).arg(startDate.toString("yyyyMMdd")).arg(endDate.toString("yyyyMMdd")));

        QJsonArray ndviInputProductsArr, ampInputProductsArr, coheInputProductsArr;
        for(const auto &marker: enabledMarkers) {
            QList<PrdFileInfo> processedFiles;
            const QList<PrdFileInfo> &missingPrdsList = ExtractMissingDataExtractionProducts(marker,
                                                                          startDate, endDate, processedFiles);
            const QList<PrdFileInfo> &filteredPrdsList = FilterAndUpdateAlreadyProcessingPrds(missingPrdsList,
                                                                                           processedFiles, marker);
            switch(marker.prdType) {
                case ProductType::L3BProductTypeId:
                    AddProductListToJSonArray(filteredPrdsList, ndviInputProductsArr);
                    break;
                case ProductType::S4CS1L2AmpProductTypeId:
                    AddProductListToJSonArray(filteredPrdsList, ampInputProductsArr);
                    break;
                case ProductType::S4CS1L2CoheProductTypeId:
                    AddProductListToJSonArray(filteredPrdsList, coheInputProductsArr);
                    break;
                default:
                    break;
            }
        }
        Logger::info(QStringLiteral("Agricultural Practices Scheduled job : Updating input products for jobId = %1, site name = %2 with a "
                                    "number of %3 NDVI products, %4 AMP products and %5 COHE products").
                     arg(event.jobId).arg(siteShortName).arg(ndviInputProductsArr.size()).
                     arg(ampInputProductsArr.size()).arg(coheInputProductsArr.size()));

        if (ndviInputProductsArr.size() > 0) {
            parameters[QStringLiteral("input_NDVI")] = ndviInputProductsArr;
        }
        if (ampInputProductsArr.size() > 0) {
            parameters[QStringLiteral("input_AMP")] = ampInputProductsArr;
        }
        if (coheInputProductsArr.size() > 0) {
            parameters[QStringLiteral("input_COHE")] = coheInputProductsArr;
        }

        event.parametersJson = jsonToString(parameters);
        return ndviInputProductsArr.size() + ampInputProductsArr.size() + coheInputProductsArr.size();
    }
    return -1;
}

QList<PrdFileInfo> S4CMarkersDB1DataExtractStepsBuilder::ExtractMissingDataExtractionProducts(
                                                                        const MarkerType &markerType, const QDateTime &startDate,
                                                                        const QDateTime &endDate, QList<PrdFileInfo> &alreadyProcessedFiles) {
    const ProductList &prds = pCtx->GetProducts(siteId, (int)markerType.prdType, startDate, endDate);
    QList<PrdFileInfo> retListUnprocessed;
    for(const Product &prd: prds) {
        const QString &dataExtrDirName = GetDataExtractionDir(prd.created.date().year(), markerType.marker);
        QStringList prdFileNames {prd.fullPath};
        if (markerType.prdType == ProductType::L3BProductTypeId) {
            prdFileNames = S4CUtils::FindL3BProductTiffFiles(prd.fullPath, {}, "S" + markerType.markerSubstrInFileName);
        }
        for(const QString &prdFileName: prdFileNames) {
            if (!IsDataExtractionPerformed(dataExtrDirName, prdFileName)) {
                retListUnprocessed.append({prdFileName, prd.created});
            } else {
                alreadyProcessedFiles.append({prdFileName, prd.created});
            }
        }
    }
    return retListUnprocessed;
}

bool S4CMarkersDB1DataExtractStepsBuilder::IsDataExtractionPerformed(const QString &dataExtrDirPath, const QString &prdPath) {
    QFileInfo fileInfo(prdPath);
    QDir directory(dataExtrDirPath);
    // empty parameters
    const QString & fileNameNoExt = fileInfo.completeBaseName();
    const QStringList &cvsFiles = directory.entryList(QStringList() << fileNameNoExt + ".csv",QDir::Files);
    if (cvsFiles.size() > 0) {
        return true;
    }

    return false;
}

/**
 * @brief S4CMarkersDB1Handler::FilterAndUpdateAlreadyProcessingPrds
 *  Returns the products that were not already launched for processing in case of overlapping schedulings
 */
QList<PrdFileInfo> S4CMarkersDB1DataExtractStepsBuilder::FilterAndUpdateAlreadyProcessingPrds(const QList<PrdFileInfo> &missingPrdsFiles,
                                                                        const QList<PrdFileInfo> &processedPrdsFiles, const MarkerType &markerType) {
    QList<PrdFileInfo> filteredPrds;

    const QMap<int, QList<PrdFileInfo>> &mapMissing = GroupProductFileInfosByYear(missingPrdsFiles);
    const QMap<int, QList<PrdFileInfo>> &mapProcessed = GroupProductFileInfosByYear(processedPrdsFiles);
    QList<int> allYears = mapMissing.keys();
    allYears.append(mapProcessed.keys());
    // keep common unique years from missing and processed products
    const QSet<int> &setYears = QSet<int>::fromList(allYears);
    for (int year: setYears) {
        const QList<PrdFileInfo> &yearMissingPrdsFiles = mapMissing[year];
        const QList<PrdFileInfo> &yearProcessedPrdsFiles = mapProcessed[year];

        const QString &dataExtrDirName = GetDataExtractionDir(year, markerType.marker);
        const QString &filePath = QDir::cleanPath(dataExtrDirName + QDir::separator() + markerType.marker +
                                                  "_current_data_extraction_products.txt");
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
            for(const PrdFileInfo &prdFileInfo: yearProcessedPrdsFiles) {
                curProcPrds.removeAll(prdFileInfo.inFilePath);
            }
        }
        // add the products that will be processed next
        for (int i = 0; i<yearMissingPrdsFiles.size(); i++) {
            const PrdFileInfo &prdFile =  yearMissingPrdsFiles[i];
            if (!curProcPrds.contains(prdFile.inFilePath)) {
                curProcPrds.append(prdFile.inFilePath);
                filteredPrds.append(prdFile);
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
    }

    return filteredPrds;
}

void S4CMarkersDB1DataExtractStepsBuilder::AddProductListToJSonArray(const QList<PrdFileInfo> &prdList, QJsonArray &retArr) {
    // we consider only products in the current season
    for (const PrdFileInfo &prd: prdList) {
        retArr.append(prd.inFilePath);
    }
}

bool S4CMarkersDB1DataExtractStepsBuilder::IsScheduledJobRequest(const QJsonObject &parameters) {
    int jobVal;
    return ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "scheduled_job", jobVal) && (jobVal == 1);
}

//bool S4CMarkersDB1DataExtractStepsBuilder::CheckExecutionPreconditions(ExecutionContextBase *pCtx, const std::map<QString, QString> &configParameters,
//                                                        int siteId, const QString &siteShortName, QString &errMsg) {
//    errMsg = "";
//    // We take it the last LPIS product for this site.
//    const ProductList &lpisPrds = GetLpisProduct(pCtx, siteId);
//    if (lpisPrds.size() == 0) {
//        errMsg = QStringLiteral("ERROR Markers DB 1: No LPIS product found for site %1.").
//                                 arg(siteShortName);
//        return false;
//    }

//    return true;
//}

QMap<int, QList<PrdFileInfo>> S4CMarkersDB1DataExtractStepsBuilder::GroupProductFileInfosByYear(const QList<PrdFileInfo> &fileInfos)
{
    QMap<int, QList<PrdFileInfo>> retMap;
    for(const PrdFileInfo &fileInfo: fileInfos) {
        QMap<int, QList<PrdFileInfo>>::iterator i = retMap.find(fileInfo.prdTime.date().year());
        if (i != retMap.end()) {
            i->append(fileInfo);
        } else {
            retMap[fileInfo.prdTime.date().year()] = {fileInfo};
        }
    }
    return retMap;
}

