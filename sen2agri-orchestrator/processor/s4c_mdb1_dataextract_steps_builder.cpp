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
#include <unordered_map>

#include <memory>

#include "products/producthelper.h"
#include "products/producthelperfactory.h"
using namespace orchestrator::products;

#define MDB1_DEF_DATA_EXTR_ROOT   "/mnt/archive/marker_database_files/mdb1/{site}/{year}/data_extraction/"
#define SECS_TILL_EOD               86399   // 24 hour x 3600 + 59 minutes x 60 + 59 seconds


// For unordered map and QString as key
namespace std {
  template<> struct hash<QString> {
    std::size_t operator()(const QString& s) const noexcept {
      return (size_t) qHash(s);
    }
  };
}

QList<MarkerType> S4CMarkersDB1DataExtractStepsBuilder::allMarkerFileTypes =
{
    // L2A markers
    {"L2AB01", ProductType::L2AProductTypeId, "B01", "B01"},
    {"L2AB02", ProductType::L2AProductTypeId, "B02", "B02"},
    {"L2AB03", ProductType::L2AProductTypeId, "B03", "B03"},
    {"L2AB04", ProductType::L2AProductTypeId, "B04", "B04"},
    {"L2AB05", ProductType::L2AProductTypeId, "B05", "B05"},
    {"L2AB06", ProductType::L2AProductTypeId, "B06", "B06"},
    {"L2AB07", ProductType::L2AProductTypeId, "B07", "B07"},
    {"L2AB08", ProductType::L2AProductTypeId, "B08", "B08"},
    {"L2AB8A", ProductType::L2AProductTypeId, "B8A", "B8A"},
    {"L2AB09", ProductType::L2AProductTypeId, "B09", "B09"},
    {"L2AB10", ProductType::L2AProductTypeId, "B10", "B10"},
    {"L2AB11", ProductType::L2AProductTypeId, "B11", "B11"},
    {"L2AB12", ProductType::L2AProductTypeId, "B12", "B12"},

    // L3B markers
    {"NDVI", ProductType::L3BProductTypeId, "SNDVI", ""},
    {"LAI", ProductType::L3BProductTypeId, "SLAIMONO", ""},
    {"FAPAR", ProductType::L3BProductTypeId, "SFAPARMONO", ""},
    {"FCOVER", ProductType::L3BProductTypeId, "SFCOVERMONO", ""},
    {"NDWI", ProductType::L3BProductTypeId, "SNDWI", ""},
    {"BRIGHTNESS", ProductType::L3BProductTypeId, "SBRIGHT", ""},

    // S1 markers
    {"AMP", ProductType::S4CS1L2AmpProductTypeId, "AMP", ""},
    {"COHE", ProductType::S4CS1L2CoheProductTypeId, "COHE", ""}
};

QList<MetricType> S4CMarkersDB1DataExtractStepsBuilder::supportedMetrics = {
  {"valid_pixels_enabled", "validity"},
//  {"stdev_enabled", "stdev"},
  {"minmax_enabled", "minmax"},
  {"median_enabled", "median"},
  {"p25_enabled", "p25"},
  {"p75_enabled", "p75"}
};

S4CMarkersDB1DataExtractStepsBuilder::S4CMarkersDB1DataExtractStepsBuilder() :
    m_idFieldName("NewID"), m_optParcelsPattern(".*_buf_5m.shp"), m_sarParcelsPattern(".*_buf_10m.shp")
{
}

QList<MarkerType> S4CMarkersDB1DataExtractStepsBuilder::GetEnabledMarkers() const {
    return enabledMarkers;
}

QDateTime S4CMarkersDB1DataExtractStepsBuilder::GetDataExtractionMinDate() const {
    return prdMinDate;
}

QDateTime S4CMarkersDB1DataExtractStepsBuilder::GetDataExtractionMaxDate() const {
    return prdMaxDate;
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

bool S4CMarkersDB1DataExtractStepsBuilder::HasAnyMarkerEnabled(const ProductType &prdType, const std::map<QString, QString> &cfgParams)
{
    QJsonObject params;
    bool markerEnabled;
    for (const auto &marker: allMarkerFileTypes) {
        if (marker.prdType != prdType)
            continue;

        markerEnabled = ProcessorHandlerHelper::GetBoolConfigValue(params, cfgParams,
                                                                       marker.marker.toLower() + "_enabled", MDB1_CFG_PREFIX);
        if (markerEnabled) {
            return true;
        }
    }
    // no marker enabled for this product type
    return false;
}

void S4CMarkersDB1DataExtractStepsBuilder::CreateTasks(const MarkerType &marker, QList<TaskToSubmit> &outAllTasksList, int &curTaskIdx) const
{
    // create for all products of this marker the data extraction tasks, if needed
    QString dataExtrTaskName(isScheduledJob ? "mdb1-data-extraction-scheduled" :
                                              "mdb1-data-extraction");
    int initialTaskIdx = curTaskIdx;
    for(int i  = 0; i<fileInfos.size(); i++) {
        if (fileInfos[i].markerInfo.marker == marker.marker) {
            outAllTasksList.append(TaskToSubmit{ dataExtrTaskName, {} });
            curTaskIdx++;
        }
    }
    Logger::info(QStringLiteral("MDB1 Create tasks: added %1 tasks for marker %2").
                 arg(curTaskIdx - initialTaskIdx).arg(marker.marker));
}

void S4CMarkersDB1DataExtractStepsBuilder::CreateSteps(const MarkerType &marker, QList<TaskToSubmit> &allTasksList,
                                                       NewStepList &steps, int &curTaskIdx, QStringList &dataExtrDirs) const
{
    for(int i  = 0; i<fileInfos.size(); i++) {
        const PrdMarkerInfo &prdMarkerInfo = fileInfos[i];
        if (prdMarkerInfo.markerInfo.marker == marker.marker) {
            TaskToSubmit &dataExtractionTask = allTasksList[curTaskIdx++];
            const QString &dataExtrTaskPath = dataExtractionTask.GetFilePath("");
            const QString &dataExtrDirName = isScheduledJob ?
                        GetDataExtractionDir(prdMarkerInfo.prdFileInfo.prdTime.date().year(), marker.marker) :
                        dataExtrTaskPath;
            if (!dataExtrDirs.contains(dataExtrDirName)) {
                QDir().mkpath(dataExtrDirName);
                dataExtrDirs.append(dataExtrDirName);
            }
            const QStringList &dataExtractionArgs = GetDataExtractionArgs(m_idFieldName, prdMarkerInfo, dataExtrDirName);
            steps.append(StepExecutionDecorator::GetInstance()->CreateTaskStep(parentProcessorName, dataExtractionTask,
                                                                               "Markers1Extractor", dataExtractionArgs));
        }
    }
}

void S4CMarkersDB1DataExtractStepsBuilder::Initialize(const QString &parentProc, EventProcessingContext &ctx, const QJsonObject &evtParams,
                                                      int siteId, int jobId, const QStringList &markersEnabled)
{
    parentProcessorName = parentProc;
    pCtx = &ctx;
    parameters = evtParams;
    configParameters = pCtx->GetJobConfigurationParameters(jobId, MDB1_CFG_PREFIX);
    this->siteId = siteId;
    this->jobId = jobId;
    siteShortName = pCtx->GetSiteShortName(siteId);
    isScheduledJob = IsScheduledJobRequest(parameters);

    // Update parcels column name and file names patterns for the parcels product
    UpdateParcelsPrdDescriptionsFromDB();

    InitEnabledMarkersDescriptions(markersEnabled);

    // Get the list of most recent LPIS products per year, according to min and max dates extracted from files
    lpisInfos = ExtractLpisInfos();

    // Extract the raster files to be processed by filtering (if is the case) is LPIS is available and in case
    // of scheduled job,  if they were not processed or processing
    ExtractProductFiles();
}

void S4CMarkersDB1DataExtractStepsBuilder::InitEnabledMarkersDescriptions(const QStringList &markersEnabled)
{
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
            if (!enabledMarkersProductTypes.contains(marker.prdType)) {
                enabledMarkersProductTypes.push_back(marker.prdType);
            }
        }
    }
}

void S4CMarkersDB1DataExtractStepsBuilder::ExtractProductFiles()
{
    QDateTime prdMinDate;
    QDateTime prdMaxDate;

    if (enabledMarkers.size() > 0) {
        for (ProductType prdType: enabledMarkersProductTypes) {
            std::unique_ptr<ProductHelper> prdHelper = ProductHelperFactory::GetProductHelper(prdType);
            const ProductList &prds = ProcessorHandler::GetInputProducts(*(pCtx), parameters, siteId, prdType,
                                                                    &prdMinDate, &prdMaxDate);
            if (prds.size() == 0) {
                continue;
            }
            for (const auto &marker: enabledMarkers) {
                QList<PrdFileInfo> missingPrdFiles;
                QList<PrdFileInfo> processedPrdFiles;
                for (const Product &prd: prds) {
                    prdHelper->SetProduct(prd.fullPath);
                    const QDateTime &prdDate = prd.created;
                    if (!prdDate.isValid()) {
                        continue;
                    }
                    // check if there exists an LPIS product for this input product
                    int prdYear = prdDate.date().year();
                    if (lpisInfos.find(prdYear) == lpisInfos.end()) {
                        Logger::info(QStringLiteral("MDB1: Product %1 not processed as there is no LPIS for product year %2").
                                    arg(prd.fullPath).arg(prdYear));
                        continue;
                    }

                    if (marker.prdType == prdType) {
                        // get all files for the marker type
                        const QStringList &rasterFiles = prdHelper->GetProductFiles(marker.markerSubstrInFileName);
                        for (const auto &rasterFile : rasterFiles) {
                            if (isScheduledJob) {
                                // first check if the product wasn't already processed
                                const QString &dataExtrDirName = GetDataExtractionDir(prdDate.date().year(), marker.marker);
                                IsDataExtractionPerformed(dataExtrDirName, rasterFile) ? processedPrdFiles.push_back({rasterFile, prdDate}) :
                                                                                         missingPrdFiles.push_back({rasterFile, prdDate});
                            } else {
                                // custom job, just add the file to the file infos list
                                fileInfos.push_back({marker, rasterFile, prdDate});
                            }
                        }
                    }
                }
                // if scheduled job, we filter the rasters that are currently processing by other jobs
                if (missingPrdFiles.size() != 0 || processedPrdFiles.size() != 0) {
                    const QList<PrdFileInfo> &filtered = FilterAndUpdateAlreadyProcessingPrds(missingPrdFiles, processedPrdFiles, marker);
                    for(const PrdFileInfo &fileInfo : filtered) {
                        fileInfos.append({marker, fileInfo});
                    }
                }
            }
        }
    } else {
        Logger::error(QStringLiteral("No enabled markers for job %1!!!").arg(jobId));
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
    const ProductList &lpisPrds = S4CUtils::GetLpisProduct(pCtx, siteId);
    if (lpisPrds.size() == 0) {
        pCtx->MarkJobFailed(jobId);
        throw std::runtime_error(QStringLiteral("No LPIS product found in database for the MDB1 execution for site %1.").
                                 arg(siteShortName).toStdString());
    }

    QMap<int, LpisInfos> lpisInfos;

    QRegularExpression reOpt(m_optParcelsPattern);
    QRegularExpression reSar(m_sarParcelsPattern);
    for(const Product &lpisPrd: lpisPrds) {
        // ignore LPIS products from a year where we already added an LPIS product newer
        QMap<int, LpisInfos>::const_iterator i = lpisInfos.find(lpisPrd.created.date().year());
        if (i != lpisInfos.end()) {
            if (lpisPrd.inserted < i.value().insertedDate) {
                Logger::info(QStringLiteral("MDB1: LPIS product %1 ignored as there is another one more recent than it for the same year %2").
                             arg(lpisPrd.fullPath).arg(i.value().productPath));
                continue;
            }
        }

        // If the year is >= 2019, then use LAEA for AMP and COHE and no matter which other for NDVI
        //const QString &prdLpisPath = lpisPrds[lpisPrds.size()-1].fullPath;
        QDir directory(lpisPrd.fullPath);
        Logger::info(QStringLiteral("MDB1: Extracting files for LPIS product %1").
                     arg(lpisPrd.fullPath));


        const QStringList &dirFiles = directory.entryList(QStringList() << "*.shp" << "*.csv" << "*.gpkg" ,QDir::Files);
        LpisInfos lpisInfo;
        foreach(const QString &fileName, dirFiles) {
            // we don't want for optical products the LAEA projection
            if (reOpt.match(fileName).hasMatch() && (lpisInfo.opticalIdsGeomShapePath.size() == 0)) {
                lpisInfo.opticalIdsGeomShapePath = directory.filePath(fileName);
            }
            // LAEA projection have priority for 10m buffer
            if (reSar.match(fileName).hasMatch() && lpisInfo.sarGeomShapePath.size() == 0) {
                lpisInfo.sarGeomShapePath = directory.filePath(fileName);
            }
        }
        if (lpisInfo.opticalIdsGeomShapePath.size() != 0 &&
                lpisInfo.sarGeomShapePath.size() != 0) {
            lpisInfo.productName = lpisPrd.name;
            lpisInfo.productPath = lpisPrd.fullPath;
            lpisInfo.productDate = lpisPrd.created;
            lpisInfo.insertedDate = lpisPrd.inserted;
            lpisInfos[lpisInfo.productDate.date().year()] = lpisInfo;
            Logger::info(QStringLiteral("MDB1: Using LPIS %1 for year %2").arg(lpisPrd.fullPath).arg(lpisInfo.productDate.date().year()));
        } else {
            Logger::info(QStringLiteral("MDB1: LPIS infos couldn't be extracted from path %1").arg(lpisPrd.fullPath));
        }
    }

    return lpisInfos;
}

QStringList S4CMarkersDB1DataExtractStepsBuilder::GetDataExtractionArgs(const QString &uidField,
                                                         const PrdMarkerInfo &inputFileInfo, const QString &outDir) const
{
    QStringList retArgs = { "Markers1Extractor", "-field", uidField,
                            "-prdtype", ProductHelper::GetProductTypeShortName(inputFileInfo.markerInfo.prdType),
                            "-outdir", outDir, "-il", inputFileInfo.prdFileInfo.inFilePath };
    const QString *idsGeomShapePath;
    QMap<int, LpisInfos>::const_iterator i = lpisInfos.find(inputFileInfo.prdFileInfo.prdTime.date().year());

    switch(inputFileInfo.markerInfo.prdType) {
        case ProductType::L3BProductTypeId:
        case ProductType::L2AProductTypeId:
            idsGeomShapePath = &(i.value().opticalIdsGeomShapePath);   // we can do that as we previously removed the products that do not have LPIS
            break;
        default:
            idsGeomShapePath = &(i.value().sarGeomShapePath);
            break;
    }

    if (idsGeomShapePath->size() > 0) {
        retArgs += "-vec";
        retArgs += *idsGeomShapePath;
    }

    if (inputFileInfo.markerInfo.bandDiscriminationInfo.size() > 0) {
        retArgs += "-banddiscr";
        retArgs += inputFileInfo.markerInfo.bandDiscriminationInfo;
    }

    for (const MetricType &mt : supportedMetrics) {
        bool mtEnabled = ProcessorHandlerHelper::GetBoolConfigValue(parameters, configParameters, mt.configName, MDB1_CFG_PREFIX);
        retArgs += ("-" + mt.paramName);
        retArgs += QString::number(mtEnabled ? 1 : 0);
    }

    return retArgs;
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
        std::unordered_map<QString, int> mapPresence;
        std::for_each(yearProcessedPrdsFiles.begin(), yearProcessedPrdsFiles.end(), [&mapPresence](const PrdFileInfo &prd) {
            mapPresence[prd.inFilePath] = 1;
        });

        const QString &dataExtrDirName = GetDataExtractionDir(year, markerType.marker);
        const QString &filePath = QDir::cleanPath(dataExtrDirName + QDir::separator() + markerType.marker +
                                                  "_current_data_extraction_products.txt");
        QDir().mkpath(QFileInfo(filePath).absolutePath());
        QFile file( filePath );
        // First read all the entries in the file to see what are the products that are currently processing

        // Get the active jobs of this site
        const JobIdsList &activeJobIds = pCtx->GetActiveJobIds(-1, siteId);

        QMap<QString, int> curProcPrds;
        if (file.open(QIODevice::ReadOnly))
        {
           QTextStream in(&file);
           while (!in.atEnd())
           {
               const QString &line = in.readLine();
               const QStringList &pieces = line.split(';');
               if (pieces.size() == 2) {
                   const QString &prdPath = pieces[0];
                   int jobId = pieces[1].toInt();
                   // remove already processed L2A products from this file
                   if (mapPresence.find(prdPath) != mapPresence.end()) {
                       continue;
                   }
                   // if the job processing the product is still active, keep the product
                   if (activeJobIds.contains(jobId)) {
                       curProcPrds[prdPath] = jobId;
                   }
               }
           }
           file.close();
        }
        // add the products that will be processed next
        for (int i = 0; i<yearMissingPrdsFiles.size(); i++) {
            const PrdFileInfo &prdFile =  yearMissingPrdsFiles[i];
            if (!curProcPrds.contains(prdFile.inFilePath)) {
                curProcPrds[prdFile.inFilePath] = jobId;
                filteredPrds.append(prdFile);
            }
            // else, if the product was already in this list, then it means it was already scheduled for processing
            // by another schedule operation
        }

        if ( file.open(QIODevice::ReadWrite | QFile::Truncate) )
        {
            if (curProcPrds.size() > 0) {
                QTextStream stream( &file );
                for(auto prdInfo : curProcPrds.keys()) {
                    stream << prdInfo << ";" << curProcPrds.value(prdInfo) << endl;
                }
            }
        }
    }

    return filteredPrds;
}

bool S4CMarkersDB1DataExtractStepsBuilder::IsScheduledJobRequest(const QJsonObject &parameters) {
    int jobVal;
    return ProcessorHandlerHelper::GetParameterValueAsInt(parameters, "scheduled_job", jobVal) && (jobVal == 1);
}

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

void S4CMarkersDB1DataExtractStepsBuilder::UpdateParcelsPrdDescriptionsFromDB() {
    const std::map<QString, QString> &parcelsPrdKeys = pCtx->GetJobConfigurationParameters(jobId, PARCELS_PRD_KEYS_PREFIX);
    const QString &parcelIdColName = ProcessorHandlerHelper::GetStringConfigValue(parameters, parcelsPrdKeys, "parcel_id_col_name", PARCELS_PRD_KEYS_PREFIX);
    if(parcelIdColName.size() > 0) {
        Logger::info(QStringLiteral("MDB1: Configured field name %1 from parcels product").arg(parcelIdColName));
        m_idFieldName = parcelIdColName;
    }
    const QString &parcelsOptFileNamePat = ProcessorHandlerHelper::GetStringConfigValue(parameters, parcelsPrdKeys, "parcels_optical_file_name_pattern", PARCELS_PRD_KEYS_PREFIX);
    if(parcelsOptFileNamePat.size() > 0) {
        Logger::info(QStringLiteral("MDB1: Configured pattern %1 for parcels product csv file").arg(parcelsOptFileNamePat));
        m_optParcelsPattern = parcelsOptFileNamePat;
    }
    const QString &parcelsSarFileNamePat = ProcessorHandlerHelper::GetStringConfigValue(parameters, parcelsPrdKeys, "parcels_sar_file_name_pattern", PARCELS_PRD_KEYS_PREFIX);
    if(parcelsSarFileNamePat.size() > 0) {
        Logger::info(QStringLiteral("MDB1: Configured pattern %1 for parcels product csv file").arg(parcelsSarFileNamePat));
        m_sarParcelsPattern = parcelsSarFileNamePat;
    }
}
