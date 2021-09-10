#include "s4c_utils.hpp"
#include "logger.hpp"
#include "processorhandler.hpp"

ProductList S4CUtils::GetLpisProduct(ExecutionContextBase *pCtx, int siteId) {
    // We take it the last LPIS product for this site.
    QDate  startDate, endDate;
    startDate.setDate(1970, 1, 1);
    QDateTime startDateTime(startDate);
    endDate.setDate(2050, 12, 31);
    QDateTime endDateTime(endDate);
    return pCtx->GetProducts(siteId, (int)ProductType::S4CLPISProductTypeId, startDateTime, endDateTime);
}

QString S4CUtils::GetSiteYearFromDisk(const QJsonObject &parameters, const std::map<QString, QString> &configParameters,
                                      const QString &siteShortName, const QString &cfgFilesSubPath,
                                      const QString &cfgKeyPrefix, const QString &cfgKey)
{
    qDebug() << "Determining year from disk ...";
    QString dataExtrDirName = ProcessorHandlerHelper::GetStringConfigValue(parameters, configParameters, cfgKey, cfgKeyPrefix);
    dataExtrDirName = dataExtrDirName.replace("{site}", siteShortName);
    const QStringList &tokens = dataExtrDirName.split("{year}");
    QString retVal;
    QDateTime lastDirDate;
    if (tokens.length() > 0) {
        const QString &yearsRoot = tokens.at(0);
        QDirIterator iter(yearsRoot, QDir::Dirs | QDir::NoDotAndDotDot);
        while(iter.hasNext()) {
            const QString &subdirFullPath = iter.next();
            QFileInfo fileInfo(subdirFullPath);
            const QString &subDir = fileInfo.fileName();
            const QDateTime &dirDate = fileInfo.lastModified();
            qDebug() << "Checking subfolder " << subDir << " from " << yearsRoot << " to determine the year ...";
            int year = subDir.toInt();
            // check if valid year
            if (year >= 1970 && year < 2100) {
                // check if it the most recent one
                if (!lastDirDate.isValid() || dirDate > lastDirDate) {
                    // check if the config subfolder has files
                    const QFileInfoList &entries = QDir(subdirFullPath + QDir::separator() + cfgFilesSubPath).
                                                    entryInfoList(QDir::NoDotAndDotDot|QDir::Files);
                    if (entries.count() > 0) {
                        retVal = subDir;
                        lastDirDate = dirDate;
                    }
                }
            }
        }
    }
    return retVal;
}

