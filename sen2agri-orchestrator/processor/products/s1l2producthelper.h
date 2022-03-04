#ifndef S1L2PRODUCTHELPER_H
#define S1L2PRODUCTHELPER_H

#include "producthelper.h"

namespace orchestrator
{
namespace products
{

typedef struct FileNameInfos {
    QString prdType;
    QDateTime startDate;
    QDateTime endDate;
    QString orbitId;
    QString tileId;
} FileNameInfosType;

class FileNameInfosExtractor
{
public:
    FileNameInfosExtractor() {}
    FileNameInfosExtractor(const QString &regex, int regexVersionIdx, int regexDateIdx, int regexDate2Idx,
                           int regexTypeIdx, int regexOrbitIdx, int regexTileIdx = -1) :
        regex(regex), regexVersionIdx(regexVersionIdx), regexDateIdx(regexDateIdx), regexDate2Idx(regexDate2Idx),
        regexTypeIdx(regexTypeIdx), regexOrbitIdx(regexOrbitIdx), regexTileIdx(regexTileIdx)
    {
    }
    bool IsIntended(const QString &productPath) const;
    virtual FileNameInfosType ExtractInfos(const QString &productPath);

private:
    QString regex;
    int regexVersionIdx;
    int regexDateIdx;
    int regexDate2Idx;
    int regexTypeIdx;
    int regexOrbitIdx;
    int regexTileIdx;
};
class S1L2ProductHelper : public ProductHelper
{
public:
    S1L2ProductHelper();
    S1L2ProductHelper(const ProductDetails &product);
    S1L2ProductHelper(const QString &product);

    virtual void SetProduct(const QString &product);
    virtual bool HasSatellite() { return true;}
    virtual bool IsRaster()  { return true; }
    virtual QStringList GetProductFiles(const QString &fileNameSubstrFilter="");
    virtual QMap<QString, QString> GetProductFilesByTile(const QString &pattern, bool isQi);

    static bool IsIntendedFor(const QString &product);
    static bool IsIntendedFor(const ProductType &prdType);

private:
    bool GetFileNameInfosExtractor(const QString &productPath, FileNameInfosExtractor &extractor);
    const static QList<FileNameInfosExtractor> fnInfoExtractors;
//    void test();
};

} // end of namespace products
} // // end of namespace orchestrator

#endif // S1L2PRODUCTHELPER_H
