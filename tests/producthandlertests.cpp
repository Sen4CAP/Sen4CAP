#include <QTest>
#include <iostream>

#include "producthandlertests.h"

#include "processor/products/l2aproducthelper.h"
#include "processor/products/generichighlevelproducthelper.h"
#include "processor/products/s1l2producthelper.h"
#include "processor/products/producthelperfactory.h"

using namespace orchestrator::products;

ProductHandlerTests::ProductHandlerTests(QObject *parent) : QObject(parent)
{

}

void ProductHandlerTests::initTestCase()
{
}

void ProductHandlerTests::cleanupTestCase()
{
}

void testPrd(ProductHelper *pHelper,  const QString &prd) {
    std::cout << "============ START CHECK FOR: " << prd.toStdString() << "  ==============" << std::endl;
    bool err = false;
    try {
        pHelper->SetProduct(prd);
    } catch (const std::exception &e) {
        QString errTxt(e.what());
        QVERIFY2(errTxt.startsWith("Only full paths are accepted for handler. Error for product "), e.what());
        std::cout << "Product: " << prd.toStdString() << " is not a full path. Ignoring..." << std::endl;
        err = true;
    }
    if (!err) {
        QVERIFY2(pHelper->IsValid(), prd.toStdString().c_str());
        const QString &acqDate = pHelper->GetAcqDate().toString("yyyyMMddThhmmss");
        QVERIFY2(acqDate.size() > 0, prd.toStdString().c_str());
        std::cout << "Acq date: " << acqDate.toStdString() << "." << std::endl;

        if (pHelper->HasTiles()) {
            const QStringList &tileIds = pHelper->GetTileIdsFromProduct();
            if (tileIds.size() > 0) {
                for (const QString &tileId: tileIds) {
                    std::cout << "Tile ID: " << tileId.toStdString() << std::endl;
                }
            } else {
                // product does not have
                std::cout << "Product: " << prd.toStdString() << " does not have tile ids in the name. They should be extracted from inner files." << std::endl;
            }
        }
    }
    std::cout << "============ END CHECK for: " << prd.toStdString() << "  ==============" << std::endl << std::endl << std::endl;
}

void testGetL2AProductFiles(ProductHelper *pHelper,  const QString &prd, const QString &bandFilter = "") {
    std::cout << "============ START CHECK FOR: " << prd.toStdString() << "  ==============" << std::endl;
    bool err = false;
    try {
        pHelper->SetProduct(prd);
    } catch (const std::exception &e) {
        QString errTxt(e.what());
        QVERIFY2(errTxt.startsWith("Only full paths are accepted for handler. Error for product "), e.what());
        std::cout << "Product: " << prd.toStdString() << " is not a full path. Ignoring..." << std::endl;
        err = true;
    }
    if (!err) {
        QVERIFY2(pHelper->IsValid(), prd.toStdString().c_str());
        const QString &acqDate = pHelper->GetAcqDate().toString("yyyyMMddThhmmss");
        QVERIFY2(acqDate.size() > 0, prd.toStdString().c_str());
        std::cout << "Acq date: " << acqDate.toStdString() << "." << std::endl;

        const QStringList &rasterFiles = pHelper->GetProductFiles(bandFilter);
        if (rasterFiles.size() > 0) {
            for (const QString &rasterFile: rasterFiles) {
                std::cout << "Raster: " << rasterFile.toStdString() << std::endl;
                QVERIFY2(QFile(rasterFile).exists(), rasterFile.toStdString().c_str());
            }
        } else {
            // product does not have
            std::cout << "Product: " << prd.toStdString() << " does not have any raster for the applied band filter . They should be extracted from inner files." << std::endl;
        }
    }
    std::cout << "============ END CHECK for: " << prd.toStdString() << " and band filter" << bandFilter.toStdString() << "  ==============" << std::endl << std::endl << std::endl;

}

void ProductHandlerTests::testL2AProductHandler()
{
    L2AProductHelper helper;

    // Old MACCS formats
//    testPrd(&helper, "LC81860522016182LGN00_L2A");
//    testPrd(&helper, "S2A_OPER_PRD_MSIL2A_PDMC_20160607T044353_R136_V20160606T094258_20160606T094258.SAFE");

//    testPrd(&helper, "/mnt/archive/test/Borno_South/LC81860522016182LGN00_L2A");
//    testPrd(&helper, "/mnt/archive/test/Borno_South/S2A_OPER_PRD_MSIL2A_PDMC_20160607T044353_R136_V20160606T094258_20160606T094258.SAFE");

//    testPrd(&helper, "/mnt/archive/test/Borno_South/LC81860522016182LGN00_L2A/L8_TEST_L8C_L2VALD_186052_20160630.HDR");
//    testPrd(&helper, "/mnt/archive/test/Borno_South/S2A_OPER_PRD_MSIL2A_PDMC_20160607T044353_R136_V20160606T094258_20160606T094258.SAFE/S2A_OPER_SSC_L2VALD_32PRT____20160606.HDR");


//    // New MAJA formats
//    testPrd(&helper, "LC08_L2A_191026_20200324_20200326_01_T1");
//    testPrd(&helper, "S2A_MSIL2A_20200329T101021_N0209_R022_T33UVQ_20200329T111343.SAFE");

//    testPrd(&helper, "/mnt/archive/SitesOutputProducts/maccs_def/test_21/l2a/LC08_L2A_191026_20200324_20200326_01_T1");
//    testPrd(&helper, "/mnt/archive/SitesOutputProducts/maccs_def/test_21/l2a/S2A_MSIL2A_20200329T101021_N0209_R022_T33UVQ_20200329T111343.SAFE");

//    testPrd(&helper, "/mnt/archive/SitesOutputProducts/maccs_def/test_21/l2a/LC08_L2A_191026_20200324_20200326_01_T1/L8_TEST_L8C_L2VALD_191026_20200324.HDR");
//    testPrd(&helper, "/mnt/archive/SitesOutputProducts/maccs_def/test_21/l2a/S2A_MSIL2A_20200329T101021_N0209_R022_T33UVQ_20200329T111343.SAFE/"
//                     "SENTINEL2A_20200329-101701-849_L2A_T33UVQ_C_V1-0/SENTINEL2A_20200329-101701-849_L2A_T33UVQ_C_V1-0_MTD_ALL.xml");

//    testPrd(&helper, "/mnt/archive/maccs_def/test_proc_sen2cor/l2a/2020/04/18/S2A_MSIL2A_20200418T101031_N0209_R022_T33UVQ_20200418T122607.SAFE/"
//                     "S2A_MSIL2A_20200418T101031_N9999_R022_T33UVQ_20201023T205720.SAFE/MTD_MSIL2A.xml");

//    testPrd(&helper, "/mnt/archive/test_dataset_20/l2a/S2A_MSIL2A_20200418T101031_N0214_R022_T33UVQ_20200418T133933.SAFE/MTD_MSIL2A.xml");

//    testGetL2AProductFiles(&helper, "/mnt/archive/maccs_def/belgium_test_alex_maccs/l2a/S2A_OPER_PRD_MSIL2A_PDMC_20161001T021708_R051_V20160928T105022_20160928T105637.SAFE/S2A_OPER_SSC_L2VALD_31UFS____20160928.HDR");
//    testGetL2AProductFiles(&helper, "/mnt/archive/maccs_def/belgium_test_alex_maccs/l2a/S2A_OPER_PRD_MSIL2A_PDMC_20161001T021708_R051_V20160928T105022_20160928T105637.SAFE");

//    testGetL2AProductFiles(&helper, "/mnt/archive/maccs_def/belgium_test_alex_maccs/l2a/LC08_L2A_199025_20160609_20170324_01_T1/L8_TEST_L8C_L2VALD_199025_20160609.HDR");
//    testGetL2AProductFiles(&helper, "/mnt/archive/maccs_def/belgium_test_alex_maccs/l2a/LC08_L2A_199025_20160609_20170324_01_T1");

//    testGetL2AProductFiles(&helper, "/mnt/archive/test/vm_test_workspace/191204_Sen2Agri201/maccs_def/test_201/l2a/S2A_MSIL2A_20181224T104441_N0207_R008_T31TFJ_20181224T111303.SAFE/SENTINEL2A_20181224-104842-609_L2A_T31TFJ_C_V1-0/SENTINEL2A_20181224-104842-609_L2A_T31TFJ_C_V1-0_MTD_ALL.xml");
//    testGetL2AProductFiles(&helper, "/mnt/archive/test/vm_test_workspace/191204_Sen2Agri201/maccs_def/test_201/l2a/S2A_MSIL2A_20181224T104441_N0207_R008_T31TFJ_20181224T111303.SAFE");

//    testGetL2AProductFiles(&helper, "/mnt/archive/maccs_def/test19/l2a/2019/04/01/S2A_MSIL2A_20190401T100031_N0207_R122_T33UVQ_20190401T105727.SAFE/S2A_MSIL2A_20190401T100031_N9999_R122_T33UVQ_20210416T071608.SAFE/MTD_MSIL2A.xml");
//    testGetL2AProductFiles(&helper, "/mnt/archive/maccs_def/test19/l2a/2019/04/01/S2A_MSIL2A_20190401T100031_N0207_R122_T33UVQ_20190401T105727.SAFE/S2A_MSIL2A_20190401T100031_N9999_R122_T33UVQ_20210416T071608.SAFE");

//    testGetL2AProductFiles(&helper, "/mnt/archive/maccs_def/test_maja_4_2_1/l2a/2020/05/10/LC08_L2A_184028_20200510_20200526_01_T1/LANDSAT8-OLITIRS-XS_20200510-090819-156_L2A_184-028_C_V1-0/LANDSAT8-OLITIRS-XS_20200510-090819-156_L2A_184-028_C_V1-0_MTD_ALL.xml");
//    testGetL2AProductFiles(&helper, "/mnt/archive/maccs_def/test_maja_4_2_1/l2a/2020/05/10/LC08_L2A_184028_20200510_20200526_01_T1");

    for (const QString &band : QStringList({"B02"})) {
        testGetL2AProductFiles(&helper, "/mnt/archive/maccs_def/test19/l2a/2019/04/01/S2A_MSIL2A_20190401T100031_N0207_R122_T33UVQ_20190401T105727.SAFE/S2A_MSIL2A_20190401T100031_N9999_R122_T33UVQ_20210416T071608.SAFE/MTD_MSIL2A.xml", band);
        testGetL2AProductFiles(&helper, "/mnt/archive/maccs_def/test19/l2a/2019/04/01/S2A_MSIL2A_20190401T100031_N0207_R122_T33UVQ_20190401T105727.SAFE/S2A_MSIL2A_20190401T100031_N9999_R122_T33UVQ_20210416T071608.SAFE", band);
    }

}

void ProductHandlerTests::testS1L2ProductHandler()
{

}

void ProductHandlerTests::testGenericHighLevelProductHandler()
{

}

