#include "producthelper.h"

using namespace orchestrator::products;

ProductHelper::ProductHelper() : m_bValid(false)
{
}

ProductHelper::ProductHelper(const QString &product)
{
    SetProduct(product);
}

ProductHelper::ProductHelper(const ProductDetails &product) :
    m_prdDetails(product), m_bValid(false)
{
}

QStringList ProductHelper::GetProductFiles(const QString &)
{
    return QStringList();
}

QStringList ProductHelper::GetProductMasks(const QString &)
{
    return QStringList();
}
QMap<QString, QString> ProductHelper::GetProductFilesByTile(const QString &, bool)
{
    return QMap<QString, QString>();
}

void ProductHelper::SetProduct(const ProductDetails &product)
{
    m_prdDetails = product;
    m_bValid = true;
}

void ProductHelper::SetProduct(const QString &product)
{
    Product prd;
    prd.fullPath = product;
    m_prdDetails.GetProductRef() = prd;
    m_bValid = false;
}

// TODO: Move this to another location (See also TilesTimeSeries)
Satellite ProductHelper::GetPrimarySatelliteId(const QList<Satellite> &satIds) {
    // Get the primary satellite id
    Satellite retSatId = Satellite::Sentinel2;
    if(satIds.contains(Satellite::Sentinel2)) {
        retSatId = Satellite::Sentinel2;
    } else if(satIds.size() >= 1) {
        // check if all satellites in the list are the same
        const Satellite &refSatId = satIds[0];
        bool bAllSameSat = true;
        for(const Satellite &satId: satIds) {
            if(satId != refSatId) {
                bAllSameSat = false;
                break;
            }
        }
        if(bAllSameSat) {
            retSatId = satIds[0];
        }
    }

    return retSatId;
}

QString ProductHelper::GetProductTypeShortName(ProductType prdType)
{
    switch (prdType) {
    case ProductType::L2AProductTypeId:
        return "L2A";
    case ProductType::L3AProductTypeId:
        return "L3A";
    case ProductType::L3BProductTypeId:
        return "L3B";
    case ProductType::L3EProductTypeId:
        return "L3E";
    case ProductType::L4AProductTypeId:
        return "L4A";
    case ProductType::L4BProductTypeId:
        return "L4B";
    case ProductType::L1CProductTypeId:
        return "L1C";
    case ProductType::L3CProductTypeId:
        return "L3C";
    case ProductType::L3DProductTypeId:
        return "L3D";
    case ProductType::S4CS1L2AmpProductTypeId:
        return "AMP";
    case ProductType::S4CS1L2CoheProductTypeId:
        return "COHE";
    case ProductType::S4CL4AProductTypeId:
        return "S4C_L4A";
    case ProductType::S4CL4BProductTypeId:
        return "S4C_L4B";
    case ProductType::S4CLPISProductTypeId:
        return "LPIS";
    case ProductType::S4CL4CProductTypeId:
        return "S4C_L4C";
    case ProductType::S4CL3CProductTypeId:
        return "S4C_L3C";
    case ProductType::S4MDB1ProductTypeId:
        return "S4C_MDB1";
    case ProductType::S4MDB2ProductTypeId:
        return "S4C_MDB2";
    case ProductType::S4MDB3ProductTypeId:
        return "S4C_MDB3";
    case ProductType::S4MDBL4AOptMainProductTypeId:
        return "S4C_MDB_L4A_Opt_Main";
    case ProductType::S4MDBL4AOptReProductTypeId:
        return "S4C_MDB_L4A_Opt_Re";
    case ProductType::S4MDBL4ASarMainProductTypeId:
        return "S4C_MDB_L4A_Sar_Main";
    case ProductType::S4MDBL4ASarTempProductTypeId:
        return "S4C_MDB_L4A_Sar_Temp";
    // TODO                      = 24,
    case ProductType::FMaskProductTypeId:
        return "FMASK";
    case ProductType::MaskedL2AProductTypeId:
        return "L2A_MASKED";
    default:
        break;
    }
    return "";
}
