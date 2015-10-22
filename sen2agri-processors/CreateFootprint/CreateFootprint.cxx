#include "otbWrapperApplication.h"
#include "otbWrapperApplicationFactory.h"

#include "../../MACCSMetadata/include/MACCSMetadataReader.hpp"
#include "../../MACCSMetadata/include/SPOT4MetadataReader.hpp"

#include "otbVectorImage.h"
#include "otbImageList.h"
#include "otbImageListToVectorImageFilter.h"
#include "otbMultiToMonoChannelExtractROI.h"
#include "otbBandMathImageFilter.h"
#include "otbVectorDataFileWriter.h"

#include "otbStreamingResampleImageFilter.h"
#include "otbStreamingStatisticsImageFilter.h"
#include "otbLabelImageToVectorDataFilter.h"

// Transform
#include "itkScalableAffineTransform.h"
#include "itkIdentityTransform.h"
#include "itkScaleTransform.h"

#include "otbOGRIOHelper.h"
#include "otbGeoInformationConversion.h"

#include "MACCSMetadataReader.hpp"
#include "SPOT4MetadataReader.hpp"

namespace otb
{

namespace Wrapper
{
class CreateFootprint : public Application
{
public:
    typedef CreateFootprint Self;
    typedef Application Superclass;
    typedef itk::SmartPointer<Self> Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    itkNewMacro(Self)

    itkTypeMacro(CreateFootprint, otb::Application)

private:
    typedef VectorDataType::DataNodeType DataNodeType;
    typedef VectorDataType::DataTreeType DataTreeType;
    typedef DataNodeType::PolygonType PolygonType;
    typedef PolygonType::ContinuousIndexType ContinuousIndexType;

    void DoInit()
    {
        SetName("CreateFootprint");
        SetDescription("Creates vector data from an image footprint");

        SetDocName("CreateFootprint");
        SetDocLongDescription(
            "Creates vector data with a polygon determined by the footprint of an image");
        SetDocLimitations("None");
        SetDocAuthors("LNI");
        SetDocSeeAlso(" ");

        AddDocTag(Tags::Vector);

        AddParameter(ParameterType_InputFilename,
                     "in",
                     "The input file, an image, or a SPOT4 or MACCS descriptor file");
        AddParameter(ParameterType_OutputVectorData, "out", "The output footprint");

        SetDocExampleParameterValue("in", "image.tif");
        SetDocExampleParameterValue("out", "footprint.shp");
    }

    void DoUpdateParameters()
    {
    }

    void DoExecute()
    {
        auto vectorData = CreateVectorData(GetParameterString("in"));

        SetParameterOutputVectorData("out", vectorData.GetPointer());
    }

    VectorDataType::Pointer CreateVectorData(const std::string &file)
    {
        auto outVectorData = VectorDataType::New();

        auto document = DataNodeType::New();
        document->SetNodeType(otb::DOCUMENT);
        document->SetNodeId("footprint");
        auto folder = DataNodeType::New();
        folder->SetNodeType(otb::FOLDER);
        auto polygonNode = DataNodeType::New();
        polygonNode->SetNodeType(otb::FEATURE_POLYGON);

        auto tree = outVectorData->GetDataTree();
        auto root = tree->GetRoot()->Get();

        tree->Add(document, root);
        tree->Add(folder, document);
        tree->Add(polygonNode, folder);

        PolygonType::Pointer polygon;
        if (auto metadata = itk::MACCSMetadataReader::New()->ReadMetadata(file)) {
            polygon = FootprintFromMACCSMetadata(*metadata);

            outVectorData->SetProjectionRef(otb::GeoInformationConversion::ToWKT(4326));
        } else if (auto metadata = itk::SPOT4MetadataReader::New()->ReadMetadata(file)) {
            polygon = FootprintFromSPOT4Metadata(*metadata);

            outVectorData->SetProjectionRef(otb::GeoInformationConversion::ToWKT(4326));
        } else {
            auto reader = otb::ImageFileReader<FloatVectorImageType>::New();
            reader->SetFileName(file);
            reader->UpdateOutputInformation();

            auto output = reader->GetOutput();

            polygon = FootprintFromGeoCoding(output);

            outVectorData->SetProjectionRef(output->GetProjectionRef());
        }

        polygonNode->SetPolygonExteriorRing(polygon);

        return outVectorData;
    }

    PolygonType::Pointer FootprintFromMACCSMetadata(const MACCSFileMetadata &metadata)
    {
        auto poly = PolygonType::New();
        poly->AddVertex(
            PointFromMACCSGeopoint(metadata.ProductInformation.GeoCoverage.UpperLeftCorner));
        poly->AddVertex(
            PointFromMACCSGeopoint(metadata.ProductInformation.GeoCoverage.LowerLeftCorner));
        poly->AddVertex(
            PointFromMACCSGeopoint(metadata.ProductInformation.GeoCoverage.LowerRightCorner));
        poly->AddVertex(
            PointFromMACCSGeopoint(metadata.ProductInformation.GeoCoverage.UpperRightCorner));
        return poly;
    }

    PolygonType::Pointer FootprintFromSPOT4Metadata(const SPOT4Metadata &metadata)
    {
        auto poly = PolygonType::New();
        poly->AddVertex(PointFromSPOT4Point(metadata.WGS84.HGX, metadata.WGS84.HGY));
        poly->AddVertex(PointFromSPOT4Point(metadata.WGS84.BGX, metadata.WGS84.BGY));
        poly->AddVertex(PointFromSPOT4Point(metadata.WGS84.BDX, metadata.WGS84.BDY));
        poly->AddVertex(PointFromSPOT4Point(metadata.WGS84.HDX, metadata.WGS84.HDY));
        return poly;
    }

    PolygonType::Pointer FootprintFromGeoCoding(const FloatVectorImageType *image)
    {
        auto poly = PolygonType::New();
        poly->AddVertex(PointFromVector(image->GetUpperLeftCorner()));
        poly->AddVertex(PointFromVector(image->GetLowerLeftCorner()));
        poly->AddVertex(PointFromVector(image->GetLowerRightCorner()));
        poly->AddVertex(PointFromVector(image->GetUpperRightCorner()));
        return poly;
    }

    ContinuousIndexType PointFromMACCSGeopoint(const MACCSGeoPoint &point)
    {
        ContinuousIndexType index;
        index[0] = point.Long;
        index[1] = point.Lat;
        return index;
    }

    ContinuousIndexType PointFromSPOT4Point(double x, double y)
    {
        ContinuousIndexType index;
        index[0] = x;
        index[1] = y;
        return index;
    }

    ContinuousIndexType PointFromVector(const FloatVectorImageType::VectorType &v)
    {
        ContinuousIndexType index;
        index[0] = v[0];
        index[1] = v[1];
        return index;
    }
};
}
}

OTB_APPLICATION_EXPORT(otb::Wrapper::CreateFootprint)