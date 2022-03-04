/*
 * Copyright (C) 2005-2020 Centre National d'Etudes Spatiales (CNES)
 *
 * This file is part of Orfeo Toolbox
 *
 *     https://www.orfeo-toolbox.org/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "otbWrapperApplication.h"
#include "otbWrapperApplicationFactory.h"
#include "otbWrapperNumericalParameter.h"

#include "otbMultiToMonoChannelExtractROI.h"
#include "otbImageToVectorImageCastFilter.h"
#include "otbWrapperTypes.h"
#include "otbObjectList.h"
#include "otbVectorImage.h"
#include "otbImageList.h"
#include "otbImageListToImageFilter.h"
#include "include/otbImageListCompositeFilter.h"

#include "include/otbMeanCompositeFunctor.h"
#include "include/otbMinCompositeFunctor.h"
#include "include/otbMaxCompositeFunctor.h"
#include "include/otbWeightedAverageCompositeFunctor.h"
#include "include/otbMedianCompositeFunctor.h"
#include "include/otbLastValueCompositeFunctor.h"

#include "otbImageToGenericRSOutputParameters.h"
#include "otbGenericRSResampleImageFilter.h"

#include "itkLinearInterpolateImageFunction.h"
#include "otbBCOInterpolateImageFunction.h"
#include "itkNearestNeighborInterpolateImageFunction.h"

#include "otbExtractROI.h"

#include "otbGeographicalDistance.h"
#include "itkBinaryFunctorImageFilter.h"
#include "itkUnaryFunctorImageFilter.h"
#include "itkNaryFunctorImageFilter.h"

namespace otb
{

//template< class TInput, class TOutput>
//class GenericCompositeFunctor
//{
//public:
//    GenericCompositeFunctor() : m_NoDataValue(0.), m_UseNoDataValue(false),
//                                m_HasMasks(false), m_MskValidValue(0) {}

//    void SetNoDataValue(float val) { m_NoDataValue = val; }
//    void UseNoDataValueOn() { m_UseNoDataValue = true;}
//    void UseNoDataValueOff() { m_UseNoDataValue = false;}
//    void SetUseNoDataValue(bool val) { m_UseNoDataValue = val;}

//    void HasMasksOn() { m_HasMasks = true;}
//    void HasMasksOff() { m_HasMasks = false;}
//    void SetHasMasksValue(bool val) { m_HasMasks = val;}
//    void SetMskValidValue(float val) { m_MskValidValue = val;}

// //    GenericCompositeFunctor& operator =(const GenericCompositeFunctor&) {
// //        return *this;
// //    }
//    bool operator!=( const GenericCompositeFunctor & other) const {
//        return m_NoDataValue != other.m_NoDataValue ||
//                m_UseNoDataValue != other.m_UseNoDataValue ||
//                m_HasMasks != other.m_HasMasks ||
//                m_MskValidValue != other.m_MskValidValue;}
//    bool operator==( const GenericCompositeFunctor &other) const { return !(this != other); }

//    TOutput operator()( const std::vector< TInput > & inputIts) {
//        TOutput outPix;
//        double sum = 0;
//        int count = 0;
//        int i = 0;
//        bool mskValidPixel = true;
//        size_t imgsCnt = m_HasMasks ? (inputIts.size() / 2) : inputIts.size();
//        for (i = 0; i<imgsCnt; i++)
//        {
//            const auto &inPix = inputIts[i];
//            mskValidPixel = (!m_HasMasks || (inputIts[i+imgsCnt] == m_MskValidValue));
//            if (mskValidPixel && (!m_UseNoDataValue || inPix != m_NoDataValue) && inPix > 0)
//            {
//                sum += inPix;
//                count++;
//            }
//        }

//        if (count > 0)
//        {
//            outPix = static_cast<TOutput>(sum / count);
//        }
//        else
//        {
//            outPix = 0;
//        }
//        return outPix;
//    }

//private:
//    float               m_NoDataValue;
//    bool                m_UseNoDataValue;
//    bool                m_HasMasks;
//    float               m_MskValidValue;
//};

enum
{
  Interpolator_BCO,
  Interpolator_NNeighbor,
  Interpolator_Linear
};

const float DefaultGridSpacingMeter = 4.0;

namespace Wrapper
{

class Sen4XGenericComposite : public Application
{
public:
  /** Standard class typedefs. */
  typedef Sen4XGenericComposite            Self;
  typedef Application                   Superclass;
  typedef itk::SmartPointer<Self>       Pointer;
  typedef itk::SmartPointer<const Self> ConstPointer;

  /** Standard macro */
  itkNewMacro(Self);

  itkTypeMacro(Sen4XGenericComposite, otb::Wrapper::Application);

  typedef FloatImageType                                                InputImageType;
  typedef FloatImageType                                                OutputImageType;
  typedef InputImageType::PixelType                                     InputPixelType;
  typedef OutputImageType::PixelType                                    OutputPixelType;
  typedef otb::ImageList<InputImageType>                                ImageListType;
  typedef otb::ImageFileReader<InputImageType>                          ReaderType;
  typedef otb::ObjectList<ReaderType>                                   ReaderListType;
  typedef otb::ExtractROI
              <InputPixelType, OutputPixelType>                         ExtractROIFilterType;
  typedef otb::ObjectList<ExtractROIFilterType>                         ExtractROIListType;

  using CompositeFunctorType    = otb::Functor::CompositeFunctor<InputPixelType, OutputPixelType, InputPixelType>;
  using CompositeFunctorWrapperType    = otb::Functor::CompositeWrapperFunctor<InputPixelType, OutputPixelType, CompositeFunctorType>;

//   typedef GenericCompositeFunctor<InputPixelType, OutputPixelType>    GenericCompositeFunctorType;
  typedef itk::NaryFunctorImageFilter< InputImageType, OutputImageType, CompositeFunctorWrapperType> CompositeFilterType;
  // TODO: This should be commented, used only for comparisons
  typedef otb::ImageListCompositeFilter<InputImageType, OutputImageType, CompositeFunctorType> ImageListCompositeFilterType;
  /** Generic Remote Sensor Resampler */
  typedef otb::GenericRSResampleImageFilter<InputImageType,
                                            InputImageType>             ResampleFilterType;

  /** Interpolators typedefs*/
  typedef itk::LinearInterpolateImageFunction<InputImageType,
                                              double>              LinearInterpolationType;
  typedef itk::NearestNeighborInterpolateImageFunction<InputImageType,
                                                       double>     NearestNeighborInterpolationType;
  typedef otb::BCOInterpolateImageFunction<InputImageType>         BCOInterpolationType;

  class CompositeDescriptor
  {
  public:
    CompositeDescriptor(std::string k, std::string i, CompositeFunctorType* functor) : key(k), item(i), functor(functor)
    {
    }
    std::string                           key;
    std::string                           item;
    std::unique_ptr<CompositeFunctorType> functor;
  };

private:
  void DoInit() override
  {
      SetName("Sen4XGenericComposite");
      SetDescription("Computes a composite of multiple images");

      SetDocName("Sen4XGenericComposite");
      SetDocLongDescription("Computes a mean composite of multiple images.");
      SetDocLimitations("None");
      SetDocAuthors("CUU");
      SetDocSeeAlso(" ");

      AddDocTag(Tags::Raster);

      AddParameter(ParameterType_InputFilenameList, "il", "Input images");
      SetParameterDescription("il", "The list of input images");

      AddParameter(ParameterType_InputFilenameList, "msks", "Input mask images");
      SetParameterDescription("msks", "The list of input mask images. If provided it should have the same size with the input images list");
      MandatoryOff("msks");

      AddParameter(ParameterType_Float, "bv", "Background value");
      SetParameterDescription("bv", "Background value to ignore in computation.");
      SetDefaultParameterFloat("bv", 0.);
      MandatoryOff("bv");

      AddParameter(ParameterType_Float, "mskvld", "Mask valid value");
      SetParameterDescription("mskvld", "Mask valid value for which the input values are considered.");
      SetDefaultParameterFloat("mskvld", 0.);
      MandatoryOff("mskvld");

      AddParameter(ParameterType_OutputImage, "out", "Output image");
      SetParameterDescription("out", "Output image.");

      AddParameter(ParameterType_String, "method", "Compositing method.");
      SetParameterDescription("method", "Compositing method.");
      MandatoryOff("method");

      AddParameter(ParameterType_Int, "mosaic", "Perform mosaic.");
      SetParameterDescription("mosaic", "If images are not alligned, a mosaic is created, assuming they are at the same resolution and projection.");
      SetDefaultParameterInt("mosaic", 0);
      MandatoryOff("mosaic");

      // TODO : Add here also the dates of the rasters (optional)

      AddParameter(ParameterType_Group, "srcwin", "Source window");
      SetParameterDescription("srcwin","This group of parameters allows one to define a source image window to process.");

      AddParameter(ParameterType_Float, "srcwin.ulx", "Upper Left X");
      SetParameterDescription("srcwin.ulx","Cartographic X coordinate of upper-left corner (meters for cartographic projections, degrees for geographic ones)");

      AddParameter(ParameterType_Float, "srcwin.uly", "Upper Left Y");
      SetParameterDescription("srcwin.uly","Cartographic Y coordinate of upper-left corner (meters for cartographic projections, degrees for geographic ones)");

      AddParameter(ParameterType_Float, "srcwin.lrx", "Lower right X");
      SetParameterDescription("srcwin.lrx","Cartographic X coordinate of lower-right corner (meters for cartographic projections, degrees for geographic ones)");

      AddParameter(ParameterType_Float, "srcwin.lry", "Lower right Y");
      SetParameterDescription("srcwin.lry","Cartographic Y coordinate of lower-right corner (meters for cartographic projections, degrees for geographic ones)");

      MandatoryOff("srcwin.ulx");
      MandatoryOff("srcwin.uly");
      MandatoryOff("srcwin.lrx");
      MandatoryOff("srcwin.lry");

      // Add the output parameters in a group
      AddParameter(ParameterType_Group, "outputs", "Output Image Grid");
      SetParameterDescription("outputs","This group of parameters allows one to define the grid on which the input image will be resampled.");

      // Upper left point coordinates
      AddParameter(ParameterType_Float, "outputs.ulx", "Upper Left X");
      SetParameterDescription("outputs.ulx","Cartographic X coordinate of upper-left corner (meters for cartographic projections, degrees for geographic ones)");

      AddParameter(ParameterType_Float, "outputs.uly", "Upper Left Y");
      SetParameterDescription("outputs.uly","Cartographic Y coordinate of the upper-left corner (meters for cartographic projections, degrees for geographic ones)");

      // Size of the output image
      AddParameter(ParameterType_Int, "outputs.sizex", "Size X");
      SetParameterDescription("outputs.sizex","Size of projected image along X (in pixels)");

      AddParameter(ParameterType_Int, "outputs.sizey", "Size Y");
      SetParameterDescription("outputs.sizey","Size of projected image along Y (in pixels)");

      // Spacing of the output image
      AddParameter(ParameterType_Float, "outputs.spacingx", "Pixel Size X");
      SetParameterDescription("outputs.spacingx","Size of each pixel along X axis (meters for cartographic projections, degrees for geographic ones)");

      // Add the output parameters in a group
      AddParameter(ParameterType_Group, "outputs", "Output Image Grid");
      SetParameterDescription("outputs", "This group of parameters allows one to define the grid on which the input images will be resampled.");

      AddParameter(ParameterType_Float, "outputs.spacingy", "Pixel Size Y");
      SetParameterDescription("outputs.spacingy","Size of each pixel along Y axis (meters for cartographic projections, degrees for geographic ones)");

      // Lower right point coordinates
      AddParameter(ParameterType_Float, "outputs.lrx", "Lower right X");
      SetParameterDescription("outputs.lrx","Cartographic X coordinate of the lower-right corner (meters for cartographic projections, degrees for geographic ones)");

      AddParameter(ParameterType_Float, "outputs.lry", "Lower right Y");
      SetParameterDescription("outputs.lry","Cartographic Y coordinate of the lower-right corner (meters for cartographic projections, degrees for geographic ones)");

      DisableParameter("outputs.lrx");
      DisableParameter("outputs.lry");

      MandatoryOff("outputs.ulx");
      MandatoryOff("outputs.uly");
      MandatoryOff("outputs.spacingx");
      MandatoryOff("outputs.spacingy");
      MandatoryOff("outputs.sizex");
      MandatoryOff("outputs.sizey");
      MandatoryOff("outputs.lrx");
      MandatoryOff("outputs.lry");

      // Existing ortho image that can be used to compute size, origin and spacing of the output
//        AddParameter(ParameterType_InputImage, "ref", "Model ortho-image");
//        SetParameterDescription("ref","A model ortho-image that can be used to compute size, origin and spacing of the output");

      // Interpolators
      AddParameter(ParameterType_Choice,   "interpolator", "Interpolation");
      AddChoice("interpolator.bco",    "Bicubic interpolation");
      AddParameter(ParameterType_Radius, "interpolator.bco.radius", "Radius for bicubic interpolation");
      SetParameterDescription("interpolator.bco.radius","This parameter allows one to control the size of the bicubic interpolation filter. If the target pixel size is higher than the input pixel size, increasing this parameter will reduce aliasing artifacts.");
      SetParameterDescription("interpolator","This group of parameters allows one to define how the input image will be interpolated during resampling.");
      AddChoice("interpolator.nn",     "Nearest Neighbor interpolation");
      SetParameterDescription("interpolator.nn","Nearest neighbor interpolation leads to poor image quality, but it is very fast.");
      AddChoice("interpolator.linear", "Linear interpolation");
      SetParameterDescription("interpolator.linear","Linear interpolation leads to average image quality but is quite fast");
      SetDefaultParameterInt("interpolator.bco.radius", 2);

      AddParameter(ParameterType_Group,"opt","Speed optimization parameters");
      SetParameterDescription("opt","This group of parameters allows optimization of processing time.");

      // Displacement Field Spacing
      AddParameter(ParameterType_Float, "opt.gridspacing", "Resampling grid spacing");
      SetDefaultParameterFloat("opt.gridspacing", DefaultGridSpacingMeter);
      SetParameterDescription("opt.gridspacing",
                              "Resampling is done according to a coordinate mapping deformation grid, "
                              "whose pixel size is set by this parameter, and "
                              "expressed in the coordinate system of the output image "
                              "The closer to the output spacing this parameter is, "
                              "the more precise will be the ortho-rectified image,"
                              "but increasing this parameter will reduce processing time.");
      MandatoryOff("opt.gridspacing");

      AddRAMParameter();

      SetDocExampleParameterValue("il", "image1.tif image2.tif");
//        SetDocExampleParameterValue("ref", "reference.tif");
      SetDocExampleParameterValue("out", "output.tif");

      m_Map.clear();

      m_Map.push_back({"mean", "Composite:MEAN", new otb::Functor::MeanCompositeFunctor<InputPixelType, OutputPixelType>()});
      m_Map.push_back({"min", "Composite:MIN", new otb::Functor::MinCompositeFunctor<InputPixelType, OutputPixelType>()});
      m_Map.push_back({"max", "Composite:MAX", new otb::Functor::MaxCompositeFunctor<InputPixelType, OutputPixelType>()});
      m_Map.push_back({"median", "Composite:MEDIAN", new otb::Functor::MedianCompositeFunctor<InputPixelType, OutputPixelType>()});
      m_Map.push_back({"waverage", "Composite:WEIGHTED_AVERAGE", new otb::Functor::WeightedAverageCompositeFunctor<InputPixelType, OutputPixelType>()});
      m_Map.push_back({"last", "Composite:LAST", new otb::Functor::LastValueCompositeFunctor<InputPixelType, OutputPixelType>()});

      // TODO : Add new functors
      // m_Map.push_back({"last", "Composite:LAST", new otb::Functor::LastValueCompositeFunctor<InputPixelType, OutputPixelType>()});
      // m_Map.push_back({"median", "Composite:MEDIAN", new otb::Functor::MedianCompositeFunctor<InputPixelType, OutputPixelType>()});

  }

  void DoUpdateParameters() override
  {{
          // Make all the parameters in this mode mandatory
          MandatoryOff("outputs.ulx");
          MandatoryOff("outputs.uly");
          MandatoryOff("outputs.spacingx");
          MandatoryOff("outputs.spacingy");
          MandatoryOff("outputs.sizex");
          MandatoryOff("outputs.sizey");
          MandatoryOff("outputs.lrx");
          MandatoryOff("outputs.lry");

          // Disable the parameters
          DisableParameter("outputs.ulx");
          DisableParameter("outputs.uly");
          DisableParameter("outputs.spacingx");
          DisableParameter("outputs.spacingy");
          DisableParameter("outputs.sizex");
          DisableParameter("outputs.sizey");
          DisableParameter("outputs.lrx");
          DisableParameter("outputs.lry");

  //        if (!HasValue("ref"))
  //        {
  //            return;
  //        }

  //        auto inOrtho = GetParameterImage("ref");

  //        ResampleFilterType::OriginType orig = inOrtho->GetOrigin();
  //        ResampleFilterType::SpacingType spacing = inOrtho->GetSpacing();
  //        ResampleFilterType::SizeType size = inOrtho->GetLargestPossibleRegion().GetSize();
  //        m_OutputProjectionRef = inOrtho->GetProjectionRef();

  //        SetParameterInt("outputs.sizex",size[0]);
  //        SetParameterInt("outputs.sizey",size[1]);
  //        SetParameterFloat("outputs.spacingx",spacing[0]);
  //        SetParameterFloat("outputs.spacingy",spacing[1]);
  //        SetParameterFloat("outputs.ulx",orig[0] - 0.5 * spacing[0]);
  //        SetParameterFloat("outputs.uly",orig[1] - 0.5 * spacing[1]);

          if (!HasUserValue("outputs.sizex") && HasUserValue("outputs.lrx") && HasUserValue("outputs.spacingx") && std::abs(GetParameterFloat("outputs.spacingx")) > 0.0) {
            SetParameterInt("outputs.sizex",static_cast<int>(std::ceil((GetParameterFloat("outputs.lrx")-GetParameterFloat("outputs.ulx"))/GetParameterFloat("outputs.spacingx"))));
          }
          if (!HasUserValue("outputs.sizey") && HasUserValue("outputs.lry") && HasUserValue("outputs.spacingy") && std::abs(GetParameterFloat("outputs.spacingy")) > 0.0) {
            SetParameterInt("outputs.sizey",static_cast<int>(std::ceil((GetParameterFloat("outputs.lry")-GetParameterFloat("outputs.uly"))/GetParameterFloat("outputs.spacingy"))));
          }

          // Update lower right
          if (!HasUserValue("outputs.lrx") && HasUserValue("outputs.ulx") && HasUserValue("outputs.spacingx") && HasUserValue("outputs.sizex")) {
              SetParameterFloat("outputs.lrx",GetParameterFloat("outputs.ulx") + GetParameterFloat("outputs.spacingx") * static_cast<double>(GetParameterInt("outputs.sizex")));
          }
          if (!HasUserValue("outputs.lry") && HasUserValue("outputs.uly") && HasUserValue("outputs.spacingy") && HasUserValue("outputs.sizey")) {
              SetParameterFloat("outputs.lry",GetParameterFloat("outputs.uly") + GetParameterFloat("outputs.spacingy") * static_cast<double>(GetParameterInt("outputs.sizey")));
          }

          if (!HasValue("outputs.ulx") || !HasValue("outputs.spacingx") || !HasValue("outputs.sizex") ||
                  !HasValue("outputs.uly") || !HasValue("outputs.spacingy") || !HasValue("outputs.sizey")) {
//              const auto &inImages = GetParameterStringList("il");
//              auto reader = ReaderType::New();
//              reader->SetFileName(inImages[0]);
//              reader->UpdateOutputInformation();
//              const auto inOrtho = reader->GetOutput();
//              ResampleFilterType::OriginType orig = inOrtho->GetOrigin();
//              ResampleFilterType::SpacingType spacing = inOrtho->GetSpacing();
//              ResampleFilterType::SizeType size = inOrtho->GetLargestPossibleRegion().GetSize();
//              m_OutputProjectionRef = inOrtho->GetProjectionRef();

//              SetParameterInt("outputs.sizex",size[0]);
//              SetParameterInt("outputs.sizey",size[1]);
//              SetParameterFloat("outputs.spacingx",spacing[0]);
//              SetParameterFloat("outputs.spacingy",spacing[1]);
//              SetParameterFloat("outputs.ulx",orig[0] - 0.5 * spacing[0]);
//              SetParameterFloat("outputs.uly",orig[1] - 0.5 * spacing[1]);
              return;
          }

          if (!HasUserValue("opt.gridspacing"))
            {
            // Update opt.gridspacing
            // In case output coordinate system is WG84,
            if (m_OutputProjectionRef == otb::GeoInformationConversion::ToWKT(4326))
              {
              // How much is 4 meters in degrees ?
              typedef itk::Point<float,2> FloatPointType;
              FloatPointType point1, point2;

              typedef otb::GeographicalDistance<FloatPointType> GeographicalDistanceType;
              GeographicalDistanceType::Pointer geoDistanceCalculator = GeographicalDistanceType::New();

              // center
              point1[0] = GetParameterFloat("outputs.ulx") + GetParameterFloat("outputs.spacingx") * GetParameterInt("outputs.sizex") / 2;
              point1[1] = GetParameterFloat("outputs.uly") + GetParameterFloat("outputs.spacingy") * GetParameterInt("outputs.sizey") / 2;

              // center + [1,0]
              point2[0] = point1[0] + GetParameterFloat("outputs.spacingx");
              point2[1] = point1[1];
              double xgroundspacing = geoDistanceCalculator->Evaluate(point1, point2);
              otbAppLogINFO( "Output X ground spacing in meter = " << xgroundspacing );

              // center + [0,1]
              point2[0] = point1[0];
              point2[1] = point1[1] + GetParameterFloat("outputs.spacingy");
              double ygroundspacing = geoDistanceCalculator->Evaluate(point1, point2);
              otbAppLogINFO( "Output Y ground spacing in meter = " << ygroundspacing );

              double xgridspacing = DefaultGridSpacingMeter * GetParameterFloat("outputs.spacingx") / xgroundspacing;
              double ygridspacing = DefaultGridSpacingMeter * GetParameterFloat("outputs.spacingy") / ygroundspacing;

              otbAppLogINFO( << DefaultGridSpacingMeter << " meters in X direction correspond roughly to "
                             << xgridspacing << " degrees" );
              otbAppLogINFO( << DefaultGridSpacingMeter << " meters in Y direction correspond roughly to "
                             << ygridspacing << " degrees" );

              // Use the smallest spacing (more precise grid)
              double optimalSpacing = std::min( std::abs(xgridspacing), std::abs(ygridspacing) );
              otbAppLogINFO( "Setting grid spacing to " << optimalSpacing );
              SetParameterFloat("opt.gridspacing",optimalSpacing);
              }
            else
              {
              // Use the smallest spacing (more precise grid)
              double optimalSpacing = std::min( std::abs(GetParameterFloat("outputs.spacingx")), std::abs(GetParameterFloat("outputs.spacingy")) );
              otbAppLogINFO( "Setting grid spacing to " << optimalSpacing );
              SetParameterFloat("opt.gridspacing",optimalSpacing);
              }
            }
      }
  }

  void DoExecute() override
  {
      auto inImages = GetParameterStringList("il");
      // Just return the image if a single image
      if (inImages.size() == 1)
      {
          SetParameterOutputImage("out", GetReader(inImages[0])->GetOutput());
          return;
      }

      bool useMasks = false;
      if (HasValue("msks")) {
          const auto &mskImages = GetParameterStringList("msks");
          if (mskImages.size() != inImages.size()) {
              otbAppLogFATAL("The number of mask images should be equal with the ones of the input images (msks = " <<
                             mskImages.size() << ", images = " << inImages.size() << ")!");
          }
          inImages.insert(inImages.end(), mskImages.begin(), mskImages.end());
          useMasks = true;
      }
      // m_ReprojectedImages = ImageListType::New();
      auto first = true;
      for (const auto &file : inImages)
      {
          auto reader = GetReader(file);
          const auto inImage = reader->GetOutput();

          if (!HasValue("outputs.ulx") || !HasValue("outputs.spacingx") || !HasValue("outputs.sizex") ||
                  !HasValue("outputs.uly") || !HasValue("outputs.spacingy") || !HasValue("outputs.sizey")) {
              m_CompositingImages.push_back(inImage);
          } else {
              m_CompositingImages.push_back(GetResampledImage(inImage, first));
          }
          first = false;
      }

      //      TODO to compare performances (not compilable yet)
      //      ImageListCompositeFilterType::Pointer filter1 = ImageListCompositeFilterType::New();
      //      filter1->SetFunctor(m_CompositeFunctor);
      //      filter1->SetInput(m_ReprojectedImages);

      CompositeFunctorType *pFunctor = GetCompositeFunctor();
      if (HasUserValue("bv"))
      {
        pFunctor->UseNoDataValueOn();
      }
      pFunctor->SetNoDataValue(GetParameterFloat("bv"));
      pFunctor->SetHasMasksValue(useMasks);
      pFunctor->SetMskValidValue(GetParameterFloat("mskvld"));

      m_CompositeFilter = CompositeFilterType::New();
      m_CompositeFilter->GetFunctor().SetFunctor(pFunctor);
      int i = 0;
      for (const auto &img: m_CompositingImages)
      {
          m_CompositeFilter->SetInput(i, img);
          i++;
      }
      // Output Image
      SetParameterOutputImage("out", m_CompositeFilter->GetOutput());
  }

  InputImageType::Pointer GetResampledImage(const InputImageType::Pointer &inImage, bool first) {
      // Resampler Instantiation
      auto resampleFilter = ResampleFilterType::New();
      m_ResampleFilters.emplace_back(resampleFilter);
      resampleFilter->SetInput(inImage);

      // Set the output projection Ref
      resampleFilter->SetInputProjectionRef(inImage->GetProjectionRef());
      resampleFilter->SetOutputProjectionRef(inImage->GetProjectionRef());
      resampleFilter->SetInputKeywordList(inImage->GetImageKeywordlist());
//            resampleFilter->SetOutputProjectionRef(m_OutputProjectionRef);

      // Check size
      if (GetParameterInt("outputs.sizex") <= 0 || GetParameterInt("outputs.sizey") <= 0)
        {
        otbAppLogCRITICAL("Wrong value : negative size : ("<<GetParameterInt("outputs.sizex")<<" , "<<GetParameterInt("outputs.sizey")<<")");
        }

      //Check spacing sign
      if (GetParameterFloat("outputs.spacingy") > 0.)
        {
        otbAppLogWARNING(<<"Wrong value for outputs.spacingy: Pixel size along Y axis should be negative, (outputs.spacingy=" <<GetParameterFloat("outputs.spacingy") << ")" )
        }

      // Get Interpolator
      switch ( GetParameterInt("interpolator") )
        {
        case Interpolator_Linear:
        {
        if (first)
          {
          otbAppLogINFO(<< "Using linear interpolator");
          }
        LinearInterpolationType::Pointer interpolator = LinearInterpolationType::New();
        resampleFilter->SetInterpolator(interpolator);
        }
        break;
        case Interpolator_NNeighbor:
        {
        if (first)
          {
          otbAppLogINFO(<< "Using nn interpolator");
          }
        NearestNeighborInterpolationType::Pointer interpolator = NearestNeighborInterpolationType::New();
        resampleFilter->SetInterpolator(interpolator);
        }
        break;
        case Interpolator_BCO:
        {
        if (first)
          {
            otbAppLogINFO(<< "Using BCO interpolator");
          }
        BCOInterpolationType::Pointer interpolator = BCOInterpolationType::New();
        interpolator->SetRadius(GetParameterInt("interpolator.bco.radius"));
        resampleFilter->SetInterpolator(interpolator);
        }
        break;
        }

      // Set Output information
      ResampleFilterType::SizeType size;
      size[0] = GetParameterInt("outputs.sizex");
      size[1] = GetParameterInt("outputs.sizey");
      resampleFilter->SetOutputSize(size);

      ResampleFilterType::SpacingType spacing;
      spacing[0] = GetParameterFloat("outputs.spacingx");
      spacing[1] = GetParameterFloat("outputs.spacingy");
      resampleFilter->SetOutputSpacing(spacing);

      ResampleFilterType::OriginType origin;
      origin[0] = GetParameterFloat("outputs.ulx") + 0.5 * GetParameterFloat("outputs.spacingx");
      origin[1] = GetParameterFloat("outputs.uly") + 0.5 * GetParameterFloat("outputs.spacingy");
      resampleFilter->SetOutputOrigin(origin);

      // Build the default pixel
      InputImageType::PixelType defaultValue = GetParameterFloat("bv");
      resampleFilter->SetEdgePaddingValue(defaultValue);

      if (first)
        {
        otbAppLogINFO("Generating output with size = " << size);
        otbAppLogINFO("Generating output with pixel spacing = " << spacing);
        otbAppLogINFO("Generating output with origin = " << origin);
        otbAppLogINFO("Area outside input image bounds will have a pixel value of " << defaultValue);
        }

      // Displacement Field spacing
      ResampleFilterType::SpacingType gridSpacing;
      if (IsParameterEnabled("opt.gridspacing"))
        {
        gridSpacing[0] = GetParameterFloat("opt.gridspacing");
        gridSpacing[1] = -GetParameterFloat("opt.gridspacing");

        if ( GetParameterFloat( "opt.gridspacing" ) == 0 )
          {
          otbAppLogFATAL( "opt.gridspacing must be different from 0 " );
          }

        // Predict size of deformation grid
        ResampleFilterType::SpacingType deformationGridSize;
        deformationGridSize[0] = static_cast<ResampleFilterType::SpacingType::ValueType >(std::abs(
            GetParameterInt("outputs.sizex") * GetParameterFloat("outputs.spacingx") / GetParameterFloat("opt.gridspacing") ));
        deformationGridSize[1] = static_cast<ResampleFilterType::SpacingType::ValueType>(std::abs(
            GetParameterInt("outputs.sizey") * GetParameterFloat("outputs.spacingy") / GetParameterFloat("opt.gridspacing") ));
        if (first)
          {
          otbAppLogINFO("Using a deformation grid with a physical spacing of " << GetParameterFloat("opt.gridspacing"));
          otbAppLogINFO("Using a deformation grid of size " << deformationGridSize);
          }

        if (deformationGridSize[0] * deformationGridSize[1] == 0)
          {
          otbAppLogFATAL("Deformation grid degenerated (size of 0). "
              "You shall set opt.gridspacing appropriately. "
              "opt.gridspacing units are the same as outputs.spacing units");
          }

        if (std::abs(GetParameterFloat("opt.gridspacing")) < std::abs(GetParameterFloat("outputs.spacingx"))
             || std::abs(GetParameterFloat("opt.gridspacing")) < std::abs(GetParameterFloat("outputs.spacingy")) )
          {
          otbAppLogWARNING("Spacing of deformation grid should be at least equal to "
              "spacing of output image. Otherwise, computation time will be slow, "
              "and precision of output will not be better. "
              "You shall set opt.gridspacing appropriately. "
              "opt.gridspacing units are the same as outputs.spacing units");
          }

        resampleFilter->SetDisplacementFieldSpacing(gridSpacing);
      }

      return resampleFilter->GetOutput();
  }

  CompositeFunctorType * GetCompositeFunctor() {
      std::string method = GetParameterString("method");
      if (method.size() == 0) {
          method = "mean";
      }
      for (const auto &spec: m_Map) {
          if (spec.key == method) {
              return spec.functor.get();
          }
      }
      throw std::runtime_error("Cannot find any functor for method " + method);
  }

  ReaderType::Pointer GetReader(const std::string &inFile) {
      if (m_Readers.IsNull()) {
        m_Readers = ReaderListType::New();
      }
      auto reader = ReaderType::New();
      reader->SetFileName(inFile);
      reader->UpdateOutputInformation();
      m_Readers->PushBack(reader);
      return reader;
  }

  ReaderListType::Pointer                      m_Readers;
  ExtractROIListType::Pointer                  m_ExtractROIFilters;
  std::vector<ResampleFilterType::Pointer>     m_ResampleFilters;
  //ImageListType::Pointer                       m_ReprojectedImages;
  std::vector<InputImageType::Pointer>         m_CompositingImages;
  //CompositeFunctorType *                        m_CompositeFunctor;
  CompositeFilterType::Pointer                 m_CompositeFilter;
  std::string                                  m_OutputProjectionRef;
  std::string                                  m_Method;
  std::vector<CompositeDescriptor>                  m_Map;
};
}
}

OTB_APPLICATION_EXPORT(otb::Wrapper::Sen4XGenericComposite)

