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

#include "otbStreamingSimpleMosaicFilter.h"

namespace otb
{

// TODO: This class is kept as an alternative for the NaryFunctorImageType implementation.
//       Should be checked its performances compared with the current one
template <class TInputImageType, class TOutputImageType, class TFunction >
class ITK_EXPORT ImageListCompositeFilter
  : public ImageListToImageFilter<TInputImageType, TInputImageType>
{
public:
  /** Standard typedefs */
  typedef ImageListCompositeFilter                           Self;
  typedef ImageListToImageFilter
      <TInputImageType, TInputImageType>            Superclass;
  typedef itk::SmartPointer<Self>                   Pointer;
  typedef itk::SmartPointer<const Self>             ConstPointer;

  typedef TInputImageType                           InputImageType;
  typedef typename InputImageType::Pointer          InputImagePointerType;
  typedef typename InputImageType::PixelType        InputPixelType;
  typedef ImageList<InputImageType>                 InputImageListType;
  typedef TOutputImageType                          OutputImageType;
  typedef typename TOutputImageType::PixelType      OutputPixelType;
  typedef typename OutputImageType::Pointer         OutputImagePointerType;
  typedef double                                    PrecisionType;

  typedef typename OutputImageType::RegionType OutputImageRegionType;
  typedef typename InputImageType::RegionType  InputImageRegionType;

  using FunctorType = TFunction;

  /** Type macro */
  itkNewMacro(Self);

  /** Creation through object factory macro */
  itkTypeMacro(ImageListCompositeFilter, ImageListToImageFilter);

  itkGetMacro(NoDataValue, InputPixelType);
  itkSetMacro(NoDataValue, InputPixelType);
  itkGetMacro(UseNoDataValue, bool);
  itkSetMacro(UseNoDataValue, bool);
  itkBooleanMacro(UseNoDataValue);

  void SetFunctor(FunctorType & functor);
  FunctorType GetFunctor();

protected:
  /** Constructor */
  ImageListCompositeFilter();
  /** Destructor */
  ~ImageListCompositeFilter() override {}

  void GenerateInputRequestedRegion() override;
  void GenerateOutputInformation() override;
  void ThreadedGenerateData(const OutputImageRegionType &outputRegionForThread, itk::ThreadIdType threadId) override;

  /**PrintSelf method */
  void PrintSelf(std::ostream& os, itk::Indent indent) const override;


private:
  ImageListCompositeFilter(const Self &) = delete;
  void operator =(const Self&) = delete;

  InputPixelType            m_NoDataValue;
  bool                      m_UseNoDataValue;
  TFunction                 m_Functor;
};

template <class TInputImageType, class TOutputImageType, class TFunction>
ImageListCompositeFilter<TInputImageType, TOutputImageType, TFunction>
::ImageListCompositeFilter()
 : m_NoDataValue(),
   m_UseNoDataValue()
{
  this->SetNumberOfRequiredInputs(1);
  this->SetNumberOfRequiredOutputs(1);
}


template <class TInputImageType, class TOutputImageType, class TFunction>
void
ImageListCompositeFilter<TInputImageType, TOutputImageType, TFunction>
::SetFunctor(TFunction &functor) {
    if (m_Functor != functor) {
        m_Functor = functor;
        this->Modified();
    }
}

template <class TInputImageType, class TOutputImageType, class TFunction>
TFunction
ImageListCompositeFilter<TInputImageType, TOutputImageType, TFunction>
::GetFunctor() {
    return m_Functor;
}

template <class TInputImageType, class TOutputImageType, class TFunction>
void
ImageListCompositeFilter<TInputImageType, TOutputImageType, TFunction>
::GenerateInputRequestedRegion(void)
{
  auto inputPtr = this->GetInput();
  for (auto inputListIt = inputPtr->Begin(); inputListIt != inputPtr->End(); ++inputListIt)
    {
    inputListIt.Get()->SetRequestedRegion(this->GetOutput()->GetRequestedRegion());
    }
}

template <class TInputImageType, class TOutputImageType, class TFunction>
void
ImageListCompositeFilter<TInputImageType, TOutputImageType, TFunction>
::GenerateOutputInformation()
{
  if (this->GetOutput())
    {
    if (this->GetInput()->Size() > 0)
      {
      this->GetOutput()->CopyInformation(this->GetInput()->GetNthElement(0));
      this->GetOutput()->SetLargestPossibleRegion(this->GetInput()->GetNthElement(0)->GetLargestPossibleRegion());
      this->GetOutput()->SetNumberOfComponentsPerPixel(1);
      }
    }
}

template <class TInputImageType, class TOutputImageType, class TFunction>
void
ImageListCompositeFilter<TInputImageType, TOutputImageType, TFunction>
::ThreadedGenerateData(const OutputImageRegionType &outputRegionForThread, itk::ThreadIdType threadId)
{
  auto inputPtr = this->GetInput();
  auto inputImages = this->GetInput()->Size();

  OutputImagePointerType  outputPtr = this->GetOutput();

  typedef itk::ImageRegionConstIteratorWithIndex<InputImageType> InputIteratorType;
  typedef itk::ImageRegionIteratorWithIndex<OutputImageType>     OutputIteratorType;

  itk::ProgressReporter progress(this, threadId, outputRegionForThread.GetNumberOfPixels());

  OutputIteratorType outputIt(outputPtr, outputRegionForThread);
  outputIt.GoToBegin();

  std::vector<InputIteratorType> inputIts;
  inputIts.reserve(inputImages);
  for (auto inputListIt = inputPtr->Begin(); inputListIt != inputPtr->End(); ++inputListIt)
    {
    InputIteratorType inputIt(inputListIt.Get(), outputRegionForThread);
    inputIts.emplace_back(std::move(inputIt));
    inputIts.back().GoToBegin();
    }

  InputPixelType zero = m_UseNoDataValue ? m_NoDataValue : 0;

  OutputPixelType outPix;
  PrecisionType sum;
  int count;

  while (!outputIt.IsAtEnd())
    {
    sum = 0;
    count = 0;

    outPix = m_Functor(inputIts);
//    for (auto &it : inputIts)
//      {
//      const auto &inPix = it.Get();
//      if ((!m_UseNoDataValue || inPix != m_NoDataValue) && inPix > 0)
//        {
//        sum += inPix;
//        count++;
//        }
//      ++it;
//      }

//    if (count > 0)
//      {
//      outPix = static_cast<OutputPixelType>(sum / count);
//      }
//      else
//      {
//      outPix = 0;
//      }

    outputIt.Set(outPix);
    ++outputIt;
    progress.CompletedPixel();
    }
}

/**
 * PrintSelf Method
 */
template <class TInputImageType, class TOutputImageType, class TFunction>
void
ImageListCompositeFilter<TInputImageType, TOutputImageType, TFunction>
::PrintSelf(std::ostream& os, itk::Indent indent) const
{
  Superclass::PrintSelf(os, indent);
}

template< class TInput, class TOutput, class TInput2=TInput>
class GenericCompositeFunctor2
{
    typedef itk::ImageRegionConstIteratorWithIndex<TInput> InputIteratorType;
    typedef itk::ImageRegionIteratorWithIndex<TOutput>     OutputIteratorType;
    typedef TOutput                                         OutputPixelType;
    typedef double                                          PrecisionType;


public:
    GenericCompositeFunctor2() : m_NoDataValue(0.), m_UseNoDataValue(false),
                                m_MskValidValue(0) {}

    void SetNoDataValue(float val) { m_NoDataValue = val;}
    void UseNoDataValueOn() { m_UseNoDataValue = true;}
    void UseNoDataValueOff() { m_UseNoDataValue = false;}
    void SetUseNoDataValue(bool val) { m_UseNoDataValue = val;}

    void SetMskValidValue(float val) { m_MskValidValue = val;}

//    GenericCompositeFunctor& operator =(const GenericCompositeFunctor&) {
//        return *this;
//    }
    bool operator!=( const GenericCompositeFunctor2 & other) const {
        return m_NoDataValue != other.m_NoDataValue ||
                m_UseNoDataValue != other.m_UseNoDataValue ||
                m_MskValidValue != other.m_MskValidValue;}
    bool operator==( const GenericCompositeFunctor2 &other) const { return !(this != other); }

    TOutput operator()( const std::vector<InputIteratorType> &inputIts) {
        PrecisionType sum = 0;
        int count = 0;

        OutputPixelType outPix;
        for (auto &it : inputIts)
          {
          const auto &inPix = it.Get();
          if ((!m_UseNoDataValue || inPix != m_NoDataValue) && inPix > 0)
            {
            sum += inPix;
            count++;
            }
          ++it;
          }

        if (count > 0)
          {
          outPix = static_cast<OutputPixelType>(sum / count);
          }
          else
          {
          outPix = 0;
          }

    }
    /*
    TOutput operator()( const TInput & inputIts, const TInput2 & msksIts ) const
    {
        TOutput outPix;
        double sum = 0;
        int count = 0;
        int i = 0;
        bool mskValidPixel = true;
        size_t imgsCnt = inputIts.size();
        for (i = 0; i<imgsCnt; i++)
        {
            const auto &inPix = inputIts[i];
            mskValidPixel = (msksIts[i] == m_MskValidValue);
            if (mskValidPixel && (!m_UseNoDataValue || inPix != m_NoDataValue) && inPix > 0)
            {
                sum += inPix;
                count++;
            }
        }

        if (count > 0)
        {
            outPix = static_cast<TOutput>(sum / count);
        }
        else
        {
            outPix = 0;
        }
        return outPix;
    }
*/

private:
    float               m_NoDataValue;
    bool                m_UseNoDataValue;
    float               m_MskValidValue;
};

enum
{
  Interpolator_BCO,
  Interpolator_NNeighbor,
  Interpolator_Linear
};

const float DefaultGridSpacingMeter = 4.0;

namespace Wrapper
{

class Sen4XGenericComposite2 : public Application
{
public:
  /** Standard class typedefs. */
  typedef Sen4XGenericComposite2            Self;
  typedef Application                   Superclass;
  typedef itk::SmartPointer<Self>       Pointer;
  typedef itk::SmartPointer<const Self> ConstPointer;

  /** Standard macro */
  itkNewMacro(Self);

  itkTypeMacro(Sen4XGenericComposite2, otb::Wrapper::Application);

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

  // typedef GenericCompositeFunctor<InputImageType::PixelType, OutputImageType::PixelType>    GenericCompositeFunctorType;
  typedef GenericCompositeFunctor2<InputImageType::PixelType, OutputImageType::PixelType, InputImageType::PixelType>    GenericCompositeFunctorType2;

  // typedef itk::NaryFunctorImageFilter< InputImageType, OutputImageType, GenericCompositeFunctorType> CompositeFilterType;
  // TODO: This should be commented, used only for comparisons
  typedef otb::ImageListCompositeFilter<InputImageType, OutputImageType, GenericCompositeFunctorType2> ImageListCompositeFilterType;
  /** Generic Remote Sensor Resampler */
  typedef otb::GenericRSResampleImageFilter<InputImageType,
                                            InputImageType>             ResampleFilterType;

  /** Interpolators typedefs*/
  typedef itk::LinearInterpolateImageFunction<InputImageType,
                                              double>              LinearInterpolationType;
  typedef itk::NearestNeighborInterpolateImageFunction<InputImageType,
                                                       double>     NearestNeighborInterpolationType;
  typedef otb::BCOInterpolateImageFunction<InputImageType>         BCOInterpolationType;


  typedef itk::UnaryFunctorImageFilter<InputImageType,OutputImageType,
                  GenericCompositeFunctorType2 > UnmaskedCompositeFilterType;

  typedef itk::BinaryFunctorImageFilter<InputImageType,OutputImageType,InputImageType,
                  GenericCompositeFunctorType2 > MaskedCompositeFilterType;

    typedef otb::StreamingSimpleMosaicFilter<InputImageType, InputImageType> SimpleMosaicFilterType;

private:
  void DoInit() override
  {
      SetName("Sen4XGenericComposite2");
      SetDescription("Computes a composite of multiple images");

      SetDocName("Sen4XGenericComposite2");
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
      std::vector<std::string> allImsg = inImages;

      m_Readers = ReaderListType::New();
      m_Images = ImageListType::New();
      m_MskImages = ImageListType::New();
      bool useMasks = false;
      if (HasValue("msks")) {
          const auto &mskImages = GetParameterStringList("msks");
          if (mskImages.size() != inImages.size()) {
              otbAppLogFATAL("The number of mask images should be equal with the ones of the input images (msks = " <<
                             mskImages.size() << ", images = " << inImages.size() << ")!");
          }
          allImsg.insert(allImsg.end(), mskImages.begin(), mskImages.end());
          for (const auto &file : mskImages) {
                m_MskImages->PushBack(GetReader(file)->GetOutput());
          }
          useMasks = true;
      }
      // m_ReprojectedImages = ImageListType::New();
      auto first = true;
      for (const auto &file : inImages)
      {
          const auto inImage = GetReader(file)->GetOutput();

          if (!HasValue("outputs.ulx") || !HasValue("outputs.spacingx") || !HasValue("outputs.sizex") ||
                  !HasValue("outputs.uly") || !HasValue("outputs.spacingy") || !HasValue("outputs.sizey")) {
              m_Images->PushBack(inImage);
          } else {
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
                m_Images->PushBack(resampleFilter->GetOutput());
             }
        }
        first = false;
      }

      m_CompositeFunctor  = GetCompositeFunctor();
      if (HasUserValue("bv"))
      {
        m_CompositeFunctor->UseNoDataValueOn();
      }
      m_CompositeFunctor->SetNoDataValue(GetParameterFloat("bv"));
      // m_CompositeFunctor->SetHasMasksValue(useMasks);
      m_CompositeFunctor->SetMskValidValue(GetParameterFloat("mskvld"));

      // m_CompositeFilter = CompositeFilterType::New();

      //      TODO to compare performances (not compilable yet)
            ImageListCompositeFilterType::Pointer filter1 = ImageListCompositeFilterType::New();
            filter1->SetFunctor(*m_CompositeFunctor);
            filter1->SetInput(m_Images);



//    if (true) {
//        if(useMasks) {
//            m_MaskedFilter = MaskedCompositeFilterType::New();
//            m_MaskedFilter->SetFunctor(*m_CompositeFunctor);
//            m_MaskedFilter->SetInput1(m_Images);
//            m_MaskedFilter->SetInput2(m_msksImg);
//            m_functorOutput = m_MaskedFilter->GetOutput();
//        } else {
//            m_UnmaskedFilter = UnmaskedCompositeFilterType::New();
//            m_MaskedFilter->SetFunctor(*m_CompositeFunctor);
//            m_UnmaskedFilter->SetInput(m_Images);
//            m_functorOutput = m_UnmaskedFilter->GetOutput();
//        }
//        SetParameterOutputImage("out", m_functorOutput);

//    }
//    else {
//      m_CompositeFilter->SetFunctor(*m_CompositeFunctor);
//      int i = 0;
//      for (const auto &img: m_CompositingImages)
//      {
//          m_CompositeFilter->SetInput(i, img);
//          i++;
//      }
//      // Output Image
//      SetParameterOutputImage("out", m_CompositeFilter->GetOutput());
//    }
  }

  GenericCompositeFunctorType2 * GetCompositeFunctor() {
      return new GenericCompositeFunctorType2();
  }

  ReaderType::Pointer GetReader(const std::string &inFile) {
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
  GenericCompositeFunctorType2 *                 m_CompositeFunctor;
  // CompositeFilterType::Pointer                 m_CompositeFilter;
  std::string                                  m_OutputProjectionRef;

  UnmaskedCompositeFilterType::Pointer m_UnmaskedFilter;
  MaskedCompositeFilterType::Pointer m_MaskedFilter;
  OutputImageType::Pointer m_functorOutput;
  ImageListType::Pointer                       m_Images;
  ImageListType::Pointer                       m_MskImages;


  SimpleMosaicFilterType::Pointer       m_SimpleMosaicFilter;
};
}
}

OTB_APPLICATION_EXPORT(otb::Wrapper::Sen4XGenericComposite2)

