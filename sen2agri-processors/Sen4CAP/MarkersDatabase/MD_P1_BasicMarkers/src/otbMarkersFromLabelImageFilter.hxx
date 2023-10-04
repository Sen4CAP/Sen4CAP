/*
 * Copyright (C) 1999-2011 Insight Software Consortium
 * Copyright (C) 2005-2017 Centre National d'Etudes Spatiales (CNES)
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

#ifndef otbMarkersFromLabelImageFilter_hxx
#define otbMarkersFromLabelImageFilter_hxx
#include "otbMarkersFromLabelImageFilter.h"

#include "itkInputDataObjectIterator.h"
#include "itkImageRegionIterator.h"
#include "itkProgressReporter.h"
#include "otbMacro.h"
#include <cmath>

namespace otb
{

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::PersistentMarkersFromLabelImageFilter()
{
  // first output is a copy of the image, DataObject created by
  // superclass
  //
  // allocate the data objects for the outputs which are
  // just decorators around pixel types
  typename PixelValueMapObjectType::Pointer output
      = static_cast<PixelValueMapObjectType*>(this->MakeOutput(1).GetPointer());
  this->itk::ProcessObject::SetNthOutput(1, output.GetPointer());

  this->Reset();

}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
typename itk::DataObject::Pointer
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::MakeOutput(DataObjectPointerArraySizeType itkNotUsed(output))
{
  return static_cast<itk::DataObject*>(PixelValueMapObjectType::New().GetPointer());
}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
void
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::SetInputLabelImage(const LabelImageType *input)
{
  // Process object is not const-correct so the const_cast is required here
  this->itk::ProcessObject::SetNthInput(1,
                                   const_cast< LabelImageType * >( input ) );

}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
const typename PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>::LabelImageType*
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::GetInputLabelImage()
{
  return static_cast< const TLabelImage * >
    (this->itk::ProcessObject::GetInput(1));
}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
void
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::SetMaskInputImage(const MaskImageType *input)
{
  // Process object is not const-correct so the const_cast is required here
  this->itk::ProcessObject::SetNthInput(2,
                                   const_cast< MaskImageType * >( input ) );

}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
const typename PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>::MaskImageType*
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::GetMaskInputImage()
{
    if (this->GetNumberOfInputs()<3)
      {
      return 0;
      }

    return static_cast< const TMaskImage * >(this->itk::ProcessObject::GetInput(2));
}


template<class TInputVectorImage, class TLabelImage, class TMaskImage>
typename PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>::PixeMeanStdDevlValueMapType
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::GetMeanStdDevValueMap() const
{
  return m_MeanStdDevRadiometricValue;
}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
typename PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>::PixelValueMapType
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::GetMinValueMap() const
{
  return m_MinRadiometricValue;
}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
typename PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>::PixelValueMapType
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::GetMaxValueMap() const
{
  return m_MaxRadiometricValue;
}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
typename PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>::PixelValueMapType
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::GetValidPixelsCntMap() const
{
  return m_ValidPixelsCnt;
}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
typename PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>::PixelValueMapType
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::GetInvalidPixelsCntMap() const
{
  return m_InvalidPixelsCnt;
}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
typename PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>::PixelValueMapType
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::GetMedianValuesMap() const
{
  return m_MedianValue;
}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
typename PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>::PixelValueMapType
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::GetP25ValuesMap() const
{
  return m_P25Value;
}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
typename PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>::PixelValueMapType
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::GetP75ValuesMap() const
{
  return m_P75Value;
}


template<class TInputVectorImage, class TLabelImage, class TMaskImage>
void
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::GenerateOutputInformation()
{
  Superclass::GenerateOutputInformation();

  if (this->GetInput())
    {
    this->GetOutput()->CopyInformation(this->GetInput());
    this->GetOutput()->SetLargestPossibleRegion(this->GetInput()->GetLargestPossibleRegion());

    if (this->GetOutput()->GetRequestedRegion().GetNumberOfPixels() == 0)
      {
      this->GetOutput()->SetRequestedRegion(this->GetOutput()->GetLargestPossibleRegion());
      }
    }
}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
void
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::AllocateOutputs()
{
  // This is commented to prevent the streaming of the whole image for the first stream strip
  // It shall not cause any problem because the output image of this filter is not intended to be used.
  //InputImagePointer image = const_cast< TInputImage * >( this->GetInput() );
  //this->GraftOutput( image );
  // Nothing that needs to be allocated for the remaining outputs
}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
void
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::Synthetize()
 {
  // Update temporary accumulator
  AccumulatorMapType outputAcc;
  auto endAcc = outputAcc.end();

  for (auto const& threadAccMap: m_AccumulatorMaps)
    {
    for(auto const& it: threadAccMap)
      {
      auto label = it.first;
      auto itAcc = outputAcc.find(label);
      if (itAcc == endAcc)
        {
        outputAcc.emplace(label, it.second);
        }
      else
        {
        itAcc->second.Update(it.second);
        }
      }
    }

  // Publish output maps
  bool noBandOk;
  for(auto& it: outputAcc)
    {
    const LabelPixelType label = it.first;
    const auto &count = it.second.GetCount();
    const auto &sum       = it.second.GetSum();
    const auto &sqSum     = it.second.GetSqSum();
    const auto &countInvalid = it.second.GetCountInvalid();


    // Mean & stdev
    RealVectorPixelType mean (sum);
    RealVectorPixelType std (sqSum);
    noBandOk = true;
    for (unsigned int band = 0 ; band < mean.GetSize() ; band++)
      {
      // Number of valid pixels in band
      auto bandCnt = count[band];
      if ((bandCnt > 1) && (bandCnt > 0.1 * (bandCnt + countInvalid[band]))) {
          noBandOk = false;
         // Mean
         mean[band] /= bandCnt;

         // Unbiased standard deviation (not sure unbiased is usefull here)
         // Compute sample standard deviation
         const double variance = (sqSum[band] - (sum[band] * mean[band])) / (bandCnt - 1);
         // Compute population standard deviation
         // const double variance = (sqSum[band] - (sum[band] * mean[band])) / (count[band]);
         std[band] = vcl_sqrt(variance);
      }
    }
    if (!noBandOk)
    {
        m_MeanStdDevRadiometricValue[label].mean = mean;
        m_MeanStdDevRadiometricValue[label].stdDev = std;

        // Min & max
        if (m_ComputeMinMax) {
            m_MinRadiometricValue[label] = it.second.GetMin();
            m_MaxRadiometricValue[label] = it.second.GetMax();
        }
        if (m_ComputeValidPixelsCnt) {
           m_ValidPixelsCnt[label] = count;
        }
        if (m_ComputeInvalidPixelsCnt) {
           m_InvalidPixelsCnt[label] = countInvalid;
        }
        // median, P25 and P75
        if (m_ComputeMedian || m_ComputeP25 || m_ComputeP75) {
            // Fill first with the minimum pixel
            if (m_ComputeMedian) {
               m_MedianValue[label] = it.second.GetMin();
            }
            if (m_ComputeP25) {
               m_P25Value[label] = it.second.GetMin();
            }
            if (m_ComputeP75) {
               m_P75Value[label] = it.second.GetMin();
            }
            // Then, update for each band the value of the corresponding marker
            for (unsigned int band = 0 ; band < count.GetSize() ; band++) {
                const std::vector<double> &values = it.second.GetBandSortedValues(band);
                if (m_ComputeMedian) {
                   m_MedianValue[label][band] = it.second.ComputeQuartile(values, 50);
                }
                if (m_ComputeP25) {
                   m_P25Value[label][band] = it.second.ComputeQuartile(values, 25);
                }
                if (m_ComputeP75) {
                   m_P75Value[label][band] = it.second.ComputeQuartile(values, 75);
                }
            }
        }
      }
    }
    m_AccumulatorMaps.clear();
 }

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
void
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::Reset()
{
    m_MeanStdDevRadiometricValue.clear();
    m_MinRadiometricValue.clear();
    m_MaxRadiometricValue.clear();
    m_ValidPixelsCnt.clear();
    m_InvalidPixelsCnt.clear();
    m_MedianValue.clear();
    m_P25Value.clear();
    m_P75Value.clear();

    m_AccumulatorMaps.clear();
    m_AccumulatorMaps.resize(this->GetNumberOfThreads());
}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
void
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::GenerateInputRequestedRegion()
{
  // The Requested Regions of all the inputs are set to their Largest Possible Regions
  this->itk::ProcessObject::GenerateInputRequestedRegion();

  // Iteration over all the inputs of the current filter (this)
  for( itk::InputDataObjectIterator it( this ); !it.IsAtEnd(); it++ )
    {
    // Check whether the input is an image of the appropriate dimension
    // dynamic_cast of all the input images as itk::ImageBase objects
    // in order to pass the if ( input ) test whatever the inputImageType (vectorImage or labelImage)
    ImageBaseType * input = dynamic_cast< ImageBaseType *>( it.GetInput() );

    if ( input )
      {
      // Use the function object RegionCopier to copy the output region
      // to the input.  The default region copier has default implementations
      // to handle the cases where the input and output are the same
      // dimension, the input a higher dimension than the output, and the
      // input a lower dimension than the output.
      InputImageRegionType inputRegion;
      this->CallCopyOutputRegionToInputRegion( inputRegion, this->GetOutput()->GetRequestedRegion() );
      input->SetRequestedRegion(inputRegion);
      }
    }
}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
void
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::ThreadedGenerateData(const RegionType& outputRegionForThread, itk::ThreadIdType threadId )
{
  /**
   * Grab the input
   */
  InputVectorImagePointer inputPtr =  const_cast<TInputVectorImage *>(this->GetInput());
  LabelImagePointer labelInputPtr =  const_cast<TLabelImage *>(this->GetInputLabelImage());
  MaskImagePointer maskInputPtr =  const_cast<TMaskImage *>(this->GetMaskInputImage());

  // TODO: Add here the Mask Image

  itk::ImageRegionConstIterator<TInputVectorImage> inIt(inputPtr, outputRegionForThread);
  itk::ImageRegionConstIterator<TLabelImage> labelIt(labelInputPtr, outputRegionForThread);
  itk::ImageRegionConstIterator<TMaskImage> maskIt;
  if (maskInputPtr) {
      itk::ImageRegionConstIterator<TMaskImage> validIt(maskInputPtr, outputRegionForThread);
      maskIt = validIt;
      maskIt.GoToBegin();
  }

  itk::ProgressReporter progress(this, threadId, outputRegionForThread.GetNumberOfPixels());

  auto &acc = m_AccumulatorMaps[threadId];
  auto endAcc = acc.end();
  bool isValidVal;

  // do the work
  for (inIt.GoToBegin(), labelIt.GoToBegin();
       !inIt.IsAtEnd() && !labelIt.IsAtEnd();
       ++inIt, ++labelIt)
    {
      isValidVal = true;
      const auto &value = inIt.Get();
      auto label = labelIt.Get();
      // TODO: TESTING ONLY - TO BE REMOVED
      // ========================================
//      if (label == 34 && value[0] != NO_DATA) {
//          std::cout << value[0] << std::endl;
//      }
      // ========================================

      if (maskInputPtr)
      {

          if (maskIt.IsAtEnd()) {
              break;
          }
          if (m_ValidMaskValue != maskIt.Value()) {
              isValidVal = false;
          }
          ++maskIt;
      }
      if (label == 0) {
          // ignore background label
          // TODO: We should make the value configurable
          continue;
      }
      // Update the accumulator
      auto itAcc = acc.find(label);
      if (itAcc == endAcc)
        {
        acc.emplace(label, AccumulatorType(value, isValidVal));
        }
      else
        {
        itAcc->second.Update(value, isValidVal);
        }

      progress.CompletedPixel();
    }
}

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
void
PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage>
::PrintSelf(std::ostream& os, itk::Indent indent) const
{
  Superclass::PrintSelf(os, indent);
}

} // end namespace otb
#endif
