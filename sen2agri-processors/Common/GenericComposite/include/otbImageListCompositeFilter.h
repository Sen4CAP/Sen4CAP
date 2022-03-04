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

#ifndef otbImageListCompositeFilter_h
#define otbImageListCompositeFilter_h

#include "otbImageListToImageFilter.h"

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

} // End namespace otb

#endif
