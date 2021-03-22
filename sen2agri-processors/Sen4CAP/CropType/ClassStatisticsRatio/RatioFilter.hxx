#include <itkImageToImageFilter.h>

namespace otb
{

template <class TInputImageType, class TOutputImageType>
class ITK_EXPORT RatioFilter
  : public itk::ImageToImageFilter<TInputImageType, TOutputImageType>
{
public:
  /** Standard typedefs */
  typedef RatioFilter                               Self;
  typedef itk::ImageToImageFilter
      <TInputImageType, TOutputImageType>           Superclass;
  typedef itk::SmartPointer<Self>                   Pointer;
  typedef itk::SmartPointer<const Self>             ConstPointer;

  typedef TInputImageType                               InputImageType;
  typedef typename InputImageType::Pointer              InputImagePointerType;
  typedef typename InputImageType::PixelType            InputPixelType;
  typedef TOutputImageType                              OutputImageType;
  typedef typename OutputImageType::PixelType           OutputPixelType;
  typedef typename OutputImageType::InternalPixelType   OutputInternalPixelType;
  typedef typename OutputImageType::Pointer             OutputImagePointerType;
  typedef double                                        PrecisionType;

  typedef typename OutputImageType::RegionType OutputImageRegionType;
  typedef typename InputImageType::RegionType  InputImageRegionType;


  /** Type macro */
  itkNewMacro(Self);

  /** Creation through object factory macro */
  itkTypeMacro(RatioFilter, ImageToImageFilter);

  itkGetMacro(NoDataValue, InputPixelType);
  itkSetMacro(NoDataValue, InputPixelType);
  itkGetMacro(UseNoDataValue, bool);
  itkSetMacro(UseNoDataValue, bool);
  itkBooleanMacro(UseNoDataValue);

protected:
  RatioFilter();
  ~RatioFilter() override {}

  void GenerateInputRequestedRegion() override;
  void GenerateOutputInformation() override;
  void ThreadedGenerateData(const OutputImageRegionType &outputRegionForThread, itk::ThreadIdType threadId) override;

  /**PrintSelf method */
  void PrintSelf(std::ostream& os, itk::Indent indent) const override;


private:
  RatioFilter(const Self &) = delete;
  void operator =(const Self&) = delete;

  InputPixelType       m_NoDataValue;
  bool                 m_UseNoDataValue;
};

template <class TInputImageType, class TOutputImageType>
RatioFilter<TInputImageType, TOutputImageType>
::RatioFilter()
 : m_NoDataValue(),
   m_UseNoDataValue()
{
  this->SetNumberOfRequiredInputs(1);
  this->SetNumberOfRequiredOutputs(1);
}

template <class TInputImageType, class TOutputImageType>
void
RatioFilter<TInputImageType, TOutputImageType>
::GenerateInputRequestedRegion(void)
{
  auto inputPtr = this->GetInput();
  for (auto inputListIt = inputPtr->Begin(); inputListIt != inputPtr->End(); ++inputListIt)
    {
    inputListIt.Get()->SetRequestedRegion(this->GetOutput()->GetRequestedRegion());
    }
}

template <class TInputImageType, class TOutputImageType>
void
RatioFilter<TInputImageType, TOutputImageType>
::GenerateOutputInformation()
{
  if (this->GetOutput())
    {
    if (this->GetInput()->Size() > 0)
      {
      this->GetOutput()->CopyInformation(this->GetInput()->GetNthElement(0));
      this->GetOutput()->SetLargestPossibleRegion(this->GetInput()->GetNthElement(0)->GetLargestPossibleRegion());
      this->GetOutput()->SetNumberOfComponentsPerPixel(this->GetInput()->Size() / 2);
      }
    }
}

template <class TInputImageType, class TOutputImageType>
void
RatioFilter<TInputImageType, TOutputImageType>
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

  outPix.SetSize(inputImages / 2);
  while (!outputIt.IsAtEnd())
    {
      size_t k = 0;
      for (int i = 0; i < inputImages; i += 2, k++) {
          auto &itVV = inputIts[i];
          auto &itVH = inputIts[i + 1];

          auto inPixVV = itVV.Get();
          auto inPixVH = itVH.Get();

          if ((!m_UseNoDataValue || inPixVV != m_NoDataValue) && /* !std::isnan(inPixVV) && */
              (!m_UseNoDataValue || inPixVH != m_NoDataValue) && /* !std::isnan(inPixVH) && */
              inPixVH > 0 && inPixVV > 0) {
              outPix[k] = inPixVV / inPixVH;
          } else {
              outPix[k] = zero;
          }

          ++itVV;
          ++itVH;
      }

    outputIt.Set(outPix);
    ++outputIt;
    progress.CompletedPixel();
    }
}

/**
 * PrintSelf Method
 */
template <class TInputImageType, class TOutputImageType>
void
RatioFilter<TInputImageType, TOutputImageType>
::PrintSelf(std::ostream& os, itk::Indent indent) const
{
  Superclass::PrintSelf(os, indent);
}

}
