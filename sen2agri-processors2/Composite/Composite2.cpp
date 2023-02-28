/*=========================================================================
  *
  * Program:      Sen2agri-Processors
  * Language:     C++
  * Copyright:    2015-2016, CS Romania, office@c-s.ro
  * See COPYRIGHT file for details.
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.

 =========================================================================*/

#include "otbWrapperApplication.h"
#include "otbWrapperApplicationFactory.h"
#include "otbWrapperInputImageListParameter.h"

#include "otbImageList.h"
#include "otbImageListToImageFilter.h"
#include "otbImageToVectorImageCastFilter.h"
#include "otbObjectList.h"
#include "otbVectorImage.h"
#include "otbWrapperTypes.h"

#include "otbFunctorImageFilter.h"

namespace otb
{

template <class TInputImageType, class TOutputImageType>
class ITK_EXPORT CompositeFilter : public ImageListToImageFilter<TInputImageType, TInputImageType>
{
public:
    /** Standard typedefs */
    typedef CompositeFilter Self;
    typedef ImageListToImageFilter<TInputImageType, TInputImageType> Superclass;
    typedef itk::SmartPointer<Self> Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    typedef TInputImageType InputImageType;
    typedef typename InputImageType::Pointer InputImagePointerType;
    typedef typename InputImageType::PixelType InputPixelType;
    typedef ImageList<InputImageType> InputImageListType;
    typedef TOutputImageType OutputImageType;
    typedef typename TOutputImageType::PixelType OutputPixelType;
    typedef typename OutputImageType::Pointer OutputImagePointerType;
    typedef float PrecisionType;

    typedef typename OutputImageType::RegionType OutputImageRegionType;
    typedef typename InputImageType::RegionType InputImageRegionType;

    itkNewMacro(Self);

    itkTypeMacro(CompositeFilter, ImageListToImageFilter);

    itkGetMacro(NoDataValue, InputPixelType);
    itkSetMacro(NoDataValue, InputPixelType);
    itkGetMacro(UseNoDataValue, bool);
    itkSetMacro(UseNoDataValue, bool);
    itkBooleanMacro(UseNoDataValue);

protected:
    CompositeFilter();
    ~CompositeFilter() override
    {
    }

    void GenerateInputRequestedRegion() override;
    void GenerateOutputInformation() override;
    void ThreadedGenerateData(const OutputImageRegionType &outputRegionForThread,
                              itk::ThreadIdType threadId) override;

private:
    CompositeFilter(const Self &) = delete;
    void operator=(const Self &) = delete;

    InputPixelType m_NoDataValue;
    bool m_UseNoDataValue;
};

template <class TInputImageType, class TOutputImageType>
CompositeFilter<TInputImageType, TOutputImageType>::CompositeFilter()
    : m_NoDataValue(), m_UseNoDataValue()
{
    this->SetNumberOfRequiredInputs(1);
    this->SetNumberOfRequiredOutputs(1);
}

template <class TInputImageType, class TOutputImageType>
void CompositeFilter<TInputImageType, TOutputImageType>::GenerateInputRequestedRegion(void)
{
    auto inputPtr = this->GetInput();
    for (auto inputListIt = inputPtr->Begin(); inputListIt != inputPtr->End(); ++inputListIt) {
        inputListIt.Get()->SetRequestedRegion(this->GetOutput()->GetRequestedRegion());
    }
}

template <class TInputImageType, class TOutputImageType>
void CompositeFilter<TInputImageType, TOutputImageType>::GenerateOutputInformation()
{
    if (this->GetOutput()) {
        if (this->GetInput()->Size() > 0) {
            this->GetOutput()->CopyInformation(this->GetInput()->GetNthElement(0));
            this->GetOutput()->SetLargestPossibleRegion(
                this->GetInput()->GetNthElement(0)->GetLargestPossibleRegion());
            this->GetOutput()->SetNumberOfComponentsPerPixel(1);
        }
    }
}

template <class TInputImageType, class TOutputImageType>
void CompositeFilter<TInputImageType, TOutputImageType>::ThreadedGenerateData(
    const OutputImageRegionType &outputRegionForThread, itk::ThreadIdType threadId)
{
    auto inputPtr = this->GetInput();
    auto inputImages = this->GetInput()->Size();

    auto outputPtr = this->GetOutput();

    typedef itk::ImageRegionConstIteratorWithIndex<InputImageType> InputIteratorType;
    typedef itk::ImageRegionIteratorWithIndex<OutputImageType> OutputIteratorType;

    itk::ProgressReporter progress(this, threadId, outputRegionForThread.GetNumberOfPixels());

    OutputIteratorType outputIt(outputPtr, outputRegionForThread);
    outputIt.GoToBegin();

    std::vector<InputIteratorType> inputIts;
    inputIts.reserve(inputImages);
    for (auto inputListIt = inputPtr->Begin(); inputListIt != inputPtr->End(); ++inputListIt) {
        inputIts.emplace_back(inputListIt.Get(), outputRegionForThread);
        inputIts.back().GoToBegin();
    }

    InputPixelType zero = m_UseNoDataValue ? m_NoDataValue : 0;

    OutputPixelType outPix;
    PrecisionType sum;
    int count;

    while (!outputIt.IsAtEnd()) {
        sum = 0;
        count = 0;

        for (auto &it : inputIts) {
            const auto &inPix = it.Get();
            if ((!m_UseNoDataValue || inPix != m_NoDataValue) && !std::isnan(inPix)) {
                sum += inPix;
                count++;
            }
            ++it;
        }

        if (count > 0) {
            outputIt.Set(static_cast<OutputPixelType>(sum / count));
        } else {
            outputIt.Set(0);
        }

        ++outputIt;
        progress.CompletedPixel();
    }
}

namespace Wrapper
{
class Composite2 : public Application
{
public:
    typedef Composite2 Self;
    typedef Application Superclass;
    typedef itk::SmartPointer<Self> Pointer;
    typedef itk::SmartPointer<const Self> ConstPointer;

    itkNewMacro(Self);
    itkTypeMacro(Composite, otb::Application);

    typedef FloatImageType InputImageType;
    typedef FloatImageType OutputImageType;
    typedef InputImageType::PixelType InputPixelType;
    typedef OutputImageType::PixelType OutputPixelType;
    typedef otb::ImageList<InputImageType> ImageListType;
    typedef otb::ImageFileReader<InputImageType> ReaderType;
    typedef otb::ObjectList<ReaderType> ReaderListType;
    typedef otb::CompositeFilter<InputImageType, OutputImageType> CompositeFilterType;

private:
    void DoInit() override
    {
        SetName("Composite2");
        SetDescription("Computes a composite of multiple images");

        SetDocLongDescription("Computes a mean composite of multiple images.");
        SetDocLimitations("None");
        SetDocAuthors("LN");
        SetDocSeeAlso(" ");

        AddDocTag(Tags::Raster);

        AddParameter(ParameterType_InputFilenameList, "il", "Input images");
        SetParameterDescription("il", "The list of input images");

        AddParameter(ParameterType_Float, "bv", "Background value");
        SetParameterDescription("bv", "Background value to ignore in computation.");
        SetDefaultParameterFloat("bv", 0.);
        MandatoryOff("bv");

        AddParameter(ParameterType_OutputImage, "out", "Output image");
        SetParameterDescription("out", "Output image.");

        AddRAMParameter();

        SetDocExampleParameterValue("il", "image1.tif image2.tif");
        SetDocExampleParameterValue("out", "output.tif");
    }

    void DoUpdateParameters() override
    {
    }

    void DoExecute() override
    {
        const auto &inImages = GetParameterStringList("il");

        auto images = ImageListType::New();
        auto readers = ReaderListType::New();

        for (const auto &file : inImages) {
            auto reader = ReaderType::New();
            reader->SetFileName(file);
            readers->PushBack(reader);

            images->PushBack(reader->GetOutput());
        }

        auto compositeFilter = CompositeFilterType::New();
        compositeFilter->SetInput(images);

        if (HasValue("bv")) {
            compositeFilter->UseNoDataValueOn();
            compositeFilter->SetNoDataValue(GetParameterFloat("bv"));
        }

        SetParameterOutputImage("out", compositeFilter->GetOutput());

        RegisterPipeline();
    }
};
} // namespace Wrapper
} // namespace otb

OTB_APPLICATION_EXPORT(otb::Wrapper::Composite2)
