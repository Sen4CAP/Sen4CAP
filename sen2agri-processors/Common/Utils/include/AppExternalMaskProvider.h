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
 
#ifndef APP_EXTERNAL_MASK_PROVIDER_H
#define APP_EXTERNAL_MASK_PROVIDER_H

#include "itkLogger.h"
#include "itkObject.h"
#include "otbWrapperMacros.h"
#include "otbImage.h"
#include "otbImageFileReader.h"
#include "itkUnaryFunctorImageFilter.h"
#include <boost/filesystem.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "GlobalDefs.h"

namespace otb
{

namespace Wrapper
{
//#define APP_EXTERNAL_MASK_PROVIDER_DO_INIT_APP_PARAMS()
//    AddParameter(ParameterType_String,  "extmask",   "Input additional external mask.");
//    MandatoryOff("extmask");

//#define APP_EXTERNAL_MASK_PROVIDER_LIST_CREATE(T)
//    std::vector<AppExternalMaskProvider<T>>::Pointer> m_appExtMskProviders;

//#define APP_EXTERNAL_MASK_PROVIDER_CREATE(T)
//    AppExternalMaskProvider<T>::Pointer appExtMskProvider = AppExternalMaskProvider<T>::New();
//    m_appExtMskProviders.push_back(appExtMskProvider);

//#define APP_EXTERNAL_MASK_PROVIDER_UPDATE_FROM_APP_PARAMS()
//    appExtMskProvider->SetLogger(this->GetLogger());
//    const std::vector<std::string> &allParams = GetParametersKeys();
//    if (std::find(allParams.begin(), allParams.end(), "extmask") != allParams.end()) {
//        appExtMskProvider->SetDefaultExtMaskLocation(GetParameterString("extmask"));
//    }

//#define APP_EXTERNAL_MASK_PROVIDER_GET_MASK(...) appExtMskProvider->GetExternalMask(__VA_ARGS__);


template< class MskPixelType>
class AppExternalMaskProvider : public itk::Object
{
public:
    /** Standard class typedefs. */
    typedef AppExternalMaskProvider    Self;
    typedef Object                     Superclass;
    typedef itk::SmartPointer<Self>             Pointer;
    typedef itk::SmartPointer<const Self>       ConstPointer;
    typedef itk::WeakPointer<const Self>        ConstWeakPointer;

    /** Method for creation through the object factory. */
    itkNewMacro(Self);

    /** Run-time type information (and related methods). */
    itkTypeMacro(Image, itk::Image);

    typedef otb::Image<MskPixelType, 2> MaskImageType;
    typedef otb::ImageFileReader<MaskImageType>             ExternaMaskImageReaderType;

    template< class TInput, class TOutput>
    class MaskImagesTranslateFunctor
    {
    public:
        MaskImagesTranslateFunctor() {
            m_bBinarize = false;
        }
        ~MaskImagesTranslateFunctor() {}
        void SetBinarizeMask(bool binarize)
        {
            m_bBinarize = binarize;
        }
        bool operator!=( const MaskImagesTranslateFunctor &) const
        {
          return false;
        }
        bool operator==( const MaskImagesTranslateFunctor & other ) const
        {
          return !(*this != other);
        }

        inline TOutput operator()( const TInput & A) const
        {
            if (m_bBinarize) {
                return (A != IMG_FLG_LAND);
            }
            return A;
        }
    private:
        bool m_bBinarize;
    };

    typedef itk::UnaryFunctorImageFilter<MaskImageType,MaskImageType,
                    MaskImagesTranslateFunctor<
                        typename MaskImageType::PixelType,
                        typename MaskImageType::PixelType> > MaskImagesTranslateFilterType;


    const char * GetNameOfClass() { return "AppExternalMaskProvider"; }
    itk::Logger* GetLogger() { return m_pLogger; }
    void SetLogger(itk::Logger* logger) { m_pLogger = logger; }

    AppExternalMaskProvider() :
        m_pLogger(itk::Logger::New())
    {
        m_pLogger->SetName("Application.logger");
        m_pLogger->SetPriorityLevel(itk::LoggerBase::DEBUG);
        m_pLogger->SetLevelForFlushing(itk::LoggerBase::CRITICAL);
    }

    void SetDefaultExtMaskLocation(const std::string &extMaskLocation) {
        m_extMaskLocation = extMaskLocation;
    }

    typename MaskImageType::Pointer GetExternalMask(int preferedRes, bool binarizeMask = false, const std::string &extMaskLocation="") {
        const std::string &extMskLoc = extMaskLocation.size() > 0 ? extMaskLocation : m_extMaskLocation;
        const std::string &extMskPath = GetExternalMaskFile(extMskLoc, preferedRes);
        typename MaskImageType::Pointer retMask;
        if (extMskPath.size() > 0) {
            m_reader = ExternaMaskImageReaderType::New();
            // set the file name
            m_reader->SetFileName(extMskPath);
            m_reader->UpdateOutputInformation();
            typename MaskImageType::Pointer extMskImg = m_reader->GetOutput();
            // MaskImageType::Pointer cutImg = CutImage(extMskImg);
            if (binarizeMask) {
                m_translateFilter = MaskImagesTranslateFilterType::New();
                m_translateFilter->GetFunctor().SetBinarizeMask(true);
                m_translateFilter->SetInput(extMskImg);
                extMskImg = m_translateFilter->GetOutput();
                extMskImg->UpdateOutputInformation();
            }
            retMask = extMskImg;
        }
        return retMask;
    }

    std::string GetExternalMaskFile(const std::string &extMaskLocation, int preferedRes) {
        // If an external mask was provided, then use this one too
        if (extMaskLocation.size() > 0) {
            if (boost::filesystem::is_regular_file(extMaskLocation)) {
                otbAppLogINFO("Using as mask from extmask parameter the file " << extMaskLocation);
                return extMaskLocation;
            }
            if (boost::filesystem::is_directory(extMaskLocation)) {
                // get the tiff files in the directory that could match our preffered resolution
                boost::filesystem::directory_iterator end_itr;

                std::vector<std::string> tifFiles;
                boost::filesystem::path dirPath(extMaskLocation);
                // cycle through the directory
                for (boost::filesystem::directory_iterator itr(dirPath); itr != end_itr; ++itr) {
                    if (boost::filesystem::is_regular_file(itr->path())) {
                        boost::filesystem::path pathObj = itr->path();
                        std::string ext = pathObj.extension().string();
                        if (!boost::iequals(ext, ".tif")) { //accept only tif files for masks
                            continue;
                        }
                        if (preferedRes == -1) {
                            // just return the first file
                            tifFiles.push_back(pathObj.string());
                        } else {
                            std::string curFileName = pathObj.filename().string();
                            const std::string &filter = std::to_string(preferedRes) + "m.tif";
                            if (boost::ifind_first(curFileName, filter)) {
                                otbAppLogINFO("Determined, for resolution " << preferedRes << ", the mask file as being the file " << pathObj.string());
                                return  pathObj.string();
                            }
                        }
                    }
                }
                if(preferedRes == -1 && tifFiles.size() > 0) {
                    std::sort(tifFiles.begin(), tifFiles.end());
                    otbAppLogINFO("Determined, for default resolution, the mask file as being the file " << tifFiles.at(0));
                    return tifFiles.at(0);
                }
            } else {
                itkExceptionMacro("The parameter extmask is expected to be a file or a directory but was provided " << extMaskLocation);
            }
        }
        otbAppLogINFO("No external mask was extracted/determined for the resolution " << preferedRes);
        return "";
    }


private:
    itk::Logger::Pointer m_pLogger;
    std::string m_extMaskLocation;

    typename ExternaMaskImageReaderType::Pointer m_reader;
    typename MaskImagesTranslateFilterType::Pointer m_translateFilter;
};

}
}

#endif // APP_EXTERNAL_MASK_PROVIDER_H

