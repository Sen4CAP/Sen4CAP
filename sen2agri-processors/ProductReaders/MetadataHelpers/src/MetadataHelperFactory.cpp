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

#include "MetadataHelperFactory.h"
#include "Spot4MetadataHelper.h"
#include "MAJAMetadataHelper.h"
#include "MAJAL8MetadataHelper.h"
#include "MACCSS2MetadataHelper.h"
#include "MACCSL8MetadataHelper.h"
#include "SEN2CORMetadataHelper.h"
#include "MaskedL2AMetadataHelper.h"

template <typename PixelType, typename MasksPixelType>
std::unique_ptr<MetadataHelper<PixelType, MasksPixelType>> METADATA_HELPER_FACTORY_EXPORT MetadataHelperFactory::GetMetadataHelper(const std::string& metadataFilePath,
                                                                                                                                   const std::string &externalMask)
{
    // std::cout << "Getting metadata helper" << std::endl;

    std::unique_ptr<MetadataHelper<PixelType, MasksPixelType>> maskedL2AMetadataHelper(new MaskedL2AMetadataHelper<PixelType, MasksPixelType>);
    if (maskedL2AMetadataHelper->LoadMetadataFile(metadataFilePath, externalMask))
        return maskedL2AMetadataHelper;

    std::unique_ptr<MetadataHelper<PixelType, MasksPixelType>> majaMetadataHelper(new MAJAMetadataHelper<PixelType, MasksPixelType>);
    if (majaMetadataHelper->LoadMetadataFile(metadataFilePath, externalMask))
        return majaMetadataHelper;

    std::unique_ptr<MetadataHelper<PixelType, MasksPixelType>> majaL8MetadataHelper(new MAJAL8MetadataHelper<PixelType, MasksPixelType>);
    if (majaL8MetadataHelper->LoadMetadataFile(metadataFilePath, externalMask))
        return majaL8MetadataHelper;

    std::unique_ptr<MetadataHelper<PixelType, MasksPixelType>> maccsS2MetadataHelper(new MACCSS2MetadataHelper<PixelType, MasksPixelType>);
    if (maccsS2MetadataHelper->LoadMetadataFile(metadataFilePath, externalMask))
        return maccsS2MetadataHelper;

    std::unique_ptr<MetadataHelper<PixelType, MasksPixelType>> maccsL8MetadataHelper(new MACCSL8MetadataHelper<PixelType, MasksPixelType>);
    if (maccsL8MetadataHelper->LoadMetadataFile(metadataFilePath, externalMask))
        return maccsL8MetadataHelper;

    std::unique_ptr<MetadataHelper<PixelType, MasksPixelType>> sen2corMetadataHelper(new SEN2CORMetadataHelper<PixelType, MasksPixelType>);
    if (sen2corMetadataHelper->LoadMetadataFile(metadataFilePath, externalMask))
        return sen2corMetadataHelper;

    std::unique_ptr<MetadataHelper<PixelType, MasksPixelType>> spot4MetadataHelper(new Spot4MetadataHelper<PixelType, MasksPixelType>);
    if (spot4MetadataHelper->LoadMetadataFile(metadataFilePath, externalMask))
        return spot4MetadataHelper;

    itkExceptionMacro("Unable to read metadata from " << metadataFilePath);

    return NULL;
}

template
std::unique_ptr<MetadataHelper<short, short>> MetadataHelperFactory::GetMetadataHelper(const std::string& metadataFileName, const std::string &externalMask);

template
std::unique_ptr<MetadataHelper<unsigned short, short>> MetadataHelperFactory::GetMetadataHelper(const std::string& metadataFileName, const std::string &externalMask);

template
std::unique_ptr<MetadataHelper<short, uint8_t>> MetadataHelperFactory::GetMetadataHelper(const std::string& metadataFileName, const std::string &externalMask);

template
std::unique_ptr<MetadataHelper<unsigned short, uint8_t>> MetadataHelperFactory::GetMetadataHelper(const std::string& metadataFileName, const std::string &externalMask);

template
std::unique_ptr<MetadataHelper<float, short>> MetadataHelperFactory::GetMetadataHelper(const std::string& metadataFileName, const std::string &externalMask);

template
std::unique_ptr<MetadataHelper<float, uint8_t>> MetadataHelperFactory::GetMetadataHelper(const std::string& metadataFileName, const std::string &externalMask);


template
std::unique_ptr<MetadataHelper<int, short>> MetadataHelperFactory::GetMetadataHelper(const std::string& metadataFileName, const std::string &externalMask);

template
std::unique_ptr<MetadataHelper<unsigned int, short>> MetadataHelperFactory::GetMetadataHelper(const std::string& metadataFileName, const std::string &externalMask);

template
std::unique_ptr<MetadataHelper<int, uint8_t>> MetadataHelperFactory::GetMetadataHelper(const std::string& metadataFileName, const std::string &externalMask);

template
std::unique_ptr<MetadataHelper<unsigned int, uint8_t>> MetadataHelperFactory::GetMetadataHelper(const std::string& metadataFileName, const std::string &externalMask);
