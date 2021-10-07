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

#include "include/otbVegetationIndicesFunctor.h"
#include "include/otbWaterIndicesFunctor.h"
// #include "otbBuiltUpIndicesFunctor.h"
#include "include/otbSoilIndicesFunctor.h"
#include "include/otbRadiometricIndex.h"
#include "include/otbIndicesStackFunctor.h"
#include "MetadataHelperFactory.h"
#include "itkBinaryFunctorImageFilter.h"

namespace otb
{
namespace Wrapper
{

class Sen4XRadiometricIndices : public Application
{
public:
  /** Standard class typedefs. */
  typedef Sen4XRadiometricIndices            Self;
  typedef Application                   Superclass;
  typedef itk::SmartPointer<Self>       Pointer;
  typedef itk::SmartPointer<const Self> ConstPointer;

  /** Standard macro */
  itkNewMacro(Self);

  itkTypeMacro(Sen4XRadiometricIndices, otb::Wrapper::Application);

  using InputType  = FloatVectorImageType;
  using MaskInputType  = InputType;
  // using OutputType = FloatImageType;
  using OutputType = FloatVectorImageType;
  using ShortImageType = Int16VectorImageType;


  using InputPixelType  = InputType::InternalPixelType;
  using MaskPixelType  = MaskInputType::InternalPixelType;
  //using OutputPixelType = OutputType::PixelType;
  using OutputPixelType = OutputType::InternalPixelType;
  using ShortPixelType = ShortImageType::PixelType;


  using RadiometricIndexType    = otb::Functor::RadiometricIndex<InputPixelType, OutputPixelType, MaskPixelType>;
  using IndicesStackFunctorType = otb::Functor::IndicesStackFunctor<InputPixelType, OutputPixelType, RadiometricIndexType, MaskPixelType>;

  using UnmaskedFilterType = itk::UnaryFunctorImageFilter<InputType,OutputType,
                  IndicesStackFunctorType >;
  using MaskedFilterType = itk::BinaryFunctorImageFilter<InputType,InputType,OutputType,
                  IndicesStackFunctorType >;

  using FloatToShortTransFilterType = itk::UnaryFunctorImageFilter<OutputType,ShortImageType,
                  FloatToShortTranslationFunctor<OutputType::PixelType, ShortPixelType> >;


  class indiceSpec
  {
  public:
    indiceSpec(std::string k, std::string i, RadiometricIndexType* ind, bool qRes = true) : key(k), item(i), indice(ind),
        quantifiableResult(qRes)
    {
    }
    std::string                           key;
    std::string                           item;
    std::unique_ptr<RadiometricIndexType> indice;
    bool quantifiableResult;                        // indicates that the value is subunitaire and
                                                    // can be converted to a short int with a quantification value of 1000
  };


private:
  void DoInit() override
  {
    SetName("Sen4XRadiometricIndices");
    SetDescription("Compute radiometric indices.");

    // Documentation
    SetDocLongDescription(
        "This application computes radiometric indices using the relevant channels of the input image. The output is a multi band image into which each "
        "channel is one of the selected indices.");
    SetDocLimitations("None");
    SetDocAuthors("OTB-Team");
    SetDocSeeAlso("otbVegetationIndicesFunctor, otbWaterIndicesFunctor and otbSoilIndicesFunctor classes");

    AddDocTag(Tags::FeatureExtraction);
    AddDocTag("Radiometric Indices");

    AddParameter(ParameterType_String, "xml", "Input product metadata file");
    SetParameterDescription("xml", "Input product metadata file");

    AddParameter(ParameterType_InputImage, "msks", "Masks flags used for masking output values");
    MandatoryOff("msks");

    AddParameter(ParameterType_OutputImage, "out", "Output Image");
    SetParameterDescription("out", "Radiometric indices output image");

    AddParameter(ParameterType_ListView, "list", "Available Radiometric Indices");
    SetParameterDescription("list",
                            "List of available radiometric indices with their relevant channels in brackets:\n\n"
                            "* Vegetation:NDVI - Normalized difference vegetation index (Red, NIR)\n"
                            "* Vegetation:TNDVI - Transformed normalized difference vegetation index (Red, NIR)\n"
                            "* Vegetation:RVI - Ratio vegetation index (Red, NIR)\n"
                            "* Vegetation:SAVI - Soil adjusted vegetation index (Red, NIR)\n"
                            "* Vegetation:TSAVI - Transformed soil adjusted vegetation index (Red, NIR)\n"
                            "* Vegetation:MSAVI - Modified soil adjusted vegetation index (Red, NIR)\n"
                            "* Vegetation:MSAVI2 - Modified soil adjusted vegetation index 2 (Red, NIR)\n"
                            "* Vegetation:GEMI - Global environment monitoring index (Red, NIR)\n"
                            "* Vegetation:IPVI - Infrared percentage vegetation index (Red, NIR)\n"
                            "* Vegetation:LAIFromNDVILog - Leaf Area Index from log NDVI (Red, NIR)\n"
                            "* Vegetation::LAIFromReflLinear - Leaf Area Index from reflectances with linear combination (Red, NIR)\n"
                            "* Vegetation::LAIFromNDVIFormo - Leaf Area Index from Formosat 2  TOC (Red, NIR)\n"
                            "* Water:NDWI - Normalized difference water index (Gao 1996) (NIR, MIR)\n"
                            "* Water:NDWI2 - Normalized difference water index (Mc Feeters 1996) (Green, NIR)\n"
                            "* Water:MNDWI - Modified normalized difference water index (Xu 2006) (Green, MIR)\n"
                            "* Water:NDTI - Normalized difference turbidity index (Lacaux et al.) (Red, Green)\n"
                            "* Soil:RI - Redness index (Red, Green)\n"
                            "* Soil:CI - Color index (Red, Green)\n"
                            "* Soil:BI - Brightness index (Red, Green)\n"
                            "* Soil:BI2 - Brightness index 2 (NIR, Red, Green)\n"
                            "* BuiltUp:ISU - Built Surfaces Index (NIR,Red) ");

    AddRAMParameter();

    // Doc example parameter settings
    SetDocExampleParameterValue("in", "qb_RoadExtract.tif");
    SetDocExampleParameterValue("list", "Vegetation:NDVI Vegetation:RVI Vegetation:IPVI");
    SetDocExampleParameterValue("out", "RadiometricIndicesImage.tif");

    m_Map.clear();

    m_Map.push_back({"list.ndvi", "Vegetation:NDVI", new otb::Functor::NDVI<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.tndvi", "Vegetation:TNDVI", new otb::Functor::TNDVI<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.rdvi", "Vegetation:RVI", new otb::Functor::RVI<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.savi", "Vegetation:SAVI", new otb::Functor::SAVI<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.tsavi", "Vegetation:TSAVI", new otb::Functor::TSAVI<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.msavi", "Vegetation:MSAVI", new otb::Functor::MSAVI<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.msavi2", "Vegetation:MSAVI2", new otb::Functor::MSAVI2<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.gemi", "Vegetation:GEMI", new otb::Functor::GEMI<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.ipvi", "Vegetation:IPVI", new otb::Functor::IPVI<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.laindvilog", "Vegetation:LAIFromNDVILog", new otb::Functor::LAIFromNDVILogarithmic<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.lairefl", "Vegetation:LAIFromReflLinear", new otb::Functor::LAIFromReflectancesLinear<InputPixelType, OutputPixelType, MaskPixelType>()});
//    m_Map.push_back({"list.laindviformo", "Vegetation:LAIFromNDVIFormo", new otb::Functor::LAIFromNDVIFormosat2Functor<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.ndwi", "Water:NDWI", new otb::Functor::NDWI<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.ndwi2", "Water:NDWI2", new otb::Functor::NDWI2<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.mndwi", "Water:MNDWI", new otb::Functor::MNDWI<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.ndti", "Water:NDTI", new otb::Functor::NDTI<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.ri", "Soil:RI", new otb::Functor::RI<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.ci", "Soil:CI", new otb::Functor::CI<InputPixelType, OutputPixelType, MaskPixelType>()});
    m_Map.push_back({"list.bi", "Soil:BI", new otb::Functor::BI<InputPixelType, OutputPixelType, MaskPixelType>(), true});
    m_Map.push_back({"list.bi2", "Soil:BI2", new otb::Functor::BI2<InputPixelType, OutputPixelType, MaskPixelType>(), true});
//    m_Map.push_back({"list.isu", "BuiltUp:ISU", new otb::Functor::ISU<InputPixelType, OutputPixelType, MaskPixelType>()});

    ClearChoices("list");

    for (unsigned int i = 0; i < m_Map.size(); i++)
    {
      AddChoice(m_Map[i].key, m_Map[i].item);
    }
  }

  // Compute required bands for selected indices
  std::vector<CommonBandNames> GetRequiredBands()
  {
    std::set<CommonBandNames> required;

    for (unsigned int idx = 0; idx < GetSelectedItems("list").size(); ++idx)
    {
      auto requiredForCurrentIndice = m_Map[GetSelectedItems("list")[idx]].indice->GetRequiredBands();
      required.insert(requiredForCurrentIndice.begin(), requiredForCurrentIndice.end());
    }
    return std::vector<CommonBandNames>(required.begin(), required.end());
  }

  std::vector<std::string> GetBandNamesToExtract(const std::vector<CommonBandNames> &bands)
  {
      std::vector<std::string> ret;
      for (const CommonBandNames &band: bands) {
          std::string bandName;
          switch(band) {
              case CommonBandNames::BLUE:
                  bandName = m_pHelper->GetBlueBandName();
                  break;
              case CommonBandNames::GREEN:
                  bandName = m_pHelper->GetGreenBandName();
                  break;
              case CommonBandNames::RED:
                  bandName = m_pHelper->GetRedBandName();
                  break;
              case CommonBandNames::NIR:
                  bandName = m_pHelper->GetNirBandName();
                  break;
              case CommonBandNames::MIR:
                  bandName = m_pHelper->GetSwirBandName();
                  break;
              default:
                  itkExceptionMacro("Band is not supported " << std::to_string((int)band));
                  break;
          }
          ret.push_back(std::move(bandName));
      }

      return ret;
  }

  void DoUpdateParameters() override
  {
    // Nothing to do here
  }

  void DoExecute() override
  {
      const std::string &inMetadataXml = GetParameterString("xml");
      if (inMetadataXml.empty())
      {
          itkExceptionMacro("No input metadata XML set...; please set the input image");
      }

    // Derive required bands from selected indices
    auto requiredBands = GetRequiredBands();

    auto factory = MetadataHelperFactory::New();
    // we load first the default resolution where we have the RED and NIR
    m_pHelper = factory->GetMetadataHelper<float>(inMetadataXml);
    const std::vector<std::string> &bandsToExtract = GetBandNamesToExtract(requiredBands);
    m_img = m_pHelper->GetImage(bandsToExtract, &m_bandIndexes);
    m_img->UpdateOutputInformation();

    // Read the spacing for the default image
//    int curRes = m_img->GetSpacing()[0];

//    int nOutRes = curRes;
//    if(HasValue("outres")) {
//        nOutRes = GetParameterInt("outres");
//        if(nOutRes != 10 && nOutRes != 20) {
//            itkExceptionMacro("Invalid output resolution specified (only 10 and 20 accepted)" << nOutRes);
//        }
//    }

    // Map to store association between bands and indices
    std::map<CommonBandNames, size_t> bandIndicesMap;

    for (size_t i = 0; i<m_bandIndexes.size(); i++) {
        bandIndicesMap[requiredBands[i]] = m_bandIndexes[i]+1;
    }

    // Find selected indices
    for (unsigned int idx = 0; idx < GetSelectedItems("list").size(); ++idx)
    {
      // Retrieve the indice instance
      m_indices.push_back(m_Map[GetSelectedItems("list")[idx]].indice.get());

      // And set bands using the band map
      m_indices.back()->SetBandsIndices(bandIndicesMap);
      m_indices.back()->SetNoDataValue(NO_DATA_VALUE);
    }

    // Build a composite indices functor to compute all indices at
    // once
    //auto compositeFunctor = IndicesStackFunctorType(indices);

    // Build and plug functor filter
    bool bHasMsks = HasValue("msks");
    if(bHasMsks) {
        m_msksImg = GetParameterFloatVectorImage("msks");
        m_maskedFilter = MaskedFilterType::New();
        m_maskedFilter->GetFunctor().SetIndices(m_indices);
        m_maskedFilter->SetInput1(m_img);
        m_maskedFilter->SetInput2(m_msksImg);
        m_maskedFilter->UpdateOutputInformation();
        m_maskedFilter->GetOutput()->SetNumberOfComponentsPerPixel(m_indices.size());
        m_maskedFilter->UpdateOutputInformation();
        m_functorOutput = m_maskedFilter->GetOutput();
    } else {
        m_unmaskedFilter = UnmaskedFilterType::New();
        m_unmaskedFilter->GetFunctor().SetIndices(m_indices);
        m_unmaskedFilter->SetInput(m_img);
        m_unmaskedFilter->UpdateOutputInformation();
        m_unmaskedFilter->GetOutput()->SetNumberOfComponentsPerPixel(m_indices.size());
        m_unmaskedFilter->UpdateOutputInformation();
        m_functorOutput = m_unmaskedFilter->GetOutput();
    }

//    m_floatToShortFunctor = FloatToShortTransFilterType::New();
//    // quantify the image using the default factor and considering 0 as NO_DATA but
//    // also setting all values less than 0 to 0
//    m_floatToShortFunctor->GetFunctor().Initialize(DEFAULT_QUANTIFICATION_VALUE, NO_DATA_VALUE, true);
//    m_floatToShortFunctor->SetInput(m_functorOutput);
//    m_floatToShortFunctor->GetOutput()->UpdateOutputInformation();

    // SetParameterOutputImagePixelType("out", ImagePixelType_int16);
    //SetParameterOutputImage("out", m_floatToShortFunctor->GetOutput());
    SetParameterOutputImage("out", m_functorOutput);

//    // Call register pipeline to allow streaming and garbage collection
//    RegisterPipeline();
  }

  std::vector<indiceSpec> m_Map;
  MetadataHelper<float>::VectorImageType::Pointer    m_img;
  MaskInputType::Pointer m_msksImg;
  std::unique_ptr<MetadataHelper<float>>             m_pHelper;
  std::vector<int>                                  m_bandIndexes;
  std::vector<RadiometricIndexType*> m_indices;
  MaskedFilterType::Pointer m_maskedFilter;
  UnmaskedFilterType::Pointer m_unmaskedFilter;
  OutputType::Pointer m_functorOutput;

  FloatToShortTransFilterType::Pointer  m_floatToShortFunctor;
};
}
}

OTB_APPLICATION_EXPORT(otb::Wrapper::Sen4XRadiometricIndices)

