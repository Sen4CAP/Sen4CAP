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

#ifndef otbSoilIndicesFunctor_h
#define otbSoilIndicesFunctor_h

#include "otbMath.h"
#include "otbRadiometricIndex.h"
#include "GlobalDefs.h"

namespace otb
{
namespace Functor
{

template <class TInput, class TOutput, class TInput2=TInput>
class SoilIndicesFunctorBase : public RadiometricIndex<TInput, TOutput, TInput2>
{
public:
    /// Enum Among which bands are used
    using BandNameType = CommonBandNames;

    SoilIndicesFunctorBase(const std::set<BandNameType>& requiredBands) :
        RadiometricIndex<TInput, TOutput, TInput2>(requiredBands)
    {
    }

    virtual TOutput operator()(const itk::VariableLengthVector<TInput>& input) const = 0;

    TOutput operator()(const itk::VariableLengthVector<TInput>& input, const itk::VariableLengthVector<TInput2>& mask) const override
    {
        if(mask[0] != IMG_FLG_LAND)
        {
            return this->m_bHasNoData ? this->m_NoDataValue : 0.;
        }
        return (*this)(input);
    }
};

/** \class RI
 *  \brief This functor computes the Redness Index (RI)
 *
 *  [Pouget et al., "Caracteristiques spectrales des surfaces sableuses
 *   de la region cotiere nord-ouest de l'Egypte: application aux donnees
 *   satellitaires Spot, In: 2eme Journeees de Teledetection: Caracterisation
 *   et suivi des milieux terrestres en regions arides et tropicales. 4-6/12/1990
 *   Ed. ORSTOM, Collection Colloques et Seminaires, Paris, pp. 27-38]
 *
 *  \ingroup Functor
 *  \ingroup Radiometry
 *
 * \ingroup OTBIndices
 */
template <class TInput, class TOutput, class TInput2=TInput>
class RI : public SoilIndicesFunctorBase<TInput, TOutput, TInput2>
{
public:
  RI() : SoilIndicesFunctorBase<TInput, TOutput, TInput2>({CommonBandNames::RED, CommonBandNames::GREEN})
  {
  }

  TOutput operator()(const itk::VariableLengthVector<TInput>& input) const override
  {
    auto green = this->Value(CommonBandNames::GREEN, input);
    auto red   = this->Value(CommonBandNames::RED, input);

    if (std::abs(green) < RadiometricIndex<TInput, TOutput>::Epsilon)
    {
      return static_cast<TOutput>(0.);
    }

    return static_cast<TOutput>(red * red / (green * green * green));
  }
};

/** \class CI
 *  \brief This functor computes the Color Index (IC)
 *
 *  [Pouget et al., "Caracteristiques spectrales des surfaces sableuses
 *   de la region cotiere nord-ouest de l'Egypte: application aux donnees
 *   satellitaires Spot, In: 2eme Journeees de Teledetection: Caracterisation
 *   et suivi des milieux terrestres en regions arides et tropicales. 4-6/12/1990
 *   Ed. ORSTOM, Collection Colloques et Seminaires, Paris, pp. 27-38]
 *
 *  \ingroup Functor
 * \ingroup Radiometry
 *
 * \ingroup OTBIndices
 */
template <class TInput, class TOutput, class TInput2=TInput>
class CI : public SoilIndicesFunctorBase<TInput, TOutput, TInput2>
{
public:
  CI() : SoilIndicesFunctorBase<TInput, TOutput, TInput2>({CommonBandNames::RED, CommonBandNames::GREEN})
  {
  }

  TOutput operator()(const itk::VariableLengthVector<TInput>& input) const override
  {
    auto green = this->Value(CommonBandNames::GREEN, input);
    auto red   = this->Value(CommonBandNames::RED, input);

    if (std::abs(green + red) < RadiometricIndex<TInput, TOutput>::Epsilon)
    {
      return static_cast<TOutput>(0.);
    }

    return (static_cast<TOutput>((red - green) / (red + green)));
  }
};

/** \class BI
 *  \brief This functor computes the Brilliance Index (BI)
 *
 *  [ ]
 *
 *  \ingroup Functor
 * \ingroup Radiometry
 *
 * \ingroup OTBIndices
 */
template <class TInput, class TOutput, class TInput2=TInput>
class BI : public SoilIndicesFunctorBase<TInput, TOutput, TInput2>
{
public:
  BI() : SoilIndicesFunctorBase<TInput, TOutput, TInput2>({CommonBandNames::RED, CommonBandNames::GREEN})
  {
  }

  TOutput operator()(const itk::VariableLengthVector<TInput>& input) const override
  {
    auto green = this->Value(CommonBandNames::GREEN, input);
    auto red   = this->Value(CommonBandNames::RED, input);

    return (static_cast<TOutput>(std::sqrt((red * red + green * green) / 2.)));
  }
};

/** \class BI2
 *  \brief This functor computes the Brilliance Index (BI2)
 *
 *  [ ]
 *
 *  \ingroup Functor
 * \ingroup Radiometry
 *
 * \ingroup OTBIndices
 */
template <class TInput, class TOutput, class TInput2=TInput>
class BI2 : public SoilIndicesFunctorBase<TInput, TOutput, TInput2>
{
public:
  BI2() : SoilIndicesFunctorBase<TInput, TOutput, TInput2>({CommonBandNames::RED, CommonBandNames::GREEN, CommonBandNames::NIR})
  {
  }

  TOutput operator()(const itk::VariableLengthVector<TInput>& input) const override
  {
    auto green = this->Value(CommonBandNames::GREEN, input);
    auto red   = this->Value(CommonBandNames::RED, input);
    auto nir   = this->Value(CommonBandNames::NIR, input);

    return (static_cast<TOutput>(std::sqrt((red * red + green * green + nir * nir) / 3.)));
  }
};

} // namespace Functor
} // namespace otb

#endif
