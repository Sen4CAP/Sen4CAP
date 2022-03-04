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

#ifndef otbCompositeFunctor_h
#define otbCompositeFunctor_h

#include "itkVariableLengthVector.h"
#include "otbBandName.h"
#include <array>
#include <set>
#include <string>
#include <map>
#include <stdexcept>

using namespace otb::BandName;

namespace otb
{
namespace Functor
{
/**
 * \class RadiometricIndex
 * \brief Base class for all composite functors
 *
 * This class is the base class for all composite functors.
 *
 * It offers services to:
 * - TBD
  * - Compute the composite response to a pixel by subclassing the pure
 * virtual operator()
 *
 * \ingroup OTBComposites
 */
template <typename TInput, typename TOutput, typename TInput2=TInput>
class CompositeFunctor
{
public:
  /// Types for input/output
  using InputType  = TInput;
  using PixelType  = itk::VariableLengthVector<InputType>;
  using MaskPixelType  = itk::VariableLengthVector<TInput2>;
  using OutputType = TOutput;

  static constexpr double Epsilon = 0.0000001;

  // Necessary to be used as an abstract base class
  virtual ~CompositeFunctor() = default;

  /**
   * \throw TBD
   */
  CompositeFunctor() : m_UseNoDataValue(false), m_NoDataValue(0.)
  {
  }

  /**
   * \param noData the no data value for the reflectances bands
   */
  void SetNoDataValue(float noData)
  {
      m_NoDataValue = noData;
  }

  /**
    * Enabled the usage of no data value. The noDataValue should be set
    * otherwise default value is used
   */
  void UseNoDataValueOn()
  {
      m_UseNoDataValue = true;
  }

  /**
    * Disables the usage of no data value
   */
  void UseNoDataValueOff()
  {
      m_UseNoDataValue = false;
  }

  /**
   * \param val enable or disable the use of no data value
   */
  void SetUseNoDataValue(bool val)
  {
      m_UseNoDataValue = val;
  }

  /**
    * Enabled the usage of mask value. The MskValidValue should be set
    * otherwise default value is used
   */
  void HasMasksOn()
  {
      m_HasMasks = true;
  }

  /**
    * Disables the usage of masks
   */
  void HasMasksOff()
  {
      m_HasMasks = false;
  }

  /**
    * Enabled the usage of mask value. The MskValidValue should be set
    * otherwise default value is used
   */
  void SetHasMasksValue(bool val)
  {
      m_HasMasks = val;
  }

  /**
   * \param val the mask valid value for the masks bands
   */
  void SetMskValidValue(float val)
  {
      m_MskValidValue = val;
  }

  /**
   * Astract method which will compute the composite
   * \param input A itk::VariableLengthVector<TInput> holding the
   * pixel values for each band
   * \return The indice value as TOutput  (starts at 1 for first band)
   */
  virtual TOutput operator()(const std::vector< TInput > & ) const = 0;

  /**
   * Method which will compute the composite using a mask. If not overridden, the default
   * implementation uses the no mask operator()
   * \param input A itk::VariableLengthVector<TInput> holding the
   * pixel values for each band
   * \return The indice value as TOutput  (starts at 1 for first band)
   */
  virtual TOutput operator()(const std::vector< TInput > & input, const std::vector< TInput > &) const
  {
      return (*this)(input);
  }

protected:
  /**
   * Check if the value provided is no data value (if no data value is set)
   * \param value the value to be checked
   * \return true if no data value is set and the value is equals to no data.
   */
  bool CheckNoData(double value) const
  {
      if (m_UseNoDataValue && (std::fabs(value - m_NoDataValue) < Epsilon))
      {
        return true;
      }
      return false;
  }

protected:
    bool m_UseNoDataValue;
    float m_NoDataValue;

    bool m_HasMasks;
    float m_MskValidValue;

private:
};

template <typename TInput, typename TOutput, typename Functor>
class CompositeWrapperFunctor
{
public:
    void SetFunctor(Functor *functor)
    {
        if (functor == NULL)
        {
            throw std::runtime_error("Can not create wrapper for null functor.");
        }
        m_Functor = functor;
    }

    TOutput operator()(const std::vector< TInput > & input)
    {
        return (*m_Functor)(input);
    }
    virtual TOutput operator()(const std::vector< TInput > & input, const std::vector< TInput > &msks)
    {
        return (*m_Functor)(input, msks);
    }

private:
    Functor *m_Functor;
};

} // namespace Functor
} // End namespace otb

#endif
