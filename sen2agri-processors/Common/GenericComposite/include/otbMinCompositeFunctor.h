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

#ifndef otbMinCompositeFunctor_h
#define otbMinCompositeFunctor_h

#include "otbCompositeFunctor.h"
#include "GlobalDefs.h"

using namespace otb::BandName;

namespace otb
{
namespace Functor
{
/**
 * \class MinCompositeFunctor
 * \brief
 *
 *
 * \ingroup OTBComposite
 */
template <typename TInput, typename TOutput, typename TInput2=TInput>
class MinCompositeFunctor : public CompositeFunctor<TInput, TOutput, TInput2>
{
public:
  MinCompositeFunctor()
  {
  }

  /**
   * TBD
   * \param TBD
   * \return TBD
   */
  TOutput operator()(const std::vector< TInput > & input)const override
  {
      TOutput outPix = NO_DATA_VALUE;
      int i = 0;
      bool mskValidPixel = true;
      size_t imgsCnt = this->m_HasMasks ? (input.size() / 2) : input.size();
      for (i = 0; i<imgsCnt; i++)
      {
          const auto &inPix = input[i];
          mskValidPixel = (!this->m_HasMasks || (input[i+imgsCnt] == this->m_MskValidValue));
          if (mskValidPixel && (!this->m_UseNoDataValue || inPix != this->m_NoDataValue) && inPix > 0)
          {
              if (outPix == NO_DATA_VALUE || inPix < outPix) {
                  outPix = inPix;
              }
          }
      }
      return outPix;
  }
};

} // namespace Functor
} // End namespace otb

#endif
