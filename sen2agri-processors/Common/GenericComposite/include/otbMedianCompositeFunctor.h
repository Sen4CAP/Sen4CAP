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

#ifndef otbMedianCompositeFunctor_h
#define otbMedianCompositeFunctor_h

#include "otbCompositeFunctor.h"
#include "GlobalDefs.h"

using namespace otb::BandName;

namespace otb
{
namespace Functor
{
/**
 * \class MedianCompositeFunctor
 * \brief
 *
 *
 * \ingroup OTBComposite
 */
template <typename TInput, typename TOutput, typename TInput2=TInput>
class MedianCompositeFunctor : public CompositeFunctor<TInput, TOutput, TInput2>
{
public:
  MedianCompositeFunctor()
  {
  }

  /**
   * TBD
   * \param TBD
   * \return TBD
   */
  TOutput operator()(const std::vector< TInput > & input)const override
  {
      TOutput outPix;

      std::vector< TInput > input2 = input;
      sort_values(input2);
      size_t imgsCnt = this->m_HasMasks ? (input2.size() / 2) : input2.size();

      // TODO: In the formula below we don't take into account the cloud masks
      outPix = ((imgsCnt%2) == 0) ? (input2[imgsCnt/2] + input2[imgsCnt/2 + 1])/2 : input2[(imgsCnt+1)/2];

      return outPix;
  }

  void sort_values(std::vector<TInput>& input) const
  {
    int miniPos;
    size_t imgsCnt = this->m_HasMasks ? (input.size() / 2) : input.size();

    for (int i = 0; i < imgsCnt; i++)
    {
        miniPos = i;
        for (int j = i + 1; j < imgsCnt; j++)
        {
            if (input[j] < input[miniPos])
            {
                miniPos = j;
            }
        }
        if (miniPos != i)
        {
            std::swap(input[miniPos], input[i]);
            if (this->m_HasMasks) {
                // swap also the mask values to correspond to the pixel values
                std::swap(input[miniPos+imgsCnt], input[i+imgsCnt]);
            }
        }
      }
    }
};

} // namespace Functor
} // End namespace otb

#endif
