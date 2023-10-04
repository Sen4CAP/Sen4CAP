/*
 * Copyright (C) 1999-2011 Insight Software Consortium
 * Copyright (C) 2005-2017 Centre National d'Etudes Spatiales (CNES)
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

#ifndef otbMarkersFromLabelImageFilter_h
#define otbMarkersFromLabelImageFilter_h

#include "otbPersistentImageFilter.h"
#include "itkNumericTraits.h"
#include "itkArray.h"
#include "itkSimpleDataObjectDecorator.h"
#include "otbPersistentFilterStreamingDecorator.h"

#include <limits>


#define NO_DATA -10000

namespace otb
{

/** \class StatisticsMapAccumulator
 * \brief Holds statistics for each label of a label image
 *
 * Intended to store and update the following statistics:
 * -count
 * -sum of values
 * -sum of squared values
 * -min
 * -max
 *
 * TODO:
 * -Better architecture?
 * -Enrich with other statistics?
 * -Move this class in a dedicated source to enable its use by other otbStatistics stuff?
 *
 * \ingroup OTBStatistics
 */
template<class TRealVectorPixelType>
class StatisticsAccumulator
{
public:

  typedef typename TRealVectorPixelType::ValueType  RealValueType;
  typedef uint64_t                                  PixelCountType;
  typedef itk::VariableLengthVector<PixelCountType> PixelCountVectorType;

  // Constructor (default)
  StatisticsAccumulator() {}

  // Constructor (initialize the accumulator with the given pixel)
  StatisticsAccumulator(const TRealVectorPixelType & pixel, bool isValidVal)
  {
      m_Pixels.push_back(pixel);
      TRealVectorPixelType &addedPixel = m_Pixels[m_Pixels.size()-1];

      auto nBands = pixel.GetSize();
      m_Count.SetSize(nBands);
      m_CountInvalid.SetSize(nBands);
      m_Sum.SetSize(nBands);
      m_Min.SetSize(nBands);
      m_Max.SetSize(nBands);
      m_SqSum.SetSize(nBands);
      for (unsigned int band = 0 ; band < m_SqSum.GetSize() ; band++) {
          if (isValidVal && checkValid(pixel[band])) {
              m_Count[band] = 1;
              m_CountInvalid[band] = 0;
              m_Sum = pixel[band];
              m_Min = pixel[band];
              m_Max = pixel[band];
              m_SqSum[band] *= m_SqSum[band];
          } else {
              m_CountInvalid[band] = 1;
              m_Count[band] = 0;
              m_Sum[band] = 0;
              m_Min[band] = 0;
              m_Max[band] = 0;
              m_SqSum[band] = 0;
              addedPixel[band] = NO_DATA;
          }
      }
  }

  // Constructor (other)
  StatisticsAccumulator(const StatisticsAccumulator & other)
  {
      m_Pixels = other.m_Pixels;

      m_Count = other.m_Count;
      m_CountInvalid = other.m_CountInvalid;
      m_Sum = other.m_Sum;
      m_Min = other.m_Min;
      m_Max = other.m_Max;
      m_SqSum = other.m_SqSum;
  }

  // Destructor
  ~StatisticsAccumulator(){}

  // Function update (pixel)
  void Update(const TRealVectorPixelType & pixel, bool isValidVal)
  {
      m_Pixels.push_back(pixel);
      TRealVectorPixelType &addedPixel = m_Pixels[m_Pixels.size()-1];

      const unsigned int nBands = pixel.GetSize();
      for (unsigned int band = 0 ; band < nBands ; band ++ )
      {
          const RealValueType value = pixel[band];
          if (isValidVal && checkValid(value))
          {
              m_Count[band]++;
              const RealValueType sqValue = value * value;
              UpdateValues(value, sqValue, value, value,
                     m_Sum[band], m_SqSum[band], m_Min[band], m_Max[band]);
          } else {
              m_CountInvalid[band]++;
              addedPixel[band] = NO_DATA;
          }
      }
  }

  // Function update (self)
  void Update(const StatisticsAccumulator & other)
  {
      m_Pixels.insert(m_Pixels.end(), other.m_Pixels.begin(), other.m_Pixels.end());

      m_Count += other.m_Count;
      m_CountInvalid += other.m_CountInvalid;
      const unsigned int nBands = other.m_Sum.GetSize();
      for (unsigned int band = 0 ; band < nBands ; band ++ )
      {
          UpdateValues(other.m_Sum[band], other.m_SqSum[band], other.m_Min[band], other.m_Max[band],
                 m_Sum[band], m_SqSum[band], m_Min[band], m_Max[band]);
      }
  }

  bool checkValid(const RealValueType &value)
  {
      if ((value != 0.0) && (value != NO_DATA))
      {
          return true;
      }
      return false;
  }

  std::vector<RealValueType> GetBandSortedValues(int band) {
      std::vector<RealValueType> retVals;
      for(auto it = std::begin(m_Pixels); it != std::end(m_Pixels); ++it) {
          const auto &val = (*it)[band];
          if (val != NO_DATA) {
              retVals.push_back(val);
          }
      }
      std::sort(retVals.begin(), retVals.end());
      // keep the only values
      retVals.erase(std::unique(retVals.begin(), retVals.end()), retVals.end());
      return retVals;
  }

  RealValueType ComputeQuartile(const std::vector<RealValueType> &vals, int percentile) {
      RealValueType ret = 0;
      int sz = vals.size();
      if (sz > 0) {
          int first = sz * percentile/100;
          if ((sz % 2) == 0) {
              ret = (vals[first] + vals[first + 1])/2;
          } else {
              ret = vals[first + 1];
          }
      }
      return ret;
  }

  // Accessors
  itkGetMacro(Sum, TRealVectorPixelType)
  itkGetMacro(SqSum, TRealVectorPixelType)
  itkGetMacro(Min, TRealVectorPixelType)
  itkGetMacro(Max, TRealVectorPixelType)
  itkGetMacro(Count, TRealVectorPixelType)
  itkGetMacro(CountInvalid, TRealVectorPixelType)
  itkGetMacro(Pixels, std::vector<TRealVectorPixelType>)

private:
  void UpdateValues(const RealValueType & otherSum, const RealValueType & otherSqSum,
                  const RealValueType & otherMin, const RealValueType & otherMax,
                  RealValueType & sum, RealValueType & sqSum,
                  RealValueType & min, RealValueType & max)
  {
      sum += otherSum;
      sqSum += otherSqSum;
      if (otherMin < min)
      {
          min = otherMin;
      }
      if (otherMax > max)
      {
          max = otherMax;
      }
  }

public:
  std::vector<TRealVectorPixelType> m_Pixels;

protected:
  TRealVectorPixelType m_Sum;
  TRealVectorPixelType m_SqSum;
  TRealVectorPixelType m_Min;
  TRealVectorPixelType m_Max;
  TRealVectorPixelType m_Count;
  TRealVectorPixelType m_CountInvalid;
};

/** \class PersistentMarkersFromLabelImageFilter
 * \brief Computes mean radiometric value for each label of a label image, based on a support VectorImage
 *
 * This filter persists its temporary data. It means that if you Update it n times on n different
 * requested regions, the output statistics will be the statitics of the whole set of n regions.
 *
 * To reset the temporary data, one should call the Reset() function.
 *
 * To get the statistics once the regions have been processed via the pipeline, use the Synthetize() method.
 *
 *
 * \sa StreamingStatisticsMapFromLabelImageFilter
 * \ingroup Streamed
 * \ingroup Multithreaded
 * \ingroup MathematicalStatisticsImageFilters
 *
 * \ingroup OTBStatistics
 */
template<class TInputVectorImage, class TLabelImage, class TMaskImage>
class ITK_EXPORT PersistentMarkersFromLabelImageFilter :
public PersistentImageFilter<TInputVectorImage, TInputVectorImage>
{
public:
  /** Standard Self typedef */
  typedef PersistentMarkersFromLabelImageFilter               Self;
  typedef PersistentImageFilter<TInputVectorImage, TInputVectorImage> Superclass;
  typedef itk::SmartPointer<Self>                         Pointer;
  typedef itk::SmartPointer<const Self>                   ConstPointer;

  /** Method for creation through the object factory. */
  itkNewMacro(Self);

  /** Runtime information support. */
  itkTypeMacro(PersistentMarkersFromLabelImageFilter, PersistentImageFilter);

  /** Image related typedefs. */
  typedef TInputVectorImage                   VectorImageType;
  typedef typename TInputVectorImage::Pointer InputVectorImagePointer;
  typedef TLabelImage                         LabelImageType;
  typedef typename TLabelImage::Pointer       LabelImagePointer;
  typedef TMaskImage                          MaskImageType;
  typedef typename TMaskImage::Pointer        MaskImagePointer;

  typedef typename VectorImageType::RegionType                          RegionType;
  typedef typename VectorImageType::PixelType                           VectorPixelType;
  typedef typename VectorImageType::PixelType::ValueType                VectorPixelValueType;
  typedef typename LabelImageType::PixelType                            LabelPixelType;
  typedef itk::VariableLengthVector<double>                             RealVectorPixelType;

  typedef struct {
      RealVectorPixelType mean;
      RealVectorPixelType stdDev;
  } MeanStdDevValueType;


  typedef StatisticsAccumulator<RealVectorPixelType>                    AccumulatorType;
  typedef std::map<LabelPixelType, AccumulatorType >                    AccumulatorMapType;
  typedef std::vector<AccumulatorMapType>                               AccumulatorMapCollectionType;
  typedef itk::VariableLengthVector<int32_t>                            PixelCountVectorType;

  typedef std::map<LabelPixelType, MeanStdDevValueType >                PixeMeanStdDevlValueMapType;
  typedef std::map<LabelPixelType, RealVectorPixelType >                PixelValueMapType;
  typedef std::map<LabelPixelType, double>                              LabelPopulationMapType;
  typedef std::map<LabelPixelType, PixelCountVectorType>                PixelCountMapType;

  itkStaticConstMacro(InputImageDimension, unsigned int,
                      TInputVectorImage::ImageDimension);

  /** Image related typedefs. */
  itkStaticConstMacro(ImageDimension, unsigned int,
                      TInputVectorImage::ImageDimension);

  /** Set/Get macro for the flag specifying if min/max should be computed or not */
  itkSetMacro(ComputeMinMax, bool)
  itkGetMacro(ComputeMinMax, bool)

  /** Set/Get macro for the flag specifying if valid pixels count should be computed or not */
  itkSetMacro(ComputeValidPixelsCnt, bool)
  itkGetMacro(ComputeValidPixelsCnt, bool)

  /** Set/Get macro for the flag specifying if invalid ixels count should be computed or not */
  itkSetMacro(ComputeInvalidPixelsCnt, bool)
  itkGetMacro(ComputeInvalidPixelsCnt, bool)

  /** Set/Get macro for the flag specifying if median should be computed or not */
  itkSetMacro(ComputeMedian, bool)
  itkGetMacro(ComputeMedian, bool)

  /** Set/Get macro for the flag specifying if P25 quartile should be computed or not */
  itkSetMacro(ComputeP25, bool)
  itkGetMacro(ComputeP25, bool)

  /** Set/Get macro for the flag specifying if P75 quartile should be computed or not */
  itkSetMacro(ComputeP75, bool)
  itkGetMacro(ComputeP75, bool)

  /** Set/Get macro for the flag specifying the value of the valid mask value */
  itkSetMacro(ValidMaskValue, int)
  itkGetMacro(ValidMaskValue, int)

  /** Smart Pointer type to a DataObject. */
  typedef typename itk::DataObject::Pointer DataObjectPointer;
  typedef itk::ProcessObject::DataObjectPointerArraySizeType DataObjectPointerArraySizeType;

  typedef itk::ImageBase<InputImageDimension> ImageBaseType;
  typedef typename ImageBaseType::RegionType InputImageRegionType;

  /** Type of DataObjects used for scalar outputs */
  typedef itk::SimpleDataObjectDecorator<PixelValueMapType>  PixelValueMapObjectType;

  /** Set input label image */
  virtual void SetInputLabelImage( const LabelImageType *image);

  /** Get input label image */
  virtual const LabelImageType * GetInputLabelImage();

  void SetMaskInputImage(const TMaskImage* mask);
  const TMaskImage* GetMaskInputImage();

  /** Return the computed Mean and Standard Deviation for each label in the input label image */
  PixeMeanStdDevlValueMapType GetMeanStdDevValueMap() const;

  /** Return the computed Min for each label in the input label image */
  PixelValueMapType GetMinValueMap() const;

  /** Return the computed Max for each label in the input label image */
  PixelValueMapType GetMaxValueMap() const;

  /** Return the computed the number of valid pixels for each label in the input label image */
  PixelValueMapType GetValidPixelsCntMap() const;

  /** Return the computed the number of invalid pixels for each label in the input label image */
  PixelValueMapType GetInvalidPixelsCntMap() const;

  /** Return the computed the median for each label in the input label image */
  PixelValueMapType GetMedianValuesMap() const;

  /** Return the computed the P25 quartile for each label in the input label image */
  PixelValueMapType GetP25ValuesMap() const;

  /** Return the computed the P55 quartile for each label in the input label image */
  PixelValueMapType GetP75ValuesMap() const;

  /** Make a DataObject of the correct type to be used as the specified
   * output. */
  DataObjectPointer MakeOutput(DataObjectPointerArraySizeType idx) override;
  using Superclass::MakeOutput;

  /** Pass the input through unmodified. Do this by Grafting in the
   *  AllocateOutputs method.
   */
  void AllocateOutputs() override;

  void GenerateOutputInformation() override;

  void Synthetize(void) override;

  void Reset(void) override;

  /** Due to heterogeneous input template GenerateInputRequestedRegion must be reimplemented using explicit cast **/
  /** This new implementation is inspired by the one of itk::ImageToImageFilter **/
  void GenerateInputRequestedRegion() override;

protected:
  PersistentMarkersFromLabelImageFilter();
  ~PersistentMarkersFromLabelImageFilter() override {}
  void PrintSelf(std::ostream& os, itk::Indent indent) const override;

  void ThreadedGenerateData(const RegionType& outputRegionForThread, itk::ThreadIdType threadId ) override;

private:
  PersistentMarkersFromLabelImageFilter(const Self &) = delete;
  void operator =(const Self&) = delete;

  AccumulatorMapCollectionType           m_AccumulatorMaps;

  PixeMeanStdDevlValueMapType                    m_MeanStdDevRadiometricValue;

  PixelValueMapType                      m_MinRadiometricValue;
  PixelValueMapType                      m_MaxRadiometricValue;
  PixelValueMapType                      m_ValidPixelsCnt;
  PixelValueMapType                      m_InvalidPixelsCnt;
  PixelValueMapType                      m_MedianValue;
  PixelValueMapType                      m_P25Value;
  PixelValueMapType                      m_P75Value;

  bool m_ComputeMinMax;
  bool m_ComputeMedian;
  bool m_ComputeP25;
  bool m_ComputeP75;
  bool m_ComputeValidPixelsCnt;
  bool m_ComputeInvalidPixelsCnt;
  int m_ValidMaskValue;

}; // end of class PersistentMarkersFromLabelImageFilter


/*===========================================================================*/

/** \class StreamingStatisticsMapFromLabelImageFilter
 * \brief Computes mean radiometric value for each label of a label image, based on a support VectorImage
 *
 * Currently the class only computes the mean value.
 *
 * This class streams the whole input image through the PersistentMarkersFromLabelImageFilter.
 *
 * This way, it allows computing the first order global statistics of this image.
 * It calls the Reset() method of the PersistentStatisticsImageFilter before streaming
 * the image and the Synthetize() method of the PersistentStatisticsImageFilter
 * after having streamed the image to compute the statistics.
 * The accessor on the results are wrapping the accessors of the
 * internal PersistentStatisticsImageFilter.
 *
 * This filter can be used as:
 * \code
 * typedef otb::StreamingStatisticsMapFromLabelImageFilter<ImageType> StatisticsType;
 * StatisticsType::Pointer statistics = StatisticsType::New();
 * statistics->SetInput(reader->GetOutput());
 * statistics->Update();
 * StatisticsType::PixelValueMapType meanValueMap = statistics->GetMeanValueMap();
 * StatisticsType::PixelValueMapType::const_iterator end = meanValueMap();
 * for (StatisticsType::PixelValueMapType::const_iterator it = meanValueMap.begin(); it != end; ++it)
 * {
 *       std::cout << "label : " << it->first << " , ";
 *                 << "mean value : " << it->second << std::endl;
 * }
 * \endcode
 *
 *
 * \sa PersistentStatisticsImageFilter
 * \sa PersistentImageFilter
 * \sa PersistentFilterStreamingDecorator
 * \sa StreamingImageVirtualWriter
 *
 * \ingroup Streamed
 * \ingroup Multithreaded
 * \ingroup MathematicalStatisticsImageFilters
 *
 * \ingroup OTBStatistics
 */

template<class TInputVectorImage, class TLabelImage, class TMaskImage>
class ITK_EXPORT MarkersFromLabelImageFilter :
public PersistentFilterStreamingDecorator<PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage> >
{
public:
  /** Standard Self typedef */
  typedef MarkersFromLabelImageFilter Self;
  typedef PersistentFilterStreamingDecorator
      <PersistentMarkersFromLabelImageFilter<TInputVectorImage, TLabelImage, TMaskImage> > Superclass;
  typedef itk::SmartPointer<Self>       Pointer;
  typedef itk::SmartPointer<const Self> ConstPointer;

  /** Type macro */
  itkNewMacro(Self);

  /** Creation through object factory macro */
  itkTypeMacro(MarkersFromLabelImageFilter, PersistentFilterStreamingDecorator);

  typedef TInputVectorImage                   VectorImageType;
  typedef TLabelImage                         LabelImageType;
  typedef TMaskImage                          MaskImageType;

  typedef typename VectorImageType::PixelType                        VectorPixelType;
  typedef typename VectorImageType::PixelType::ValueType             VectorPixelValueType;

  typedef typename Superclass::FilterType::PixelValueMapType         PixelValueMapType;
  typedef typename Superclass::FilterType::PixelValueMapObjectType   PixelValueMapObjectType;

  typedef typename Superclass::FilterType::LabelPopulationMapType    LabelPopulationMapType;
  typedef typename Superclass::FilterType::PixelCountMapType    PixelCountMapType;
  typedef typename Superclass::FilterType::PixeMeanStdDevlValueMapType PixeMeanStdDevlValueMapType;

  /** Set input multispectral image */
  using Superclass::SetInput;
  void SetInput(const VectorImageType * input)
  {
    this->GetFilter()->SetInput(input);
  }

  /** Get input multispectral image */
  const VectorImageType * GetInput()
  {
    return this->GetFilter()->GetInput();
  }

  /** Set input label image (monoband) */
  void SetInputLabelImage(const LabelImageType * input)
  {
    this->GetFilter()->SetInputLabelImage(input);
  }

  /** Get input label image (monoband) */
  const LabelImageType * GetInputLabelImage()
  {
    return this->GetFilter()->GetInputLabelImage();
  }

  /** Set input mask image (monoband) */
  void SetMaskInputImage(const MaskImageType * input)
  {
    this->GetFilter()->SetMaskInputImage(input);
  }

  /** Get input mask image (monoband) */
  const LabelImageType * GetMaskInputImage()
  {
    return this->GetFilter()->GetMaskInputImage();
  }

  /** Return the computed Mean and Standard Deviation for each label in the input label image */
  PixeMeanStdDevlValueMapType GetMeanStdDevValueMap() const
  {
      return this->GetFilter()->GetMeanStdDevValueMap();
  }

  /** Return the computed Min for each label in the input label image */
  PixelValueMapType GetMinValueMap() const
  {
      return this->GetFilter()->GetMinValueMap();
  }

  /** Return the computed Max for each label in the input label image */
  PixelValueMapType GetMaxValueMap() const
  {
      return this->GetFilter()->GetMaxValueMap();
  }

  /** Return the computed the number of valid pixels for each label in the input label image */
  PixelValueMapType GetValidPixelsCntMap() const
  {
      return this->GetFilter()->GetValidPixelsCntMap();
  }

  /** Return the computed the number of invalid pixels for each label in the input label image */
  PixelValueMapType GetInvalidPixelsCntMap() const
  {
      return this->GetFilter()->GetInvalidPixelsCntMap();
  }

  /** Return the computed median for each label in the input label image */
  PixelValueMapType GetMedianValuesMap() const
  {
      return this->GetFilter()->GetMedianValuesMap();
  }

  /** Return the computed the P25 quatrile for each label in the input label image */
  PixelValueMapType GetP25ValuesMap() const
  {
      return this->GetFilter()->GetP25ValuesMap();
  }

  /** Return the computed the P75 quatrile for each label in the input label image */
  PixelValueMapType GetP75ValuesMap() const
  {
      return this->GetFilter()->GetP75ValuesMap();
  }

  void SetComputeMinMax(bool exp)
  {
      this->GetFilter()->SetComputeMinMax(exp);
  }
  bool GetComputeMinMax() const
  {
      return this->GetFilter()->GetComputeMinMax();
  }
  void SetComputeValidPixelsCnt(bool exp)
  {
      this->GetFilter()->SetComputeValidPixelsCnt(exp);
  }
  bool GetComputeValidPixelsCnt()
  {
      return this->GetFilter()->GetComputeValidPixelsCnt();
  }
  void SetComputeInvalidPixelsCnt(bool exp)
  {
    this->GetFilter()->SetComputeInvalidPixelsCnt(exp);
  }
  bool GetComputeInvalidPixelsCnt() const
  {
      return this->GetFilter()->GetComputeInvalidPixelsCnt();
  }
  void SetComputeMedian(bool exp)
  {
    this->GetFilter()->SetComputeMedian(exp);
  }
  bool GetComputeMedian()
  {
      return this->GetFilter()->GetComputeMedian();
  }
  void SetComputeP25(bool exp)
  {
    this->GetFilter()->SetComputeP25(exp);
  }
  bool GetComputeP25() const
  {
      return this->GetFilter()->GetComputeP25();
  }
  void SetComputeP75(bool exp)
  {
    this->GetFilter()->SetComputeP75(exp);
  }
  bool GetComputeP75() const
  {
      return this->GetFilter()->GetComputeP75();
  }
  void SetMaskValidValue(int exp)
  {
    this->GetFilter()->SetValidMaskValue(exp);
  }
  int GetMaskValidValue() const
  {
      return this->GetFilter()->GetValidMaskValue();
  }
protected:
  /** Constructor */
  MarkersFromLabelImageFilter() {}
  /** Destructor */
  ~MarkersFromLabelImageFilter() override {}

private:
  MarkersFromLabelImageFilter(const Self &) = delete;
  void operator =(const Self&) = delete;
};

} // end namespace otb

#ifndef OTB_MANUAL_INSTANTIATION
#include "otbMarkersFromLabelImageFilter.hxx"
#endif

#endif
