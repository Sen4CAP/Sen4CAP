/*=========================================================================
  Program:   otb-bv
  Language:  C++

  Copyright (c) CESBIO. All rights reserved.

  See otb-bv-copyright.txt for details.

  This software is distributed WITHOUT ANY WARRANTY; without even
  the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
  PURPOSE.  See the above copyright notices for more information.

=========================================================================*/

#include "otbWrapperApplication.h"
#include "otbWrapperApplicationFactory.h"
#include "itkBinaryFunctorImageFilter.h"

#include <vector>

#include "dateUtils.h"
#include "phenoFunctions.h"
#include "MetadataHelperFactory.h"

using PrecisionType = double;
using VectorType = std::vector<PrecisionType>;
constexpr PrecisionType not_processed_value{0};
constexpr PrecisionType processed_value{1};

namespace otb
{
int date_to_doy(std::string& date_str)
{
  return pheno::doy(pheno::make_date(date_str));
}

namespace Functor
{
}


namespace Wrapper
{

template< class TInput1, class TInput2, class TOutput>
class MetricsEstimationFunctor
{
public:
  MetricsEstimationFunctor() {}
  ~MetricsEstimationFunctor() {}
  bool operator!=(const MetricsEstimationFunctor &a) const
  {
      return (this->date_vect != a.date_vect);
  }

  bool operator==(const MetricsEstimationFunctor & other) const
  {
    return !( *this != other );
  }

  void SetDates(VectorType &idDates)
  {
      date_vect = idDates;
  }

  inline TOutput operator()(const TInput1 & A) const
  {
      //itk::VariableLengthVector vect;
    /*
     // TODO: Uncomment this if the approximation is needed and use in pheno_metrix x_hat instead of ts
    int nbBvElems = A.GetNumberOfElements();

    VectorType ts(nbBvElems);
    int i;
    for(i = 0; i<nbBvElems; i++) {
        ts[i] = A[i];
    }

    auto approximation_result =
      pheno::normalized_sigmoid::TwoCycleApproximation(ts, date_vect);
    auto princ_cycle = std::get<1>(approximation_result);
    auto x_hat = std::get<0>(princ_cycle);
*/

    double dgx0, t0, t1, t2, t3, dgx2;
    std::tie(dgx0, t0, t1, t2, t3, dgx2) =
        pheno::normalized_sigmoid::pheno_metrics<double>(A);
        
    TOutput result(6);
    result[0] = dgx0;
    result[1] = t0;
    result[2] = t1;
    result[3] = t2;
    result[4] = t3;
    result[5] = dgx2;

    return result;
  }
private:
  // input dates vector
  VectorType date_vect;
};

class MetricsEstimation : public Application
{
public:
  /** Standard class typedefs. */
  typedef MetricsEstimation               Self;
  typedef Application                   Superclass;
  typedef itk::SmartPointer<Self>       Pointer;
  typedef itk::SmartPointer<const Self> ConstPointer;

  typedef float                                   PixelType;
  typedef FloatVectorImageType                    InputImageType;
  typedef FloatVectorImageType                    OutImageType;

  typedef MetricsEstimationFunctor <InputImageType::PixelType,
                                    InputImageType::PixelType,
                                    OutImageType::PixelType>                FunctorType;

  typedef itk::UnaryFunctorImageFilter<InputImageType,
                                        OutImageType, FunctorType> FilterType;

  /** Standard macro */
  itkNewMacro(Self);

  itkTypeMacro(MetricsEstimation, otb::Application);

private:
  void DoInit()
  {

    SetName("MetricsEstimation");
    SetDescription("Reprocess a BV time profile.");
   
    AddParameter(ParameterType_InputImage, "ipf", "Input profile file.");
    SetParameterDescription( "ipf", "Input file containing the profile to process. This is an ASCII file where each line contains the date (YYYMMDD) the BV estimation and the error." );

    AddParameter(ParameterType_InputFilenameList, "ilxml", "The XML metadata files list");

    AddParameter(ParameterType_OutputImage, "opf", "Output profile file.");
    SetParameterDescription( "opf", "Filename where the reprocessed profile saved. This is an raster band contains the new BV estimation value for each pixel. The last band contains the boolean information which is 0 if the value has not been reprocessed." );
  }

  void DoUpdateParameters()
  {
    //std::cout << "MetricsEstimation::DoUpdateParameters" << std::endl;
  }

  void DoExecute()
  {
      FloatVectorImageType::Pointer ipf_image = this->GetParameterImage("ipf");
      std::vector<std::string> xmlsList = this->GetParameterStringList("ilxml");
      unsigned int nb_ipf_bands = ipf_image->GetNumberOfComponentsPerPixel();
      unsigned int nb_xmls = xmlsList.size();
      if((nb_ipf_bands == 0) || (nb_ipf_bands != nb_xmls)) {
          itkExceptionMacro("Invalid number of bands or xmls: ipf bands=" <<
                            nb_ipf_bands << ", nb_xmls=" << nb_xmls);
      }

      VectorType date_vec{};
      std::string date_str;
      for (std::string strXml : xmlsList)
      {
          MetadataHelperFactory::Pointer factory = MetadataHelperFactory::New();
          // we are interested only in the 10m resolution as we need only the date
          auto pHelper = factory->GetMetadataHelper(strXml, 10);
          date_str = pHelper->GetAcquisitionDate();
          date_vec.push_back(date_to_doy(date_str));
      }

      //instantiate a functor with the regressor and pass it to the
      //unary functor image filter pass also the normalization values
      m_MetricsEstimationFilter = FilterType::New();
      m_functor.SetDates(date_vec);

      m_MetricsEstimationFilter->SetFunctor(m_functor);
      m_MetricsEstimationFilter->SetInput(ipf_image);
      m_MetricsEstimationFilter->UpdateOutputInformation();
      m_MetricsEstimationFilter->GetOutput()->SetNumberOfComponentsPerPixel(6);
      SetParameterOutputImage("opf", m_MetricsEstimationFilter->GetOutput());

  }

  FilterType::Pointer m_MetricsEstimationFilter;
  FunctorType m_functor;
};
}
}

OTB_APPLICATION_EXPORT(otb::Wrapper::MetricsEstimation)