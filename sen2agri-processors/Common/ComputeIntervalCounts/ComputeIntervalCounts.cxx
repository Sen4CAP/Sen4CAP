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

//#include "otbStreamingHistogramImageFilter.h"

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
#include "otbStreamingHistogramImageFilter.h"

#include <algorithm>
#include <tuple>

namespace otb
{
namespace Wrapper
{

class ComputeIntervalCounts : public Application
{
public:
/** Standard class typedefs. */
typedef ComputeIntervalCounts            Self;
typedef Application                   Superclass;
typedef itk::SmartPointer<Self>       Pointer;
typedef itk::SmartPointer<const Self> ConstPointer;

typedef Int32ImageType                                                         ImageType;
typedef otb::StreamingHistogramImageFilter<ImageType>                          StreamingHistogramFilterType;
typedef struct {int min; int max; }                                            StatusInterval;

/** Standard macro */
itkNewMacro(Self)
itkTypeMacro(ComputeIntervalCounts, otb::Application)

private:

void DoInit() override
{
  SetName( "ComputeIntervalCounts" );
  SetDescription( "Computes the class counts on a labelled image" );

  AddParameter( ParameterType_InputImage , "in" ,  "Input Image" );
  SetParameterDescription( "in" , "The input image to be filtered." );

  AddParameter( ParameterType_OutputFilename, "out" ,  "Output File" );
  SetParameterDescription( "out" , "Output CSV file with class counts." );

  AddParameter( ParameterType_StringList, "intervals" ,  "Intervals as a simple list of values" );
  SetParameterDescription( "intervals" , "Intervals as a simple list of values." );

  AddParameter( ParameterType_StringList, "labels" ,  "Intervals labels corresponding to the provided intervals" );
  SetParameterDescription( "labels" , "Intervals labels corresponding to the provided intervals." );


  AddRAMParameter();

  SetDocExampleParameterValue("in", "qb_RoadExtract.tif");
  SetDocExampleParameterValue("out", "classes.csv");
}

void DoUpdateParameters() override
{
}

void DoExecute() override
{
  Int32ImageType::Pointer inImage = GetParameterInt32Image("in");

  m_HistogramFilter = StreamingHistogramFilterType::New();
  m_HistogramFilter->SetInput(inImage);

  const std::vector<std::string> &intervalStrs = GetParameterStringList("intervals");
  const std::vector<std::string> &labels = GetParameterStringList("labels");
  if (intervalStrs.size() != labels.size()) {
      otbAppLogFATAL(<<"Interval and label lists should have the same size");
  }
  std::vector<int> intervals;
  int prevVal = INT_MIN;
  int curVal;
  std::map<int, std::string> mapLabels;
  int i = 0;
  for (const auto &val : intervalStrs) {
      curVal = std::atoi(val.c_str());
      if (curVal <= prevVal) {
          otbAppLogFATAL(<<"Interval values should be unique and provided sorted. The provided value "
                         << val << " is smaller or equal than " << prevVal);
      }
      intervals.push_back(curVal);
      mapLabels[i] = labels[i];
      prevVal = curVal;
      i++;
  }
  m_HistogramFilter->SetIntervalsLimits(intervals);

  m_HistogramFilter->Update();

  const auto &populationMap = m_HistogramFilter->GetLabelPopulationMap();

  std::vector<std::tuple<int32_t, std::string, uint64_t>> counts;
  counts.reserve(populationMap.size());

  uint64_t totalPixCnt;
  for (auto it = populationMap.begin(); it != populationMap.end(); ++it) {
    counts.emplace_back(std::make_tuple(it->first, mapLabels[it->first], it->second));
    totalPixCnt += it->second;
  }

  std::sort(counts.begin(), counts.end());

  std::ofstream fout(GetParameterString("out"));
  fout << "thresholdIdx" << ',' << "threshold" << ',' << "label" << ',' << "count" << ',' << "percentage" << ',' << "status" << '\n';
  float percentage;
  for (const auto &p : counts) {
      percentage = (((float)std::get<2>(p)) * 100 / totalPixCnt);
      fout << std::get<0>(p) << ',' << intervals[std::get<0>(p)] << ',' << std::get<1>(p) << ',' << std::get<2>(p) <<
            ',' << percentage << ',' << ComputeStatus(percentage) << '\n';
  }
}

std::string ComputeStatus(float percentage) {
    for (auto const& pair : StatusIntervals) {
        if (percentage >= pair.second.min && percentage <= pair.second.max) {
            return pair.first;
        }
    }
    return "Not available";
}

StreamingHistogramFilterType::Pointer              m_HistogramFilter;
std::map<std::string, StatusInterval>       StatusIntervals = {
    {"Very Low", {0, 20}},
    {"Low", {21, 40}},
    {"Medium", {41, 50}},
    {"High", {60, 80}},
    {"Very High", {81, 100}},
};

};
}
}

OTB_APPLICATION_EXPORT(otb::Wrapper::ComputeIntervalCounts)
