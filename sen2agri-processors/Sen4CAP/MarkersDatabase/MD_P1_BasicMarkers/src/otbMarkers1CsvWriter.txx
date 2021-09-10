/*
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

#ifndef otbMarkers1CsvWriter_txx
#define otbMarkers1CsvWriter_txx

#include "otbMarkers1CsvWriter.h"
#include "itkMacro.h"
#include "otbWrapperMacros.h"
#include "itksys/SystemTools.hxx"
#include "otb_tinyxml.h"
#include "otbStringUtils.h"
#include "CommonFunctions.h"

namespace otb {

template < class TMeasurementVector >
Markers1CsvWriter<TMeasurementVector>
::Markers1CsvWriter(): m_TargetFileName(""),
    m_bUseMinMax(false), m_bUseValidityCnt(false)
{
    m_IdPosInHeader = -1;
    m_MeanPosInHeader = -1;
    m_StdevPosInHeader = -1;
    m_MinPosInHeader = -1;
    m_MaxPosInHeader = -1;
    m_ValidPixelsPosInHeader = -1;
    m_csvSeparator = ',';
    m_mapValuesIndex = 0;
}

template < class TMeasurementVector >
std::string
Markers1CsvWriter<TMeasurementVector>
::BuildHeaderItem(const std::string &dateStr, const std::string &hdrItem, const std::string &fileType,
                  const std::vector<std::string> &additionalFields)
{
    std::string ret = dateStr + "_" + hdrItem;
    if (fileType.size() > 0) {
        ret += ("_" + fileType);
    }
    for (const std::string &addField: additionalFields) {
        if (addField.size() > 0) {
            ret += ("_" + addField);
        }
    }

    return ret;
}

template < class TMeasurementVector >
void
Markers1CsvWriter<TMeasurementVector>
::SetHeaderFields(const std::string &fileName, const StringVectorType &vec,
                  const std::string &idFieldName, bool bIdIsInteger)
{
    std::string fileType;
    std::string polarisation;
    std::string orbit;
    std::string tile;
    std::string band;
    time_t fileDate;
    time_t prevDate = 0;
    Satellite sat;
    if (!GetFileInfosFromName(fileName, sat, fileType, polarisation, orbit, fileDate, tile, band, prevDate))
    {
        std::cout << "Error extracting file informations from file name " << fileName << std::endl;
        fileType = m_defaultPrdType;
    }
    std::vector<std::string> headerAdditionalFields = {polarisation, orbit, tile, band};
    char buffer[10];
    struct tm * timeinfo = localtime(&fileDate);
    strftime(buffer, sizeof(buffer), "%Y%m%d", timeinfo);
    const std::string &dateStr = std::string(buffer);

    m_bIdIsInteger = bIdIsInteger;
    bool isFieldNameHdrItem;
    for (const auto &hdrItem: vec) {
        isFieldNameHdrItem = (hdrItem == idFieldName);
        bool isDoubleValHdr = (!isFieldNameHdrItem && (hdrItem == "mean" || hdrItem == "stdev"));
        const HeaderInfoType &hdrInfo = isFieldNameHdrItem ?
                    HeaderInfoType(hdrItem, m_bIdIsInteger) :
                    HeaderInfoType(BuildHeaderItem(dateStr, hdrItem, fileType, headerAdditionalFields),
                                   isDoubleValHdr);
        m_vecHeaderFields.emplace_back(hdrInfo);
    }
    m_IdPosInHeader = GetPositionInHeader(idFieldName);
    m_MeanPosInHeader = GetPositionInHeader(BuildHeaderItem(dateStr, "mean",  fileType, headerAdditionalFields));
    m_StdevPosInHeader = GetPositionInHeader(BuildHeaderItem(dateStr, "stdev",  fileType, headerAdditionalFields));
    m_MinPosInHeader = GetPositionInHeader(BuildHeaderItem(dateStr, "min",  fileType, headerAdditionalFields));
    m_MaxPosInHeader = GetPositionInHeader(BuildHeaderItem(dateStr, "max",  fileType, headerAdditionalFields));
    m_ValidPixelsPosInHeader = GetPositionInHeader(BuildHeaderItem(dateStr, "valid_pixels_cnt",  fileType, headerAdditionalFields));
}

template < class TMeasurementVector >
template <typename MapType, typename MapMinMaxType>
void
Markers1CsvWriter<TMeasurementVector>
::AddInputMap(const MapType& map, const MapMinMaxType& mapMins, const MapMinMaxType& mapMax,
              const MapMinMaxType& mapValidPixels)
{
    // We ensure that we have the same number of values for these maps as in the input map
    bool useStdev = false;
    if (m_bUseStdev && m_StdevPosInHeader > 0) {
        useStdev = true;
    }

    bool useMinMax = false;
    if (m_bUseMinMax) {
        if (map.size() == mapMins.size() && map.size() == mapMax.size() &&
                m_MinPosInHeader > 0 && m_MaxPosInHeader > 0) {
            useMinMax = true;
        }
    }
    bool useValidityCnt = false;
    if (m_bUseValidityCnt) {
        if (map.size() == mapValidPixels.size() && m_ValidPixelsPosInHeader > 0) {
            useValidityCnt = true;
        }
    }

    typename MapType::const_iterator it;
    typename MapMinMaxType::const_iterator itMin = mapMins.begin();
    typename MapMinMaxType::const_iterator itMax = mapMax.begin();
    typename MapMinMaxType::const_iterator itValidPixelsCnt = mapValidPixels.begin();

    std::string fieldId;
    for ( it = map.begin() ; it != map.end() ; ++it)
    {
      fieldId = boost::lexical_cast<std::string>(it->first);
      const auto &meanVal = it->second.mean[m_mapValuesIndex];

      FieldEntriesType fieldEntry;
      // we exclude from the header the fid
      fieldEntry.values.resize(m_vecHeaderFields.size() - 1);
      // exclude header so subtract 1 from the pos in header
      fieldEntry.values[m_MeanPosInHeader-1] = meanVal;
      if (useStdev) {
          const auto &stdDevVal = it->second.stdDev[m_mapValuesIndex];
          fieldEntry.values[m_StdevPosInHeader-1] = stdDevVal;
      }
      if (useMinMax) {
          fieldEntry.values[m_MinPosInHeader-1] = itMin->second[m_mapValuesIndex];
          fieldEntry.values[m_MaxPosInHeader-1] = itMax->second[m_mapValuesIndex];
      }
      if (useValidityCnt) {
          fieldEntry.values[m_ValidPixelsPosInHeader-1] = itValidPixelsCnt->second[m_mapValuesIndex];
      }

      // add it into the container
      FileFieldsInfoType fileFieldsInfoType;
      fileFieldsInfoType.fid = fieldId;
      fileFieldsInfoType.fieldsEntries.push_back(fieldEntry);

      m_FileFieldsContainer.push_back(fileFieldsInfoType);
      if (useMinMax) {
          ++itMin;
          ++itMax;
      }
      if (useValidityCnt) {
          ++itValidPixelsCnt;
      }
    }
}

template < class TMeasurementVector >
void
Markers1CsvWriter<TMeasurementVector>
::WriteOutputCsvFormat()
{
    std::ofstream fileStream;
    fileStream.open(m_TargetFileName, std::ios_base::trunc | std::ios_base::out);
    if (!fileStream.is_open()) {
        itkExceptionMacro(<<"Cannot open file " << m_TargetFileName << " for writing. Please check if output folder exists or has the needed rights!");
    }

    // write the header
    WriteCsvHeader(fileStream);

    // now sort the resVect
    m_FieldsComparator.SetIsInt(m_bIdIsInteger);
    std::sort (m_FileFieldsContainer.begin(), m_FileFieldsContainer.end(), m_FieldsComparator);


    typename FileFieldsContainer::iterator fileFieldContainerIt;
    for (fileFieldContainerIt = m_FileFieldsContainer.begin(); fileFieldContainerIt != m_FileFieldsContainer.end();
        ++fileFieldContainerIt)
    {
        WriteEntriesToCsvOutputFile(fileStream, *fileFieldContainerIt);
    }
    fileStream.flush();
    fileStream.close();
}

template < class TMeasurementVector >
void
Markers1CsvWriter<TMeasurementVector>
::WriteEntriesToCsvOutputFile(std::ofstream &outStream, const FileFieldsInfoType &fileFieldsInfos)
{
    // Remove the duplicate items
    // RemoveDuplicates(fileFieldsInfos.fieldsEntries);

    outStream << fileFieldsInfos.fid.c_str() << m_csvSeparator;
    int fieldEntriesSize = fileFieldsInfos.fieldsEntries.size();
    bool isDoubleFieldVal;
    int curHdrItemIdx;
    int hdrVecSize = m_vecHeaderFields.size();  // do not consider the id
    for (int i = 0; i<fieldEntriesSize; i++) {
        const std::vector<double> &curLineVect = fileFieldsInfos.fieldsEntries[i].values;
        for (int j = 0; j<curLineVect.size(); j++) {
            isDoubleFieldVal = true;
            curHdrItemIdx = j+1;
            if (curHdrItemIdx < hdrVecSize) {
                const HeaderInfoType &hdrItem = m_vecHeaderFields[curHdrItemIdx];
                isDoubleFieldVal = hdrItem.IsDouble();
            }
            if (isDoubleFieldVal) {
                outStream << DoubleToString(curLineVect[j]).c_str();
            } else {
                outStream << (int)curLineVect[j];
            }
            if (j < curLineVect.size() - 1 ) {
                outStream << m_csvSeparator;
            }
        }
        outStream << "\n";
    }
}

//template < class TMeasurementVector >
//void
//Markers1CsvWriter<TMeasurementVector>
//::RemoveDuplicates(std::vector<FieldEntriesType> &fieldsEntries) {

//    auto comp = [this] ( const FieldEntriesType& lhs, const FieldEntriesType& rhs ) {
//        // TODO: For now we keep the ones that have different mean values
//        return ((lhs.date == rhs.date) && (lhs.additionalFileDate == rhs.additionalFileDate)/* &&
//                (lhs.values[m_MeanPosInHeader] == rhs.values[m_MeanPosInHeader])*/);};

//    auto pred = []( const FieldEntriesType& lhs, const FieldEntriesType& rhs ) {return (lhs.date < rhs.date);};
//    std::sort(fieldsEntries.begin(), fieldsEntries.end(), pred);
//    auto last = std::unique(fieldsEntries.begin(), fieldsEntries.end(), comp);
//    fieldsEntries.erase(last, fieldsEntries.end());
//}


template < class TMeasurementVector >
void
Markers1CsvWriter<TMeasurementVector>
::WriteCsvHeader(std::ofstream &fileStream) {
    for (int i = 0; i<m_vecHeaderFields.size(); i++) {
        fileStream << m_vecHeaderFields[i].GetName();
        if (i < m_vecHeaderFields.size()-1) {
            fileStream << m_csvSeparator;
        }
    }
    fileStream << "\n";
}

template < class TMeasurementVector >
void
Markers1CsvWriter<TMeasurementVector>
::GenerateData()
{
    // Check if the input are not null
    if(m_FileFieldsContainer.size() == 0) {
        std::cout << "At least one input is required, please set input using the methods AddInputMap" << std::endl;
        // Commented to allow writing of even empty files (containing only header), just output the warning
//        return;
    }

    // Check if the filename is not empty
    if(m_TargetFileName.empty()) {
        itkExceptionMacro(<<"The output directory TargetDir is empty, please set the target dir name via the method SetTargetDir");
    }

    WriteOutputCsvFormat();
}

template < class TMeasurementVector >
void
Markers1CsvWriter<TMeasurementVector>
::CleanInputs()
{
}

template < class TMeasurementVector >
int
Markers1CsvWriter<TMeasurementVector>
::GetPositionInHeader(const std::string &name)
{
    auto pred = [name](const HeaderInfoType &hdrItem) {
        return hdrItem.GetName() == name;
    };
    // check if the name is found in the headers list
    typename std::vector<HeaderInfoType>::const_iterator hdrIt =std::find_if(std::begin(m_vecHeaderFields), std::end(m_vecHeaderFields), pred);
    if (hdrIt == m_vecHeaderFields.end())
    {
        return -1;
    }
    // we exclude from the header the fid and the date
    return hdrIt - m_vecHeaderFields.begin();
}

template < class TMeasurementVector >
void
Markers1CsvWriter<TMeasurementVector>
::PrintSelf(std::ostream& os, itk::Indent indent) const
{
  // Call superclass implementation
  Superclass::PrintSelf(os, indent);

//  // Print Writer state
//  os << indent << "Output FileName: "<< m_FileName << std::endl;
}

} // End namespace otb

#endif
