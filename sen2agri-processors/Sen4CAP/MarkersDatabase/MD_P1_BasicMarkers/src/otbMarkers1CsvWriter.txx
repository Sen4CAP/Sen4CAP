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
#include "itksys/SystemTools.hxx"
#include "otb_tinyxml.h"
#include "otbStringUtils.h"
#include "CommonFunctions.h"

namespace otb {

template < class TMeasurementVector >
Markers1CsvWriter<TMeasurementVector>
::Markers1CsvWriter(): m_TargetFileName(""),
    m_bUseMinMax(false)
{
    m_IdPosInHeader = -1;
    m_MeanPosInHeader = -1;
    m_StdevPosInHeader = -1;
    m_MinPosInHeader = -1;
    m_MaxPosInHeader = -1;
    m_ValidPixelsPosInHeader = -1;
    m_InvalidPixelsPosInHeader = -1;
    m_csvSeparator = ',';
}

template < class TMeasurementVector >
std::string
Markers1CsvWriter<TMeasurementVector>
::BuildHeaderItem(const std::string &dateStr, const std::string &hdrItem, const std::string &fileType,
                  const std::string &polarisation, const std::string &orbit)
{
    std::string ret = dateStr + "_" + hdrItem + "_" + fileType;
    if (polarisation.size() > 0) {
        ret += ("_" + polarisation);
    }
    if (orbit.size() > 0) {
        ret += ("_" + orbit);
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
    time_t fileDate;
    time_t additionalFileDate = 0;
    if (!GetFileInfosFromName(fileName, fileType, polarisation, orbit, fileDate, additionalFileDate))
    {
        std::cout << "Error extracting file informations from file name " << fileName << std::endl;
        return;
    }
    char buffer[10];
    struct tm * timeinfo = localtime(&fileDate);
    strftime(buffer, sizeof(buffer), "%Y%m%d", timeinfo);
    const std::string &dateStr = std::string(buffer);

    m_idFieldName = idFieldName;
    m_bIdIsInteger = bIdIsInteger;
    for (const auto &hdrItem: vec) {
        if (hdrItem == idFieldName) {
            this->m_HeaderFields.push_back(hdrItem);
        } else {
            this->m_HeaderFields.push_back(BuildHeaderItem(dateStr, hdrItem, fileType, polarisation, orbit));
        }
    }
    m_IdPosInHeader = GetPositionInHeader(idFieldName);
    m_MeanPosInHeader = GetPositionInHeader(BuildHeaderItem(dateStr, "mean",  fileType, polarisation, orbit));
    m_StdevPosInHeader = GetPositionInHeader(BuildHeaderItem(dateStr, "stdev",  fileType, polarisation, orbit));
    m_MinPosInHeader = GetPositionInHeader(BuildHeaderItem(dateStr, "min",  fileType, polarisation, orbit));
    m_MaxPosInHeader = GetPositionInHeader(BuildHeaderItem(dateStr, "max",  fileType, polarisation, orbit));
    m_ValidPixelsPosInHeader = GetPositionInHeader(BuildHeaderItem(dateStr, "valid_pixels_cnt",  fileType, polarisation, orbit));
    m_InvalidPixelsPosInHeader = GetPositionInHeader(BuildHeaderItem(dateStr, "invalid_pixels_cnt",  fileType, polarisation, orbit));
}

template < class TMeasurementVector >
template <typename MapType, typename MapMinMaxType>
void
Markers1CsvWriter<TMeasurementVector>
::AddInputMap(const std::string &fileName, const MapType& map, const MapMinMaxType& mapMins, const MapMinMaxType& mapMax,
              const MapMinMaxType& mapValidPixels, const MapMinMaxType& mapInvalidPixels)
{
    std::string fileType;
    std::string polarisation;
    std::string orbit;
    time_t fileDate;
    time_t additionalFileDate = 0;
    if (!GetFileInfosFromName(fileName, fileType, polarisation, orbit, fileDate, additionalFileDate))
    {
        std::cout << "Error extracting file informations from file name " << fileName << std::endl;
        return;
    }
    if (additionalFileDate) {
        fileDate = additionalFileDate;
    }

    // We ensure that we have the same number of values for these maps as in the input map
    bool useMinMax = false;
    if (m_bUseMinMax) {
        if (map.size() == mapMins.size() && map.size() == mapMax.size() &&
                map.size() == mapValidPixels.size() &&
                map.size() == mapInvalidPixels.size()) {
            useMinMax = true;
        }
    }

    typename MapType::const_iterator it;
    typename MapMinMaxType::const_iterator itMin = mapMins.begin();
    typename MapMinMaxType::const_iterator itMax = mapMax.begin();
    typename MapMinMaxType::const_iterator itValidPixelsCnt = mapValidPixels.begin();
    typename MapMinMaxType::const_iterator itInvalidPixelsCnt = mapInvalidPixels.begin();

    std::string fieldId;
    for ( it = map.begin() ; it != map.end() ; ++it)
    {
      fieldId = boost::lexical_cast<std::string>(it->first);
      const auto &meanVal = it->second.mean[0];
      const auto &stdDevVal = it->second.stdDev[0];

      FieldEntriesType fieldEntry;
      // we exclude from the header the fid
      fieldEntry.values.resize(m_HeaderFields.size() - 1);
      // exclude header so subtract 1 from the pos in header
      fieldEntry.values[m_MeanPosInHeader-1] = meanVal;
      fieldEntry.values[m_StdevPosInHeader-1] = stdDevVal;
      if (useMinMax && m_MinPosInHeader > 0 && m_MaxPosInHeader > 0 &&
              m_ValidPixelsPosInHeader > 0 && m_InvalidPixelsPosInHeader > 0) {
          fieldEntry.values[m_MinPosInHeader-1] = itMin->second[0];
          fieldEntry.values[m_MaxPosInHeader-1] = itMax->second[0];
          fieldEntry.values[m_ValidPixelsPosInHeader-1] = itValidPixelsCnt->second[0];
          fieldEntry.values[m_InvalidPixelsPosInHeader-1] = itInvalidPixelsCnt->second[0];
      }

      // add it into the container
      FileFieldsInfoType fileFieldsInfoType;
      fileFieldsInfoType.fid = fieldId;
      fileFieldsInfoType.fieldsEntries.push_back(fieldEntry);

      m_FileFieldsContainer.push_back(fileFieldsInfoType);
      if (useMinMax) {
          ++itMin;
          ++itMax;
          ++itValidPixelsCnt;
          ++itInvalidPixelsCnt;
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
    for (int i = 0; i<fieldEntriesSize; i++) {
        const std::vector<double> &curLineVect = fileFieldsInfos.fieldsEntries[i].values;
        for (int j = 0; j<curLineVect.size(); j++) {
            outStream << DoubleToString(curLineVect[j]).c_str();
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
    for (int i = 0; i<m_HeaderFields.size(); i++) {
        fileStream << m_HeaderFields[i];
        if (i < m_HeaderFields.size()-1) {
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
    // check if the name is found in the headers list
    std::vector<std::string>::iterator hdrIt = std::find(m_HeaderFields.begin(), m_HeaderFields.end(), name);
    if (hdrIt == m_HeaderFields.end())
    {
        return -1;
    }
    // we exclude from the header the fid and the date
    return hdrIt - m_HeaderFields.begin();
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
