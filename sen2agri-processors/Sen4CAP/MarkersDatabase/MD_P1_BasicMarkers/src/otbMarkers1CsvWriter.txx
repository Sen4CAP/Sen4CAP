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
    m_bUseMinMax(false), m_bUseValidityCnt(false),
    m_bUseMedian(false), m_bUseP25(false), m_bUseP75(false)
{
    m_IdPosInHeader = -1;
    m_MeanPosInHeader = -1;
    m_StdevPosInHeader = -1;
    m_MinPosInHeader = -1;
    m_MaxPosInHeader = -1;
    m_ValidPixelsPosInHeader = -1;
    m_MedianPosInHeader = -1;
    m_P25PosInHeader = -1;
    m_P75PosInHeader = -1;
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
    int idx = 0;
    for (const auto &hdrItem: vec) {
        isFieldNameHdrItem = (hdrItem == idFieldName);
        bool isIntValHdr = (hdrItem == "valid_pixels_cnt");
        const HeaderInfoType &hdrInfo = isFieldNameHdrItem ?
                    HeaderInfoType(hdrItem, hdrItem, m_bIdIsInteger, true, idx) :
                    HeaderInfoType(BuildHeaderItem(dateStr, hdrItem, fileType, headerAdditionalFields),
                                   hdrItem, isIntValHdr, false, idx);
        m_vecHeaderFields.emplace_back(hdrInfo);
        idx++;
    }
}

template < class TMeasurementVector >
template <typename MapType, typename MapMinMaxType>
void
Markers1CsvWriter<TMeasurementVector>
::AddInputMap(const std::map<std::string, const MapType*> &map, const std::map<std::string, const MapMinMaxType*> &mapOptionals)
{
    std::vector<ColumnToValuesInfo<const MapType, const MapMinMaxType>> columnToVals;
    for (const HeaderInfoType &hdrField: m_vecHeaderFields) {
        if (!hdrField.IsFieldIdColumn() && hdrField.GetPositionInHeader() > 0) {
            columnToVals.push_back(ColumnToValuesInfo<const MapType, const MapMinMaxType>(hdrField, map, mapOptionals));
        }
    }

    const MapType* mapMean = map.find("mean")->second;
    typename MapType::const_iterator it;
    for ( it = mapMean->begin() ; it != mapMean->end() ; ++it)
    {
      FieldEntriesType fieldEntry;
      // we exclude from the header the fid
      fieldEntry.values.resize(m_vecHeaderFields.size() - 1);
      // exclude header so subtract 1 from the pos in header
      for (auto &colVals: columnToVals) {
          if (!colVals.hdrItemInfo.IsFieldIdColumn()) {
              fieldEntry.values[colVals.hdrItemInfo.GetPositionInHeader()-1] = colVals.GetNextValue(m_mapValuesIndex);
          }
      }

      // add it into the container
      FileFieldsInfoType fileFieldsInfoType;
      fileFieldsInfoType.fid = boost::lexical_cast<std::string>(it->first);
      fileFieldsInfoType.fieldsEntries.push_back(fieldEntry);

      m_FileFieldsContainer.push_back(fileFieldsInfoType);
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
    bool isIntValue;
    int curHdrItemIdx;
    int hdrVecSize = m_vecHeaderFields.size();  // do not consider the id
    for (int i = 0; i<fieldEntriesSize; i++) {
        const std::vector<double> &curLineVect = fileFieldsInfos.fieldsEntries[i].values;
        for (int j = 0; j<curLineVect.size(); j++) {
            isIntValue = false;
            curHdrItemIdx = j+1;
            if (curHdrItemIdx < hdrVecSize) {
                const HeaderInfoType &hdrItem = m_vecHeaderFields[curHdrItemIdx];
                isIntValue = hdrItem.IsIntegerValue();
            }
            if (!isIntValue) {
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
        fileStream << m_vecHeaderFields[i].GetFullColumnName();
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
        return hdrItem.GetFullColumnName() == name;
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
