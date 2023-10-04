/*************************************************************************
 *
 * lwString.h, v 1.0, 2013 (edited 2018)
 *
 * Copyright (C) Thomas De Maet <demaet@gmail.com>
 * partially from Evan Teran, stackoverflow
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all cop
 
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND WITHOUT ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
 * MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS AND
 * CONTRIBUTORS ACCEPT NO RESPONSIBILITY IN ANY CONCEIVABLE MANNER.
 *
 ************************************************************************/

#ifndef _LW_STRING
#define _LW_STRING

#include <string>
#include <sstream>
#include <vector>
#include <cstdlib>      /* atof */
#include "lwScalar.h"

namespace lwString {

  void split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
  }

  std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    split(s, delim, elems);
    return elems;
  }

  template <class SCALAR>
  std::vector<SCALAR> splitNum(const std::string &s, char delim) {
    std::vector<std::string> elems = split(s, delim);
    std::vector<SCALAR> vals(elems.size());
    if(lwScalar::isInteger<SCALAR>())
      for(int i = 0; i < elems.size(); i++)
        vals[i] = atoi(elems[i].c_str());
    else
      for(int i = 0; i < elems.size(); i++)
        vals[i] = atof(elems[i].c_str());
    return vals;
  }

  std::string replace(const std::string& str, const std::string& from, const std::string& to) {
    if(from.empty())
      return str;
    size_t pos = str.find(from);
    if(pos == std::string::npos)
      return str;
    std::string out = str;
    out.replace(pos, from.length(), to);
    return out;
  }

  std::string replace(const char* str, const std::string& from, const std:: string& to) {
    std::string stdStr(str);
    return replace(stdStr, from, to);
  }

  void concat(const std::string &str1, const std::string &str2, std::string &str) {
    str = str1 + str2;
  }
  void concat(std::string &str, const std::string &str2) {
    str += str2;
  }

  std::string int_to_string( int n ) {
      std::ostringstream stm ;
      stm << n ;
      return stm.str() ;
  }

  template <class SCALAR>
  SCALAR toNum(const std::string &s) {
    return atof(s.c_str());
  }

}

#endif

