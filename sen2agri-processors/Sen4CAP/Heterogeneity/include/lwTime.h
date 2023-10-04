/*************************************************************************
 *
 * lwTime.h, v 1.0, 2013
 *
 * Copyright (C) Thomas De Maet <demaet@gmail.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND WITHOUT ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
 * MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS AND
 * CONTRIBUTORS ACCEPT NO RESPONSIBILITY IN ANY CONCEIVABLE MANNER.
 *
 ************************************************************************/

#ifndef _LW_TIME
#define _LW_TIME

#include <stdio.h>
#include <string>
#include <math.h>
#include "predef.h"

#if defined(PREDEF_PLATFORM_UNIX) || defined(PREDEF_OS_MACOSX)
#  define PREDEF_PLATFORM_UNIX
#  include <sys/time.h>
#elif PREDEF_PLATFORM_WIN32
#  include <WinBase.h>
#else
#  error "Platform unknown ! (see lwTime.h)"
#endif

// give the current time with good precision on UNIX and WINDOWS
// get() is used to measure the process time with a second get() at the end of the process
// toString(duration) is used to transform a second-based duration into human-readable time
namespace lwTime {

  std::string toString(double duration) {
    char output[100];
    sprintf(output, "%01.0f:%02.0f:%02.2f", 
                    floor(duration / 3600.), floor(fmod(duration, 3600.) / 60.), fmod(duration, 60.) );
    return std::string(output);
  }

  double get() {
#ifdef PREDEF_PLATFORM_UNIX
    struct timeval t;
    gettimeofday(&t, NULL);
    return t.tv_sec + (t.tv_usec / 1000000.);
#elif PREDEF_PLATFORM_WIN32
    ULONGLONG tickCount = GetTickCount64();
    return tickCount / 1000.;
#endif
  }

}


#endif
