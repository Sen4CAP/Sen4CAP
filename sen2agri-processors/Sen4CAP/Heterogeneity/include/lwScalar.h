/*************************************************************************
 *
 * lwScalar.h, v 1.0, 2013
 *
 * Copyright (C) Thomas De Maet <demaet@gmail.com>
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

#ifndef _LW_SCALAR
#define _LW_SCALAR

#include <limits>


namespace lwScalar {

  template <class SCALAR>
  SCALAR getNaN() {
    std::numeric_limits<SCALAR> limits;
    SCALAR nan_value;
    if(limits.has_quiet_NaN)
      nan_value = limits.quiet_NaN();
    else if(limits.has_signaling_NaN)
      nan_value = limits.signaling_NaN();
    else
      nan_value = limits.max();
    return nan_value;
  }

  template <class SCALAR>
  bool isInteger() {
    std::numeric_limits<SCALAR> limits;
    return limits.is_integer;
  }

  template <class SCALAR>
  SCALAR min() {
    std::numeric_limits<SCALAR> limits;
    return limits.min();
  }

  template <class SCALAR>
  SCALAR max() {
    std::numeric_limits<SCALAR> limits;
    SCALAR max = limits.max();
    return (getNaN<SCALAR>() == max) ? (max - limits.epsilon()) : max;
  }
}

#endif
