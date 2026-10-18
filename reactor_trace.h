/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// The reactor's logging seam: L_EXC reports an exception the runtime swallows to keep a
// loop alive (today: a UdpServer Datagram handler that threw). No-op by default.
//
// Contract for a consumer's L_EXC:
//   1. It takes one string literal and no other arguments, so fmt- and printf-style
//      hooks both fit.
//   2. It runs inside the catch block, with the exception in flight
//      (std::current_exception()).
//   3. It must not throw; the loop drops anything it throws.
//   4. A captured exception may view the datagram's bytes, which the next receive
//      overwrites; an asynchronous logger must not read such views later.
//   5. Every translation unit must see the same definition (receive_loop is inline, so
//      differing ones violate the ODR). Set REACTOR_TRACE_HEADER target-wide, e.g.
//        -DREACTOR_TRACE_HEADER='"my_trace.h"'
//      reactor_udp.h includes that header, then this one, which fills in whatever it
//      leaves undefined.

#pragma once

#ifndef L_EXC
#define L_EXC(...) ((void)0)
#endif
