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

/*
 * Default (no-op) logging hook for the header-only `reactor` library.
 *
 *   - L_EXC(...)  logs an exception the runtime swallows to keep a loop alive
 *                 (today: a UdpServer Datagram handler that threw). Called from
 *                 inside the catch block, so the hook can inspect the in-flight
 *                 exception. No-op by default.
 *
 * To plug in real logging (as Xapiand does for its other libraries), either:
 *
 *   1. Define REACTOR_TRACE_HEADER to the path of a header that defines it,
 *        c++ -DREACTOR_TRACE_HEADER='"my_trace.h"' ...
 *      and the reactor headers include that instead of this file, or
 *
 *   2. Define the macro before including the reactor headers.
 *
 * The macro is `#ifndef`-guarded, so an L_EXC already defined by the consumer
 * (or by a sibling Kronuz library's trace header) wins.
 */

#pragma once

#ifndef L_EXC
#define L_EXC(...) ((void)0)
#endif
