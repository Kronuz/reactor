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

// reactor::tls: incomplete options for each role are refused, and client connections bind
// IP literals and DNS names but refuse empty names and embedded NULs.
#include "reactor_tls.h"

#include <cstdio>
#include <cstdlib>
#include <functional>

static int failures = 0;
#define CHECK(cond) \
	do { \
		if (!(cond)) { std::fprintf(stderr, "%s:%d: CHECK(%s)\n", __FILE__, __LINE__, #cond); ++failures; } \
	} while (0)

static bool invalid(const std::function<void()>& f) {
	try {
		f();
	} catch (const std::invalid_argument&) {
		return true;
	}
	return false;
}

int main() {
	using reactor::tls::Options;
	using reactor::tls::Role;
	asio::ssl::context context(asio::ssl::context::tls);
	CHECK(invalid([&] { reactor::tls::configure(context, Options{}, Role::Client); }));                                  // no authority
	CHECK(invalid([&] { reactor::tls::configure(context, Options{"", "", "ca.pem"}, Role::Peer); }));                    // peers need a certificate
	CHECK(invalid([&] { reactor::tls::configure(context, Options{"", "", "ca.pem"}, Role::Server); }));                  // servers too
	CHECK(invalid([&] { reactor::tls::configure(context, Options{"cert.pem", "", "ca.pem"}, Role::Client); }));          // a certificate without its key
	SSL* ssl = SSL_new(context.native_handle());
	CHECK(!invalid([&] { reactor::tls::bind_hostname(ssl, "127.0.0.1"); }));
	CHECK(!invalid([&] { reactor::tls::bind_hostname(ssl, "::1"); }));
	CHECK(!invalid([&] { reactor::tls::bind_hostname(ssl, "example.com"); }));
	CHECK(invalid([&] { reactor::tls::bind_hostname(ssl, ""); }));
	CHECK(invalid([&] { reactor::tls::bind_hostname(ssl, std::string("a\0b", 3)); }));
	SSL_free(ssl);
	std::printf("tls: %s\n", failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
