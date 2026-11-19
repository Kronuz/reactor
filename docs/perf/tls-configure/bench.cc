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

// The cost of one reactor::tls::configure on a fresh context, per role and key type, on
// unchanged files. `bench DIR LABEL [iterations]` writes its certificates into DIR and
// prints one line per case: LABEL, case, median, p10 and p90 in microseconds.
#include "reactor_tls.h"

#include <openssl/pem.h>
#include <openssl/x509v3.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

static void self_signed(const std::string& cert_path, const std::string& key_path, bool rsa) {
	EVP_PKEY* key = rsa ? EVP_RSA_gen(2048) : EVP_EC_gen("P-256");
	X509* x = X509_new();
	X509_set_version(x, 2);
	ASN1_INTEGER_set(X509_get_serialNumber(x), 1);
	X509_gmtime_adj(X509_getm_notBefore(x), 0);
	X509_gmtime_adj(X509_getm_notAfter(x), 3600);
	X509_set_pubkey(x, key);
	X509_NAME* name = X509_NAME_new();
	X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char*>("localhost"), -1, -1, 0);
	X509_set_subject_name(x, name);
	X509_set_issuer_name(x, name);
	X509_NAME_free(name);
	X509V3_CTX v3;
	X509V3_set_ctx_nodb(&v3);
	X509V3_set_ctx(&v3, x, x, nullptr, nullptr, 0);
	for (const auto& [ext, value] : {std::pair{"subjectAltName", "DNS:localhost,IP:127.0.0.1"}, std::pair{"basicConstraints", "critical,CA:TRUE"},
			 std::pair{"keyUsage", "critical,digitalSignature,keyCertSign"}}) {
		X509_EXTENSION* x_ext = X509V3_EXT_conf(nullptr, &v3, ext, value);
		X509_add_ext(x, x_ext, -1);
		X509_EXTENSION_free(x_ext);
	}
	X509_sign(x, key, EVP_sha256());
	FILE* f = std::fopen(cert_path.c_str(), "wb");
	PEM_write_X509(f, x);
	std::fclose(f);
	f = std::fopen(key_path.c_str(), "wb");
	PEM_write_PrivateKey(f, key, nullptr, nullptr, 0, nullptr, nullptr);
	std::fclose(f);
	X509_free(x);
	EVP_PKEY_free(key);
}

int main(int argc, char** argv) {
	if (argc < 3) {
		std::fprintf(stderr, "usage: %s DIR LABEL [iterations]\n", argv[0]);
		return 2;
	}
	const std::string dir = argv[1], label = argv[2];
	const int iterations = argc > 3 ? std::atoi(argv[3]) : 2000;
	const std::string ec = dir + "/ec.pem", ec_key = dir + "/ec.key", rsa = dir + "/rsa.pem", rsa_key = dir + "/rsa.key";
	self_signed(ec, ec_key, false);
	self_signed(rsa, rsa_key, true);
	using reactor::tls::Options;
	using reactor::tls::Role;
	struct Case {
		const char* name;
		Options options;
		Role role;
	};
	Options roots;
	roots.system_roots = true;
	const Case cases[] = {
		{"server-ec", Options{ec, ec_key, ""}, Role::Server},
		{"server-rsa", Options{rsa, rsa_key, ""}, Role::Server},
		{"client-ca", Options{"", "", ec}, Role::Client},
		{"peer-ec", Options{ec, ec_key, ec}, Role::Peer},
		{"client-system-roots", roots, Role::Client},
	};
	for (const Case& c : cases) {
		std::vector<double> us;
		us.reserve(iterations);
		for (int i = 0; i < iterations; ++i) {
			asio::ssl::context context(asio::ssl::context::tls);
			const auto start = std::chrono::steady_clock::now();
			reactor::tls::configure(context, c.options, c.role);
			us.push_back(std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - start).count());
		}
		std::sort(us.begin(), us.end());
		std::printf("%s %-20s median %9.2f p10 %9.2f p90 %9.2f us\n", label.c_str(), c.name, us[us.size() / 2], us[us.size() / 10],
			us[us.size() * 9 / 10]);
	}
	for (const std::string& path : {ec, ec_key, rsa, rsa_key}) { std::remove(path.c_str()); }
	return 0;
}
