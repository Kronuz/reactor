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
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <unistd.h>
#include <utility>
#include <cstdlib>
#include <functional>

static int failures = 0;
#define CHECK(cond) \
	do { \
		if (!(cond)) { std::fprintf(stderr, "%s:%d: CHECK(%s)\n", __FILE__, __LINE__, #cond); ++failures; } \
	} while (0)

// A self-signed certificate for localhost, and its key, written as PEM files.
static void self_signed(const std::string& cert_path, const std::string& key_path, bool rsa = false) {
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
	for (const char* ext : {"subjectAltName=DNS:localhost,IP:127.0.0.1", "basicConstraints=critical,CA:TRUE", "keyUsage=critical,digitalSignature,keyCertSign"}) {
		const std::string e(ext);
		const auto eq = e.find('=');
		X509_EXTENSION* x_ext = X509V3_EXT_conf(nullptr, &v3, e.substr(0, eq).c_str(), e.substr(eq + 1).c_str());
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

// Runs a TLS handshake between two contexts over a memory BIO pair; the client verifies
// "localhost". Returns the client's verification result, or -1 when the handshake fails.
static long handshake(asio::ssl::context& server, asio::ssl::context& client, const char* tls12_cipher = nullptr) {
	SSL* s = SSL_new(server.native_handle());
	SSL* c = SSL_new(client.native_handle());
	if (tls12_cipher) {  // the client offers TLS 1.2 with this one suite only
		SSL_set_max_proto_version(c, TLS1_2_VERSION);
		SSL_set_cipher_list(c, tls12_cipher);
	}
	reactor::tls::bind_hostname(c, "localhost");
	BIO *sb = nullptr, *cb = nullptr;
	BIO_new_bio_pair(&sb, 0, &cb, 0);
	SSL_set_bio(s, sb, sb);
	SSL_set_bio(c, cb, cb);
	SSL_set_accept_state(s);
	SSL_set_connect_state(c);
	bool s_done = false, c_done = false, failed = false;
	for (int i = 0; i < 100 && !(s_done && c_done) && !failed; ++i) {
		for (auto [ssl, done] : {std::pair{c, &c_done}, std::pair{s, &s_done}}) {
			if (*done) { continue; }
			const int r = SSL_do_handshake(ssl);
			if (r == 1) {
				*done = true;
			} else {
				const int e = SSL_get_error(ssl, r);
				failed = failed || (e != SSL_ERROR_WANT_READ && e != SSL_ERROR_WANT_WRITE);
			}
		}
	}
	const long result = s_done && c_done ? SSL_get_verify_result(c) : -1;
	SSL_free(s);
	SSL_free(c);
	return result;
}

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
	// A server presents its certificate without an authority; a client trusts that
	// certificate, or the system's roots; peers still need their own authority.
	char dir[] = "/tmp/reactor_tls_XXXXXX";
	CHECK(::mkdtemp(dir) != nullptr);
	const std::string cert = std::string(dir) + "/cert.pem", key = std::string(dir) + "/key.pem";
	self_signed(cert, key);
	CHECK(!invalid([&] { reactor::tls::configure(context, Options{cert, key, ""}, Role::Server); }));
	CHECK(invalid([&] { reactor::tls::configure(context, Options{cert, key, ""}, Role::Peer); }));
	CHECK(invalid([&] { reactor::tls::configure(context, Options{cert, key, "", true}, Role::Peer); }));
	CHECK(!invalid([&] { reactor::tls::configure(context, Options{"", "", "", true}, Role::Client); }));
	{
		asio::ssl::context server(asio::ssl::context::tls), trusting(asio::ssl::context::tls), roots(asio::ssl::context::tls);
		reactor::tls::configure(server, Options{cert, key, ""}, Role::Server);
		reactor::tls::configure(trusting, Options{"", "", cert}, Role::Client);
		reactor::tls::configure(roots, Options{"", "", "", true}, Role::Client);
		CHECK(handshake(server, trusting) == X509_V_OK);
		CHECK(handshake(server, roots) == -1);  // a self-signed certificate is in no system's roots
	}
	// strict: TLS 1.2 keeps only forward-secret AEAD suites; without it, OpenSSL's defaults.
	{
		const std::string rsa_cert = std::string(dir) + "/rsa.pem", rsa_key = std::string(dir) + "/rsa.key";
		self_signed(rsa_cert, rsa_key, true);
		asio::ssl::context strict(asio::ssl::context::tls), lax(asio::ssl::context::tls), client(asio::ssl::context::tls);
		reactor::tls::Options so{rsa_cert, rsa_key, ""};
		so.strict = true;
		reactor::tls::configure(strict, so, Role::Server);
		reactor::tls::configure(lax, Options{rsa_cert, rsa_key, ""}, Role::Server);
		reactor::tls::configure(client, Options{"", "", rsa_cert}, Role::Client);
		CHECK(handshake(strict, client, "ECDHE-RSA-AES128-GCM-SHA256") == X509_V_OK);  // ECDHE-AEAD accepted
		CHECK(handshake(strict, client, "AES128-GCM-SHA256") == -1);                    // static RSA refused
		CHECK(handshake(strict, client, "ECDHE-RSA-AES128-SHA") == -1);                 // CBC refused
		CHECK(handshake(lax, client, "AES128-GCM-SHA256") == X509_V_OK);                // unchanged without strict
		CHECK((SSL_CTX_get_options(strict.native_handle()) & SSL_OP_NO_TICKET) != 0);
		CHECK(SSL_CTX_get_num_tickets(strict.native_handle()) == 0);
		CHECK(SSL_CTX_get_session_cache_mode(strict.native_handle()) == SSL_SESS_CACHE_OFF);
		CHECK((SSL_CTX_get_options(lax.native_handle()) & SSL_OP_NO_TICKET) == 0);
		std::remove(rsa_cert.c_str());
		std::remove(rsa_key.c_str());
	}
	std::remove(cert.c_str());
	std::remove(key.c_str());
	::rmdir(dir);

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
