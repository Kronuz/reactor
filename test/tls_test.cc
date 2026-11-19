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
// IP literals and DNS names but refuse empty names and embedded NULs. Unchanged files load
// once however many contexts use them; a rotated file is served by the next configure.
// `reactor_tls_test [basic|cache|rotation|threads]` runs one part; no argument runs all.
#include "reactor_tls.h"

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <set>
#include <thread>
#include <unistd.h>
#include <utility>
#include <cstdlib>
#include <functional>
#include <vector>

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

using reactor::tls::Options;
using reactor::tls::Role;

static X509* presented(asio::ssl::context& context) { return SSL_CTX_get0_certificate(context.native_handle()); }

// The certificates a context trusts, as the objects its store holds.
static std::vector<X509*> trusted(asio::ssl::context& context) {
	std::vector<X509*> out;
#if OPENSSL_VERSION_NUMBER >= 0x30300000L
	STACK_OF(X509_OBJECT)* objects = X509_STORE_get1_objects(SSL_CTX_get_cert_store(context.native_handle()));
#else
	STACK_OF(X509_OBJECT)* objects = X509_STORE_get0_objects(SSL_CTX_get_cert_store(context.native_handle()));
#endif
	for (int i = 0; i < sk_X509_OBJECT_num(objects); ++i) {
		if (X509* x = X509_OBJECT_get0_X509(sk_X509_OBJECT_value(objects, i))) { out.push_back(x); }
	}
#if OPENSSL_VERSION_NUMBER >= 0x30300000L
	sk_X509_OBJECT_pop_free(objects, X509_OBJECT_free);
#endif
	return out;
}

// Whether the certificate in a PEM file is `x`.
static bool same(X509* x, const std::string& path) {
	FILE* f = std::fopen(path.c_str(), "rb");
	if (!f) { return false; }
	X509* file = PEM_read_X509(f, nullptr, nullptr, nullptr);
	std::fclose(f);
	const bool equal = x && file && X509_cmp(x, file) == 0;
	X509_free(file);
	return equal;
}

static void copy(const std::string& from, const std::string& to) { std::filesystem::copy_file(from, to, std::filesystem::copy_options::overwrite_existing); }

// Repeated configures of unchanged files share one parse: one leaf, one key and one
// trusted certificate across every context, and on a context configured twice.
static void cache(const std::string& dir) {
	const std::string cert = dir + "/cache.pem", key = dir + "/cache.key";
	self_signed(cert, key);
	std::set<X509*> leaves, authorities;
	std::set<EVP_PKEY*> keys;
	for (int i = 0; i < 50; ++i) {
		asio::ssl::context server(asio::ssl::context::tls), client(asio::ssl::context::tls);
		reactor::tls::configure(server, Options{cert, key, ""}, Role::Server);
		reactor::tls::configure(client, Options{"", "", cert}, Role::Client);
		if (i == 0) { reactor::tls::configure(server, Options{cert, key, ""}, Role::Server); }
		leaves.insert(presented(server));
		keys.insert(SSL_CTX_get0_privatekey(server.native_handle()));
		for (X509* x : trusted(client)) { authorities.insert(x); }
		CHECK(handshake(server, client) == X509_V_OK);
	}
	CHECK(leaves.size() == 1);
	CHECK(keys.size() == 1);
	CHECK(authorities.size() == 1);
	CHECK(same(*leaves.begin(), cert));
	std::remove(cert.c_str());
	std::remove(key.c_str());
}

// A rotated certificate, key or authority is what the next configure serves, even when the
// new file keeps the old one's modification time; contexts configured earlier keep theirs.
// A half-rotated pair is refused, and the next complete one is served.
static void rotation(const std::string& dir) {
	const std::string cert = dir + "/rot.pem", key = dir + "/rot.key", ca = dir + "/rot-ca.pem";
	const std::string a = dir + "/a.pem", a_key = dir + "/a.key", b = dir + "/b.pem", b_key = dir + "/b.key", c = dir + "/c.pem",
		c_key = dir + "/c.key";
	self_signed(a, a_key);
	self_signed(b, b_key);
	self_signed(c, c_key);
	copy(a, cert);
	copy(a_key, key);
	copy(a, ca);
	asio::ssl::context old_server(asio::ssl::context::tls), old_client(asio::ssl::context::tls), trusts_a(asio::ssl::context::tls),
		trusts_b(asio::ssl::context::tls);
	reactor::tls::configure(old_server, Options{cert, key, ""}, Role::Server);
	reactor::tls::configure(old_client, Options{"", "", ca}, Role::Client);
	reactor::tls::configure(trusts_a, Options{"", "", a}, Role::Client);
	reactor::tls::configure(trusts_b, Options{"", "", b}, Role::Client);
	CHECK(same(presented(old_server), a));
	CHECK(handshake(old_server, old_client) == X509_V_OK);

	// In place, with the old modification times restored.
	const auto cert_time = std::filesystem::last_write_time(cert), key_time = std::filesystem::last_write_time(key),
			   ca_time = std::filesystem::last_write_time(ca);
	copy(b, cert);
	copy(b_key, key);
	copy(b, ca);
	std::filesystem::last_write_time(cert, cert_time);
	std::filesystem::last_write_time(key, key_time);
	std::filesystem::last_write_time(ca, ca_time);
	asio::ssl::context server(asio::ssl::context::tls), client(asio::ssl::context::tls);
	reactor::tls::configure(server, Options{cert, key, ""}, Role::Server);
	reactor::tls::configure(client, Options{"", "", ca}, Role::Client);
	CHECK(same(presented(server), b));
	CHECK(handshake(server, trusts_b) == X509_V_OK);
	CHECK(handshake(server, trusts_a) == -1);
	CHECK(handshake(server, client) == X509_V_OK);
	CHECK(handshake(server, old_client) == -1);     // the old client still trusts only a
	CHECK(handshake(old_server, trusts_a) == X509_V_OK);  // and the old server still presents it
	CHECK(same(presented(old_server), a));

	// By rename, the certificate first: the pair is refused until its key follows.
	copy(c, dir + "/next.pem");
	std::filesystem::rename(dir + "/next.pem", cert);
	asio::ssl::context half(asio::ssl::context::tls);
	bool refused = false;
	try {
		reactor::tls::configure(half, Options{cert, key, ""}, Role::Server);
	} catch (const std::exception&) {
		refused = true;
	}
	CHECK(refused);
	copy(c_key, dir + "/next.key");
	std::filesystem::rename(dir + "/next.key", key);
	asio::ssl::context next(asio::ssl::context::tls);
	reactor::tls::configure(next, Options{cert, key, ""}, Role::Server);
	CHECK(same(presented(next), c));
	for (const std::string& path : {cert, key, ca, a, a_key, b, b_key, c, c_key}) { std::remove(path.c_str()); }
}

// Contexts configured on several threads at once from the same files, then handshaking
// on those threads, share the cached certificates without a race.
static void threads(const std::string& dir) {
	const std::string cert = dir + "/threads.pem", key = dir + "/threads.key";
	self_signed(cert, key);
	std::vector<std::thread> workers;
	std::vector<int> ok(4, 0);
	for (int t = 0; t < 4; ++t) {
		workers.emplace_back([&, t] {
			for (int i = 0; i < 20; ++i) {
				asio::ssl::context server(asio::ssl::context::tls), client(asio::ssl::context::tls);
				reactor::tls::configure(server, Options{cert, key, ""}, Role::Server);
				reactor::tls::configure(client, Options{"", "", cert}, Role::Client);
				ok[t] += handshake(server, client) == X509_V_OK;
			}
		});
	}
	for (auto& worker : workers) { worker.join(); }
	for (int t = 0; t < 4; ++t) { CHECK(ok[t] == 20); }
	std::remove(cert.c_str());
	std::remove(key.c_str());
}

static void basic(const std::string& dir_name) {
	asio::ssl::context context(asio::ssl::context::tls);
	CHECK(invalid([&] { reactor::tls::configure(context, Options{}, Role::Client); }));                                  // no authority
	CHECK(invalid([&] { reactor::tls::configure(context, Options{"", "", "ca.pem"}, Role::Peer); }));                    // peers need a certificate
	CHECK(invalid([&] { reactor::tls::configure(context, Options{"", "", "ca.pem"}, Role::Server); }));                  // servers too
	CHECK(invalid([&] { reactor::tls::configure(context, Options{"cert.pem", "", "ca.pem"}, Role::Client); }));          // a certificate without its key
	// A server presents its certificate without an authority; a client trusts that
	// certificate, or the system's roots; peers still need their own authority.
	const char* dir = dir_name.c_str();
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

	SSL* ssl = SSL_new(context.native_handle());
	CHECK(!invalid([&] { reactor::tls::bind_hostname(ssl, "127.0.0.1"); }));
	CHECK(!invalid([&] { reactor::tls::bind_hostname(ssl, "::1"); }));
	CHECK(!invalid([&] { reactor::tls::bind_hostname(ssl, "example.com"); }));
	CHECK(invalid([&] { reactor::tls::bind_hostname(ssl, ""); }));
	CHECK(invalid([&] { reactor::tls::bind_hostname(ssl, std::string("a\0b", 3)); }));
	SSL_free(ssl);
}

int main(int argc, char** argv) {
	const std::string part = argc > 1 ? argv[1] : "";
	char dir[] = "/tmp/reactor_tls_XXXXXX";
	CHECK(::mkdtemp(dir) != nullptr);
	const std::pair<const char*, void (*)(const std::string&)> parts[] = {{"basic", basic}, {"cache", cache}, {"rotation", rotation}, {"threads", threads}};
	bool ran = false;
	for (auto [name, run] : parts) {
		if (part.empty() || part == name) {
			run(dir);
			ran = true;
		}
	}
	CHECK(ran);
	::rmdir(dir);
	std::printf("tls%s%s: %s\n", part.empty() ? "" : " ", part.c_str(), failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
