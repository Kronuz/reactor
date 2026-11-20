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

// reactor::tls: incomplete options for each role are refused, server_context refuses a loose key and
// a certificate out of its dates, and client connections bind IP literals and DNS names but refuse
// empty names and embedded NULs. Unchanged files load once however many contexts use them; a
// rotated file is served by the next configure. A parse lives only while a context holds it, and
// every copy of a password's digest is cleansed.
// `reactor_tls_test [basic|server_context|cache|rotation|password|combined|race|threads|release|bounded|digest]`
// runs one part; none runs all.
#include "reactor_tls.h"

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <new>
#include <set>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <cstdlib>
#include <functional>
#include <type_traits>
#include <vector>

static int failures = 0;
#define CHECK(cond) \
	do { \
		if (!(cond)) { std::fprintf(stderr, "%s:%d: CHECK(%s)\n", __FILE__, __LINE__, #cond); ++failures; } \
	} while (0)

// A self-signed certificate for localhost, and its key, written as PEM files.
static void self_signed(const std::string& cert_path, const std::string& key_path, bool rsa = false, long not_before = 0,
	long not_after = 3600) {
	EVP_PKEY* key = rsa ? EVP_RSA_gen(2048) : EVP_EC_gen("P-256");
	X509* x = X509_new();
	X509_set_version(x, 2);
	ASN1_INTEGER_set(X509_get_serialNumber(x), 1);
	X509_gmtime_adj(X509_getm_notBefore(x), not_before);
	X509_gmtime_adj(X509_getm_notAfter(x), not_after);
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
	::chmod(key_path.c_str(), 0600);
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
// trusted certificate across every live context, server_context's included, and on a
// context configured twice.
static void cache(const std::string& dir) {
	const std::string cert = dir + "/cache.pem", key = dir + "/cache.key";
	self_signed(cert, key);
	::chmod(key.c_str(), 0600);
	std::vector<std::shared_ptr<asio::ssl::context>> servers, clients;   // alive, so no object is freed and reused
	std::set<X509*> leaves, authorities;
	std::set<EVP_PKEY*> keys;
	for (int i = 0; i < 50; ++i) {
		auto server = i % 2 ? reactor::tls::server_context(Options{cert, key, ""}) : std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
		auto client = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
		if (i % 2 == 0) { reactor::tls::configure(*server, Options{cert, key, ""}, Role::Server); }
		reactor::tls::configure(*client, Options{"", "", cert}, Role::Client);
		if (i == 0) { reactor::tls::configure(*server, Options{cert, key, ""}, Role::Server); }
		leaves.insert(presented(*server));
		keys.insert(SSL_CTX_get0_privatekey(server->native_handle()));
		for (X509* x : trusted(*client)) { authorities.insert(x); }
		CHECK(handshake(*server, *client) == X509_V_OK);
		servers.push_back(std::move(server));
		clients.push_back(std::move(client));
	}
	CHECK(leaves.size() == 1);
	CHECK(keys.size() == 1);
	CHECK(authorities.size() == 1);
	CHECK(same(presented(*servers.front()), cert));
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

// The key at `from`, written to `to` encrypted with `password`.
static void encrypt_key(const std::string& from, const std::string& to, const char* password) {
	FILE* f = std::fopen(from.c_str(), "rb");
	EVP_PKEY* key = PEM_read_PrivateKey(f, nullptr, nullptr, nullptr);
	std::fclose(f);
	f = std::fopen(to.c_str(), "wb");
	PEM_write_PrivateKey(f, key, EVP_aes_128_cbc(), reinterpret_cast<const unsigned char*>(password), int(std::strlen(password)), nullptr, nullptr);
	std::fclose(f);
	EVP_PKEY_free(key);
}

// An encrypted key loads with the right password only, before and after it is cached; the
// right password still shares one parse.
static void password(const std::string& dir) {
	const std::string cert = dir + "/pw.pem", plain = dir + "/pw-plain.key", key = dir + "/pw.key";
	self_signed(cert, plain);
	encrypt_key(plain, key, "right");
	std::vector<std::shared_ptr<asio::ssl::context>> live;
	auto with = [&](const char* pw) -> EVP_PKEY* {
		auto context = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
		context->set_password_callback([pw](std::size_t, asio::ssl::context::password_purpose) { return std::string(pw); });
		try {
			reactor::tls::configure(*context, Options{cert, key, ""}, Role::Server);
		} catch (const std::exception&) {
			return nullptr;
		}
		live.push_back(context);
		return SSL_CTX_get0_privatekey(context->native_handle());
	};
	CHECK(with("wrong") == nullptr);
	EVP_PKEY* first = with("right");
	CHECK(first != nullptr);
	CHECK(with("wrong") == nullptr);   // cached, and still refused
	CHECK(with("") == nullptr);
	CHECK(with("right") == first);     // cached: one parse
	for (const std::string& path : {cert, plain, key}) { std::remove(path.c_str()); }
}

// One file holding a certificate and its key, named as certificate, key and authority: each
// use keeps its own parse, on a miss and on a hit.
static void combined(const std::string& dir) {
	const std::string cert = dir + "/both-cert.pem", key = dir + "/both-key.pem", both = dir + "/both.pem";
	self_signed(cert, key);
	{
		std::ofstream out(both, std::ios::binary);
		out << std::ifstream(cert, std::ios::binary).rdbuf() << std::ifstream(key, std::ios::binary).rdbuf();
	}
	for (int round = 0; round < 2; ++round) {
		asio::ssl::context server(asio::ssl::context::tls), client(asio::ssl::context::tls), peer(asio::ssl::context::tls),
			other(asio::ssl::context::tls);
		CHECK(!invalid([&] { reactor::tls::configure(server, Options{both, both, ""}, Role::Server); }));
		CHECK(!invalid([&] { reactor::tls::configure(client, Options{"", "", both}, Role::Client); }));
		CHECK(!invalid([&] { reactor::tls::configure(peer, Options{both, both, both}, Role::Peer); }));
		CHECK(!invalid([&] { reactor::tls::configure(other, Options{both, both, both}, Role::Peer); }));
		CHECK(same(presented(server), cert));
		CHECK(handshake(server, client) == X509_V_OK);
		CHECK(handshake(peer, other) == X509_V_OK);   // mutual: each presents and verifies
	}
	for (const std::string& path : {cert, key, both}) { std::remove(path.c_str()); }
}

// Four threads configure while a fifth rotates certificate, key and authority by rename
// through 40 generations, the certificate briefly ahead of its key. Every context that
// configures presents and trusts a generation no older than the one published when its call
// began, with a matching key; after the rotation the next configure serves the last one.
static void race(const std::string& dir) {
	constexpr int generations = 40;
	const std::string cert = dir + "/race.pem", key = dir + "/race.key", ca = dir + "/race-ca.pem";
	std::vector<X509*> certificates;
	for (int g = 0; g < generations; ++g) {
		const std::string base = dir + "/race-" + std::to_string(g);
		self_signed(base + ".pem", base + ".key");
		FILE* f = std::fopen((base + ".pem").c_str(), "rb");
		certificates.push_back(PEM_read_X509(f, nullptr, nullptr, nullptr));
		std::fclose(f);
	}
	auto generation = [&](X509* x) {
		for (int g = 0; g < generations; ++g) {
			if (x && X509_cmp(x, certificates[g]) == 0) { return g; }
		}
		return -1;
	};
	auto publish = [&](int g, const char* ext, const std::string& to) {
		copy(dir + "/race-" + std::to_string(g) + ext, to + ".tmp");
		std::filesystem::rename(to + ".tmp", to);
	};
	publish(0, ".pem", cert);
	publish(0, ".key", key);
	publish(0, ".pem", ca);
	std::atomic<int> version{0};
	std::atomic<bool> done{false};
	std::atomic<long> served{0}, wrong{0};
	std::vector<std::thread> workers;
	for (int t = 0; t < 4; ++t) {
		workers.emplace_back([&] {
			for (int i = 0; !done.load() || i < 5; ++i) {
				const int before = version.load();
				asio::ssl::context server(asio::ssl::context::tls), client(asio::ssl::context::tls);
				bool ok = true;
				try {
					reactor::tls::configure(server, Options{cert, key, ""}, Role::Server);
				} catch (const std::exception&) {
					ok = false;   // a half-rotated pair
				}
				reactor::tls::configure(client, Options{"", "", ca}, Role::Client);
				const int after = version.load();
				const auto authority = trusted(client);
				const int c = authority.size() == 1 ? generation(authority.front()) : -1;
				wrong += c < before || c > after + 1;
				if (!ok) { continue; }
				++served;
				const int s = generation(presented(server));
				wrong += s < before || s > after + 1;
				wrong += X509_check_private_key(presented(server), SSL_CTX_get0_privatekey(server.native_handle())) != 1;
				wrong += s == c && handshake(server, client) != X509_V_OK;
			}
		});
	}
	for (int g = 1; g < generations; ++g) {
		publish(g, ".pem", ca);
		publish(g, ".pem", cert);
		std::this_thread::sleep_for(std::chrono::microseconds(200));
		publish(g, ".key", key);
		version.store(g);
		std::this_thread::sleep_for(std::chrono::milliseconds(2));
	}
	done.store(true);
	for (auto& worker : workers) { worker.join(); }
	asio::ssl::context last(asio::ssl::context::tls), last_client(asio::ssl::context::tls);
	reactor::tls::configure(last, Options{cert, key, ""}, Role::Server);
	reactor::tls::configure(last_client, Options{"", "", ca}, Role::Client);
	CHECK(generation(presented(last)) == generations - 1);
	const auto authority = trusted(last_client);
	CHECK(authority.size() == 1 && generation(authority.front()) == generations - 1);
	CHECK(handshake(last, last_client) == X509_V_OK);
	CHECK(wrong == 0);
	CHECK(served > 0);
	for (X509* x : certificates) { X509_free(x); }
	for (int g = 0; g < generations; ++g) {
		std::remove((dir + "/race-" + std::to_string(g) + ".pem").c_str());
		std::remove((dir + "/race-" + std::to_string(g) + ".key").c_str());
	}
	for (const std::string& path : {cert, key, ca}) { std::remove(path.c_str()); }
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

using reactor::tls::detail::Use;

// How many entries the parse cache keeps, and whether it keeps one for this use and path.
static std::size_t entries() {
	std::lock_guard lock(reactor::tls::detail::cache().mutex);
	return reactor::tls::detail::cache().entries.size();
}
static bool cached(Use use, const std::string& path) {
	std::lock_guard lock(reactor::tls::detail::cache().mutex);
	return reactor::tls::detail::cache().entries.count({use, path}) != 0;
}
static std::weak_ptr<const reactor::tls::detail::Pem> entry(Use use, const std::string& path) {
	std::lock_guard lock(reactor::tls::detail::cache().mutex);
	auto it = reactor::tls::detail::cache().entries.find({use, path});
	return it == reactor::tls::detail::cache().entries.end() ? std::weak_ptr<const reactor::tls::detail::Pem>() : it->second;
}

// A parse lives while a context configured from it does: freeing the last such context frees the
// entry and its decrypted key, and a context configured again lets go of the parses it left.
static void release(const std::string& dir) {
	const std::string cert = dir + "/rel.pem", plain = dir + "/rel-plain.key", key = dir + "/rel.key", other = dir + "/rel2.pem",
		other_key = dir + "/rel2.key";
	self_signed(cert, plain);
	encrypt_key(plain, key, "secret");
	self_signed(other, other_key);
	auto server = [&] {
		auto context = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
		context->set_password_callback([](std::size_t, asio::ssl::context::password_purpose) { return std::string("secret"); });
		reactor::tls::configure(*context, Options{cert, key, ""}, Role::Server);
		return context;
	};
	auto a = server(), b = server();
	asio::ssl::context client(asio::ssl::context::tls);
	reactor::tls::configure(client, Options{"", "", cert}, Role::Client);
	CHECK(SSL_CTX_get0_privatekey(a->native_handle()) == SSL_CTX_get0_privatekey(b->native_handle()));   // one parse
	const auto chain = entry(Use::Chain, cert), parsed = entry(Use::Key, key), authority = entry(Use::Authority, cert);
	CHECK(!chain.expired() && !parsed.expired() && !authority.expired());
	CHECK(parsed.lock() && parsed.lock()->encrypted);
	a.reset();
	CHECK(!chain.expired() && !parsed.expired());   // b still holds them
	b.reset();
	CHECK(chain.expired() && parsed.expired());
	CHECK(!cached(Use::Chain, cert) && !cached(Use::Key, key));
	CHECK(!authority.expired() && cached(Use::Authority, cert));   // the client still holds its authority

	// Configured again from other files, a context holds only the new parses.
	reactor::tls::configure(client, Options{"", "", other}, Role::Client);
	CHECK(authority.expired() && !cached(Use::Authority, cert));
	CHECK(cached(Use::Authority, other));
	for (const std::string& path : {cert, plain, key, other, other_key}) { std::remove(path.c_str()); }
}

// Contexts configured from many distinct paths leave the cache empty once they are freed.
static void bounded(const std::string& dir) {
	constexpr int n = 16;
	const std::size_t before = entries();
	std::vector<std::shared_ptr<asio::ssl::context>> live;
	for (int i = 0; i < n; ++i) {
		const std::string cert = dir + "/bound-" + std::to_string(i) + ".pem", key = dir + "/bound-" + std::to_string(i) + ".key";
		self_signed(cert, key);
		auto server = std::make_shared<asio::ssl::context>(asio::ssl::context::tls), client = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
		reactor::tls::configure(*server, Options{cert, key, ""}, Role::Server);
		reactor::tls::configure(*client, Options{"", "", cert}, Role::Client);
		CHECK(handshake(*server, *client) == X509_V_OK);
		live.push_back(std::move(server));
		live.push_back(std::move(client));
		std::remove(cert.c_str());
		std::remove(key.c_str());
	}
	CHECK(entries() == before + 3 * n);   // a chain, a key and an authority per path
	live.clear();
	CHECK(entries() == 0);
}

// Whether the digest-sized bytes at `at` are all zero, read as raw storage.
static bool zero(const unsigned char* at) {
	const volatile unsigned char* p = at;
	for (std::size_t i = 0; i < sizeof(reactor::tls::detail::Digest::bytes); ++i) {
		if (p[i] != 0) { return false; }
	}
	return true;
}

// Every holder of a password's digest keeps it as a Digest, so each copy, the context's on every
// configure and the entry's, is cleansed when freed: the bytes it stood in read zero afterwards.
static void digest(const std::string&) {
	using reactor::tls::detail::Digest;
	using reactor::tls::detail::Password;
	using reactor::tls::detail::Pem;
	static_assert(std::is_same_v<decltype(Password::digest), Digest>);
	static_assert(std::is_same_v<decltype(Pem::password), Digest>);
	pem_password_cb* secret = [](char* buffer, int, int, void*) {
		std::memcpy(buffer, "secret", 6);
		return 6;
	};
	char buffer[PEM_BUFSIZE];
	{
		alignas(Password) unsigned char storage[sizeof(Password)];
		auto* password = new (storage) Password{secret, nullptr};
		CHECK(Password::supply(buffer, sizeof buffer, 0, password) == 6);
		CHECK(password->digest.set);
		const unsigned char* at = password->digest.bytes;
		CHECK(!zero(at));
		password->~Password();
		CHECK(zero(at));
	}
	{
		Password supplied{secret, nullptr};
		(void)Password::supply(buffer, sizeof buffer, 0, &supplied);
		alignas(Pem) unsigned char storage[sizeof(Pem)];
		auto* pem = new (storage) Pem;
		pem->password = supplied.digest;
		CHECK(pem->password == supplied.digest);
		const unsigned char* at = pem->password.bytes;
		CHECK(!zero(at));
		pem->~Pem();
		CHECK(zero(at));
	}
	{
		Password one{secret, nullptr}, other{[](char* b, int, int, void*) { b[0] = 'x'; return 1; }, nullptr}, none{nullptr, nullptr};
		(void)Password::supply(buffer, sizeof buffer, 0, &one);
		(void)Password::supply(buffer, sizeof buffer, 0, &other);
		CHECK(!(one.digest == other.digest));
		CHECK(!(none.digest == none.digest));   // no digest matches nothing
	}
	OPENSSL_cleanse(buffer, sizeof buffer);
}

// The message server_context refuses with, or "" when it accepts.
static std::string refusal(const std::string& cert, const std::string& key) {
	try {
		(void)reactor::tls::server_context(reactor::tls::Options{cert, key, ""});
	} catch (const std::invalid_argument& e) {
		return e.what();
	}
	return "";
}
static bool contains(const std::string& text, const char* part) { return text.find(part) != std::string::npos; }

// server_context: Xapiand's startup checks (the key's mode, the validity dates, the pair) and strict;
// seconds_to_expiry against the certificate's notAfter and the 14-day warning.
static void server_context(const std::string& dir) {
	const std::string cert = dir + "/sc.pem", key = dir + "/sc.key", other_cert = dir + "/other.pem", other_key = dir + "/other.key";
	self_signed(cert, key);
	self_signed(other_cert, other_key);
	CHECK(refusal(cert, key).empty());
	{
		const auto context = reactor::tls::server_context(reactor::tls::Options{cert, key, ""});
		CHECK(SSL_CTX_get_num_tickets(context->native_handle()) == 0);   // strict
		CHECK(SSL_CTX_get_session_cache_mode(context->native_handle()) == SSL_SESS_CACHE_OFF);
		CHECK((SSL_CTX_get_options(context->native_handle()) & SSL_OP_NO_TICKET) != 0);
		asio::ssl::context client(asio::ssl::context::tls);
		reactor::tls::configure(client, reactor::tls::Options{"", "", cert}, reactor::tls::Role::Client);
		CHECK(handshake(*context, client, "ECDHE-ECDSA-AES128-GCM-SHA256") == X509_V_OK);
		CHECK(handshake(*context, client, "ECDHE-ECDSA-AES128-SHA") == -1);   // CBC refused: strict
		const long left = reactor::tls::seconds_to_expiry(*context);
		CHECK(left > 3500 && left <= 3600);
		CHECK(std::chrono::seconds(left) < reactor::tls::expiry_warning);
	}
	::chmod(key.c_str(), 0640);
	CHECK(contains(refusal(cert, key), "make it mode 0600"));   // group-readable
	::chmod(key.c_str(), 0604);
	CHECK(contains(refusal(cert, key), "make it mode 0600"));   // others-readable
	::chmod(key.c_str(), 0600);
	CHECK(contains(refusal(cert, dir + "/missing.key"), "cannot read TLS key"));
	CHECK(contains(refusal(cert, other_key), "mismatch"));      // the pair
	const std::string expired = dir + "/expired.pem", expired_key = dir + "/expired.key";
	self_signed(expired, expired_key, false, -7200, -3600);
	CHECK(contains(refusal(expired, expired_key), "has expired"));
	const std::string future = dir + "/future.pem", future_key = dir + "/future.key";
	self_signed(future, future_key, false, 3600, 7200);
	CHECK(contains(refusal(future, future_key), "is not valid yet"));
	// Thirty days left: past the warning. A context with no certificate has 0 left.
	const std::string month = dir + "/month.pem", month_key = dir + "/month.key";
	self_signed(month, month_key, false, 0, 30L * 86400);
	{
		const auto context = reactor::tls::server_context(reactor::tls::Options{month, month_key, ""});
		const long left = reactor::tls::seconds_to_expiry(*context);
		CHECK(left > 30L * 86400 - 100 && left <= 30L * 86400);
		CHECK(std::chrono::seconds(left) >= reactor::tls::expiry_warning);
		asio::ssl::context empty(asio::ssl::context::tls);
		CHECK(reactor::tls::seconds_to_expiry(empty) == 0);
	}
	for (const auto& path : {cert, key, other_cert, other_key, expired, expired_key, future, future_key, month, month_key}) { std::remove(path.c_str()); }
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
	const std::pair<const char*, void (*)(const std::string&)> parts[] = {{"basic", basic}, {"server_context", server_context}, {"cache", cache}, {"rotation", rotation}, {"password", password}, {"combined", combined},
		{"race", race}, {"threads", threads}, {"release", release}, {"bounded", bounded}, {"digest", digest}};
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
