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

// TLS configuration for reactor services and their clients: one place that sets the
// protocol floor, the hardening options, the certificates, and the verification mode,
// and binds a client connection to the name it asked for. Requires OpenSSL 3
// (target reactor::tls).
#pragma once
#include <asio/ip/address.hpp>
#include <asio/ssl.hpp>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <sys/stat.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

namespace reactor::tls {

// PEM files: this end's certificate chain and key, and the authority that signs peers.
// `system_roots` also trusts the system's root certificates, for clients of servers with
// public certificates; peers always need their own authority. `strict` keeps TLS 1.2 to
// forward-secret AEAD suites (ECDHE with AES-GCM or ChaCha20-Poly1305) and turns session
// resumption off in both versions: no tickets, no session-ID cache.
struct Options {
	std::string certificate, private_key, certificate_authority;
	bool system_roots = false;
	bool strict = false;
};

// Peer: mutual TLS, both ends present certificates. Server: presents one and does not
// ask the client for one, so it needs no authority. Client: verifies the server and may
// present one.
enum class Role { Peer, Server, Client };

namespace detail {

// A password's SHA-256 digest; every copy is cleansed when destroyed.
struct Digest {
	unsigned char bytes[32]{};
	bool set = false;
	Digest() = default;
	Digest(const Digest&) = default;
	Digest& operator=(const Digest&) = default;
	~Digest() { OPENSSL_cleanse(bytes, sizeof bytes); }
	bool operator==(const Digest& other) const { return set && other.set && CRYPTO_memcmp(bytes, other.bytes, sizeof bytes) == 0; }
};

// One PEM file's bytes as last parsed, and what they parsed to. Contexts configured from the
// same bytes share these objects, which are only read once published.
struct Pem {
	std::string bytes;
	std::vector<X509*> certificates;
	std::vector<X509_CRL*> revocations;
	EVP_PKEY* key = nullptr;
	bool encrypted = false;  // the key's parse asked for a password
	Digest password;         // that password's digest
	Pem() = default;
	Pem(const Pem&) = delete;
	Pem& operator=(const Pem&) = delete;
	~Pem() {
		for (X509* x : certificates) { X509_free(x); }
		for (X509_CRL* crl : revocations) { X509_CRL_free(crl); }
		EVP_PKEY_free(key);
		OPENSSL_cleanse(bytes.data(), bytes.size());
	}
};

// A context's password callback (OpenSSL's default when it has none), recording whether it was
// asked and the digest of what it supplied.
struct Password {
	pem_password_cb* callback;
	void* data;
	bool asked = false;
	Digest digest;
	static int supply(char* buffer, int size, int rwflag, void* self) {
		auto& password = *static_cast<Password*>(self);
		const int n = password.callback ? password.callback(buffer, size, rwflag, password.data) : PEM_def_callback(buffer, size, rwflag, password.data);
		unsigned int length = 0;
		password.asked = true;
		password.digest.set = n > 0 && EVP_Digest(buffer, static_cast<std::size_t>(n), password.digest.bytes, &length, EVP_sha256(), nullptr) == 1 &&
			length == sizeof password.digest.bytes;
		return n;
	}
};

// Whether the context supplies the password an encrypted key entry was decrypted with.
inline bool same_password(const Pem& pem, SSL_CTX* context) {
	Password password{SSL_CTX_get_default_passwd_cb(context), SSL_CTX_get_default_passwd_cb_userdata(context)};
	char buffer[PEM_BUFSIZE];
	(void)Password::supply(buffer, sizeof buffer, 0, &password);
	OPENSSL_cleanse(buffer, sizeof buffer);
	return password.digest == pem.password;
}

inline constexpr long max_file_bytes = 16L << 20;

// How a file is parsed: a certificate chain (leaf first), a private key, or an authority's
// certificates and revocation lists.
enum class Use { Chain, Key, Authority };

[[noreturn]] inline void ssl_error(const char* what) {
	throw std::system_error(asio::error_code(static_cast<int>(ERR_get_error()), asio::error::get_ssl_category()), what);
}

// The file's bytes, refused past max_file_bytes; sized up front so no grown copy is left uncleansed.
inline std::string read(const std::string& path, const char* what) {
	std::unique_ptr<std::FILE, decltype(&std::fclose)> file(std::fopen(path.c_str(), "rb"), &std::fclose);
	if (!file) { throw std::system_error(errno, std::generic_category(), what); }
	std::string bytes;
	if (std::fseek(file.get(), 0, SEEK_END) == 0) {
		if (const long size = std::ftell(file.get()); size > 0 && size <= max_file_bytes) { bytes.reserve(static_cast<std::size_t>(size)); }
		std::rewind(file.get());
	}
	char buffer[8192];
	for (std::size_t n; (n = std::fread(buffer, 1, sizeof buffer, file.get())) > 0 && bytes.size() <= std::size_t(max_file_bytes);) { bytes.append(buffer, n); }
	OPENSSL_cleanse(buffer, sizeof buffer);
	const int error = std::ferror(file.get()) != 0 ? EIO : bytes.size() > std::size_t(max_file_bytes) ? EFBIG : 0;
	if (error != 0) {
		OPENSSL_cleanse(bytes.data(), bytes.size());
		throw std::system_error(error, std::generic_category(), what);
	}
	return bytes;
}

// Parses as OpenSSL's file loaders do (SSL_CTX_use_certificate_chain_file,
// SSL_CTX_use_PrivateKey_file, X509_load_cert_crl_file), from memory, with the context's
// password callback, whose digest a key entry keeps when it was asked. Each certificate's extensions are decoded before it is shared, so
// handshakes on other threads only read them.
inline std::unique_ptr<Pem> parse(Use use, std::string bytes, SSL_CTX* context, const char* what) {
	auto pem = std::make_unique<Pem>();
	pem->bytes = std::move(bytes);
	std::unique_ptr<BIO, decltype(&BIO_free)> bio(BIO_new_mem_buf(pem->bytes.data(), static_cast<int>(pem->bytes.size())), &BIO_free);
	if (!bio) { ssl_error(what); }
	pem_password_cb* password = SSL_CTX_get_default_passwd_cb(context);
	void* password_data = SSL_CTX_get_default_passwd_cb_userdata(context);
	ERR_clear_error();
	if (use == Use::Chain) {
		X509* leaf = PEM_read_bio_X509_AUX(bio.get(), nullptr, password, password_data);
		if (!leaf) { ssl_error(what); }
		pem->certificates.push_back(leaf);
		while (X509* x = PEM_read_bio_X509(bio.get(), nullptr, password, password_data)) { pem->certificates.push_back(x); }
		const unsigned long end = ERR_peek_last_error();
		if (end != 0 && (ERR_GET_LIB(end) != ERR_LIB_PEM || ERR_GET_REASON(end) != PEM_R_NO_START_LINE)) { ssl_error(what); }
		ERR_clear_error();
	} else if (use == Use::Key) {
		Password supplied{password, password_data};
		pem->key = PEM_read_bio_PrivateKey(bio.get(), nullptr, &Password::supply, &supplied);
		if (!pem->key) { ssl_error(what); }
		pem->encrypted = supplied.asked;
		pem->password = supplied.digest;
	} else {
		STACK_OF(X509_INFO)* infos = PEM_X509_INFO_read_bio(bio.get(), nullptr, nullptr, const_cast<char*>(""));
		if (!infos) { ssl_error(what); }
		for (int i = 0; i < sk_X509_INFO_num(infos); ++i) {
			X509_INFO* info = sk_X509_INFO_value(infos, i);
			if (info->x509) { pem->certificates.push_back(std::exchange(info->x509, nullptr)); }
			if (info->crl) { pem->revocations.push_back(std::exchange(info->crl, nullptr)); }
		}
		sk_X509_INFO_pop_free(infos, X509_INFO_free);
		if (pem->certificates.empty() && pem->revocations.empty()) {
			ERR_raise(ERR_LIB_X509, X509_R_NO_CERTIFICATE_OR_CRL_FOUND);
			ssl_error(what);
		}
	}
	for (X509* x : pem->certificates) { (void)X509_check_purpose(x, -1, 0); }
	return pem;
}

// The latest parse of each use and path, while a context holds it (hold()): an entry and its
// parse go when the last context holding the parse is freed.
struct Cache {
	std::mutex mutex;
	std::map<std::pair<Use, std::string>, std::weak_ptr<const Pem>> entries;
};

// Never destroyed, so a context freed during static destruction still finds it.
inline Cache& cache() {
	static Cache& cache = *new Cache;
	return cache;
}

// Frees a parse and drops its entry unless a newer parse replaced it. It locks the cache, so no
// last reference to a parse is dropped while the cache is locked.
struct Release {
	std::pair<Use, std::string> key;
	void operator()(const Pem* pem) const {
		{
			std::lock_guard lock(cache().mutex);
			if (auto it = cache().entries.find(key); it != cache().entries.end() && it->second.expired()) { cache().entries.erase(it); }
		}
		delete pem;
	}
};

// The file's parse: reused while its bytes are those last parsed for this path and use (and,
// for an encrypted key, the context supplies the same password) and a live context holds it,
// parsed again and kept otherwise. The password callback runs outside the lock.
inline std::shared_ptr<const Pem> load(Use use, const std::string& path, SSL_CTX* context, const char* what) {
	std::string bytes = read(path, what);
	std::shared_ptr<const Pem> cached;
	{
		std::lock_guard lock(cache().mutex);
		if (auto it = cache().entries.find({use, path}); it != cache().entries.end()) { cached = it->second.lock(); }
	}
	if (cached && cached->bytes == bytes && (!cached->encrypted || same_password(*cached, context))) {
		OPENSSL_cleanse(bytes.data(), bytes.size());
		return cached;
	}
	cached.reset();
	std::shared_ptr<const Pem> pem(parse(use, std::move(bytes), context, what).release(), Release{{use, path}});
	std::lock_guard lock(cache().mutex);
	cache().entries[{use, path}] = pem;
	return pem;
}

// The parses a context was configured from, freed with the context.
using Held = std::vector<std::shared_ptr<const Pem>>;

inline void free_held(void*, void* held, CRYPTO_EX_DATA*, int, long, void*) { delete static_cast<Held*>(held); }

inline int held_index() {
	static const int index = SSL_CTX_get_ex_new_index(0, nullptr, nullptr, nullptr, &free_held);
	if (index < 0) { ssl_error("SSL_CTX_get_ex_new_index"); }
	return index;
}

// Makes `held` what the context holds, replacing what an earlier configure left.
inline void hold(SSL_CTX* context, Held held) {
	const int index = held_index();
	auto next = std::make_unique<Held>(std::move(held));
	auto* previous = static_cast<Held*>(SSL_CTX_get_ex_data(context, index));
	if (SSL_CTX_set_ex_data(context, index, next.get()) != 1) { ssl_error("SSL_CTX_set_ex_data"); }
	(void)next.release();
	delete previous;
}

}  // namespace detail

// TLS 1.2 or later, no renegotiation, no 0-RTT data, a 64 KiB certificate-list cap.
// Every call reads the files; a file is parsed only when its bytes differ from the last
// parse of that path a live context holds, so unchanged files load once while a context uses
// them and a replaced one is served at once. The context holds its parses until it is freed.
// Throws std::invalid_argument when the options are incomplete for the role or the key
// does not match the certificate, std::system_error when a file is unreadable or invalid.
inline void configure(asio::ssl::context& context, const Options& options, Role role) {
	const bool certificate_required = role != Role::Client;
	const bool system_roots = options.system_roots && role != Role::Peer;
	const bool authority = !options.certificate_authority.empty() || system_roots;
	if ((role != Role::Server && !authority) || (certificate_required && (options.certificate.empty() || options.private_key.empty())) ||
		(options.certificate.empty() != options.private_key.empty()) || (options.system_roots && role == Role::Peer)) {
		throw std::invalid_argument("incomplete TLS configuration");
	}
	context.set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 | asio::ssl::context::no_sslv3 |
		asio::ssl::context::no_tlsv1 | asio::ssl::context::no_tlsv1_1);
	if (SSL_CTX_set_min_proto_version(context.native_handle(), TLS1_2_VERSION) != 1) { throw std::runtime_error("TLS minimum version"); }
	SSL_CTX_set_max_early_data(context.native_handle(), 0);
	SSL_CTX_set_options(context.native_handle(), SSL_OP_NO_RENEGOTIATION);
	SSL_CTX_set_max_cert_list(context.native_handle(), 64 * 1024);
	if (options.strict) {
		if (SSL_CTX_set_cipher_list(context.native_handle(), "ECDHE+AESGCM:ECDHE+CHACHA20:!aNULL") != 1) { throw std::runtime_error("TLS cipher list"); }
		SSL_CTX_set_options(context.native_handle(), SSL_OP_NO_TICKET);
		SSL_CTX_set_num_tickets(context.native_handle(), 0);  // with NO_TICKET alone, TLS 1.3 tickets turn stateful
		SSL_CTX_set_session_cache_mode(context.native_handle(), SSL_SESS_CACHE_OFF);
	}
	SSL_CTX* const handle = context.native_handle();
	detail::Held held;
	if (!options.certificate.empty()) {
		const auto chain = detail::load(detail::Use::Chain, options.certificate, handle, "use_certificate_chain_file");
		const auto key = detail::load(detail::Use::Key, options.private_key, handle, "use_private_key_file");
		ERR_clear_error();
		if (SSL_CTX_use_certificate(handle, chain->certificates.front()) != 1 || SSL_CTX_clear_chain_certs(handle) != 1) {
			detail::ssl_error("use_certificate_chain_file");
		}
		for (std::size_t i = 1; i < chain->certificates.size(); ++i) {
			if (SSL_CTX_add1_chain_cert(handle, chain->certificates[i]) != 1) { detail::ssl_error("use_certificate_chain_file"); }
		}
		if (SSL_CTX_use_PrivateKey(handle, key->key) != 1) { detail::ssl_error("use_private_key_file"); }
		if (SSL_CTX_check_private_key(handle) != 1) { throw std::invalid_argument("TLS key mismatch"); }
		held.push_back(chain);
		held.push_back(key);
	}
	if (!options.certificate_authority.empty()) {
		const auto authority = detail::load(detail::Use::Authority, options.certificate_authority, handle, "load_verify_file");
		X509_STORE* store = SSL_CTX_get_cert_store(handle);
		ERR_clear_error();
		for (X509* x : authority->certificates) {
			if (X509_STORE_add_cert(store, x) != 1) { detail::ssl_error("load_verify_file"); }
		}
		for (X509_CRL* crl : authority->revocations) {
			if (X509_STORE_add_crl(store, crl) != 1) { detail::ssl_error("load_verify_file"); }
		}
		held.push_back(authority);
	}
	detail::hold(handle, std::move(held));
	if (system_roots) { context.set_default_verify_paths(); }
	context.set_verify_mode(role == Role::Peer ? asio::ssl::verify_peer | asio::ssl::verify_fail_if_no_peer_cert
		: role == Role::Client                 ? asio::ssl::verify_peer
											   : asio::ssl::verify_none);
}

namespace detail {
// Seconds from now until `when`, negative when it has passed; false when the time is unreadable.
inline bool seconds_until(const ASN1_TIME* when, long& seconds) {
	int days = 0, rest = 0;
	if (ASN1_TIME_diff(&days, &rest, nullptr, when) != 1) { return false; }
	seconds = long(days) * 86400 + rest;
	return true;
}
}  // namespace detail

// A server logs a warning while its certificate expires within this (seconds_to_expiry).
inline constexpr std::chrono::seconds expiry_warning{14 * 24 * 3600};

// The context a server presents: configure() for Role::Server with `strict` set, after refusing a
// private key readable by group or others (checked before the key is read) and a certificate not
// yet valid or expired. Throws std::invalid_argument naming the file, for a file it cannot read or a
// pair that does not match too. Read the key before privileges drop.
inline std::shared_ptr<asio::ssl::context> server_context(Options options) {
	if (!options.private_key.empty()) {
		struct stat st {};
		if (::stat(options.private_key.c_str(), &st) != 0) { throw std::invalid_argument("cannot read TLS key " + options.private_key); }
		if ((st.st_mode & (S_IRWXG | S_IRWXO)) != 0) {
			throw std::invalid_argument("TLS key " + options.private_key + " is readable by group or others; make it mode 0600");
		}
	}
	options.strict = true;
	auto context = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	try {
		configure(*context, options, Role::Server);
	} catch (const std::exception& e) {
		throw std::invalid_argument("TLS certificate " + options.certificate + " or key " + options.private_key + ": " + e.what());
	}
	X509* presented = SSL_CTX_get0_certificate(context->native_handle());
	if (presented == nullptr) { throw std::invalid_argument("TLS certificate " + options.certificate + " holds no certificate"); }
	long left = 0;
	if (!detail::seconds_until(X509_get0_notBefore(presented), left) || left > 0) {
		throw std::invalid_argument("TLS certificate " + options.certificate + " is not valid yet");
	}
	if (!detail::seconds_until(X509_get0_notAfter(presented), left) || left < 0) {
		throw std::invalid_argument("TLS certificate " + options.certificate + " has expired");
	}
	return context;
}

// Seconds until the certificate `context` presents expires, negative once it has; 0 without one.
inline long seconds_to_expiry(asio::ssl::context& context) {
	const X509* presented = SSL_CTX_get0_certificate(context.native_handle());
	long left = 0;
	if (presented == nullptr || !detail::seconds_until(X509_get0_notAfter(presented), left)) { return 0; }
	return left;
}

// A client connection verifies the server's chain against the name it asked for: an IP
// address literal, or a DNS name also sent as SNI. (Peers instead pin the verified leaf
// to a configured identity.)
inline void bind_hostname(SSL* ssl, const std::string& hostname) {
	if (hostname.empty() || hostname.find('\0') != std::string::npos) { throw std::invalid_argument("TLS hostname"); }
	asio::error_code error;
	(void)asio::ip::make_address(hostname, error);
	if (!error) {
		if (X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), hostname.c_str()) != 1) { throw std::invalid_argument("TLS IP address"); }
	} else if (SSL_set_tlsext_host_name(ssl, hostname.c_str()) != 1 ||
		X509_VERIFY_PARAM_set1_host(SSL_get0_param(ssl), hostname.c_str(), hostname.size()) != 1) {
		throw std::invalid_argument("TLS hostname");
	}
}

}  // namespace reactor::tls
