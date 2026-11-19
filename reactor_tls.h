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

#include <cerrno>
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

// One PEM file's bytes as last parsed, and what they parsed to. Contexts configured from the
// same bytes share these objects, which are only read once published.
struct Pem {
	std::string bytes;
	std::vector<X509*> certificates;
	std::vector<X509_CRL*> revocations;
	EVP_PKEY* key = nullptr;
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

// How a file is parsed: a certificate chain (leaf first), a private key, or an authority's
// certificates and revocation lists.
enum class Use { Chain, Key, Authority };

[[noreturn]] inline void ssl_error(const char* what) {
	throw std::system_error(asio::error_code(static_cast<int>(ERR_get_error()), asio::error::get_ssl_category()), what);
}

inline std::string read(const std::string& path, const char* what) {
	std::FILE* file = std::fopen(path.c_str(), "rb");
	if (!file) { throw std::system_error(errno, std::generic_category(), what); }
	std::string bytes;
	char buffer[8192];
	for (std::size_t n; (n = std::fread(buffer, 1, sizeof buffer, file)) > 0;) { bytes.append(buffer, n); }
	const bool failed = std::ferror(file) != 0;
	std::fclose(file);
	OPENSSL_cleanse(buffer, sizeof buffer);
	if (failed) { throw std::system_error(EIO, std::generic_category(), what); }
	return bytes;
}

// Parses as OpenSSL's file loaders do (SSL_CTX_use_certificate_chain_file,
// SSL_CTX_use_PrivateKey_file, X509_load_cert_crl_file), from memory, with the context's
// password callback. Each certificate's extensions are decoded before it is shared, so
// handshakes on other threads only read them.
inline std::shared_ptr<const Pem> parse(Use use, std::string bytes, SSL_CTX* context, const char* what) {
	auto pem = std::make_shared<Pem>();
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
		pem->key = PEM_read_bio_PrivateKey(bio.get(), nullptr, password, password_data);
		if (!pem->key) { ssl_error(what); }
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

// The file's parse: reused while its bytes are those last parsed for this path and use,
// parsed again (and kept) when they changed.
inline std::shared_ptr<const Pem> load(Use use, const std::string& path, SSL_CTX* context, const char* what) {
	static std::mutex mutex;
	static std::map<std::pair<Use, std::string>, std::shared_ptr<const Pem>> cache;
	std::string bytes = read(path, what);
	{
		std::lock_guard lock(mutex);
		if (auto it = cache.find({use, path}); it != cache.end() && it->second->bytes == bytes) {
			OPENSSL_cleanse(bytes.data(), bytes.size());
			return it->second;
		}
	}
	auto pem = parse(use, std::move(bytes), context, what);
	std::lock_guard lock(mutex);
	cache[{use, path}] = pem;
	return pem;
}

}  // namespace detail

// TLS 1.2 or later, no renegotiation, no 0-RTT data, a 64 KiB certificate-list cap.
// Every call reads the files; a file is parsed only when its bytes differ from the last
// parse of that path, so unchanged files load once and a replaced one is served at once.
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
	}
	if (system_roots) { context.set_default_verify_paths(); }
	context.set_verify_mode(role == Role::Peer ? asio::ssl::verify_peer | asio::ssl::verify_fail_if_no_peer_cert
		: role == Role::Client                 ? asio::ssl::verify_peer
											   : asio::ssl::verify_none);
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
