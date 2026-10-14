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
#include <openssl/x509.h>

#include <stdexcept>
#include <string>

namespace reactor::tls {

// PEM files: this end's certificate chain and key, and the authority that signs peers.
struct Options {
	std::string certificate, private_key, certificate_authority;
};

// Peer: mutual TLS, both ends present certificates. Server: presents one and does not
// ask the client for one. Client: verifies the server and may present one.
enum class Role { Peer, Server, Client };

// TLS 1.2 or later, no renegotiation, no 0-RTT data, a 64 KiB certificate-list cap.
// Throws std::invalid_argument when the options are incomplete for the role or the key
// does not match the certificate.
inline void configure(asio::ssl::context& context, const Options& options, Role role) {
	const bool certificate_required = role != Role::Client;
	if (options.certificate_authority.empty() || (certificate_required && (options.certificate.empty() || options.private_key.empty())) ||
		(options.certificate.empty() != options.private_key.empty())) {
		throw std::invalid_argument("incomplete TLS configuration");
	}
	context.set_options(asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 | asio::ssl::context::no_sslv3 |
		asio::ssl::context::no_tlsv1 | asio::ssl::context::no_tlsv1_1);
	if (SSL_CTX_set_min_proto_version(context.native_handle(), TLS1_2_VERSION) != 1) { throw std::runtime_error("TLS minimum version"); }
	SSL_CTX_set_max_early_data(context.native_handle(), 0);
	SSL_CTX_set_options(context.native_handle(), SSL_OP_NO_RENEGOTIATION);
	SSL_CTX_set_max_cert_list(context.native_handle(), 64 * 1024);
	if (!options.certificate.empty()) {
		context.use_certificate_chain_file(options.certificate);
		context.use_private_key_file(options.private_key, asio::ssl::context::pem);
		if (SSL_CTX_check_private_key(context.native_handle()) != 1) { throw std::invalid_argument("TLS key mismatch"); }
	}
	context.load_verify_file(options.certificate_authority);
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
