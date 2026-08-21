"""M4 and M1 — request parsing and API CSRF / content-type enforcement.

The parsing tests run at two levels. The unit level drives ``Request`` directly.
The socket level starts the real application on a real port and writes malformed
HTTP bytes at it, because the property under test — "a hostile Content-Length
produces a clean status line, not an unhandled exception" — is a property of the
whole stack, and a WSGI dict built in a test cannot demonstrate it.
"""

from __future__ import annotations

import io
import socket
import threading
from typing import Dict, Tuple
from wsgiref.simple_server import WSGIRequestHandler, make_server

import pytest

from conftest import Client
from markna_server.api.wsgi import (
    CSRF_HEADER,
    BadRequest,
    PayloadTooLarge,
    Request,
    UnsupportedMediaType,
)



def environ(body: bytes = b"", **overrides) -> Dict[str, object]:
    base: Dict[str, object] = {
        "REQUEST_METHOD": "POST",
        "PATH_INFO": "/api/v1/projects",
        "QUERY_STRING": "",
        "wsgi.input": io.BytesIO(body),
        "CONTENT_LENGTH": str(len(body)),
        "CONTENT_TYPE": "application/json",
        "REMOTE_ADDR": "127.0.0.1",
    }
    base.update(overrides)
    return base


# ----------------------------------------------------------------- M4 unit


class TestContentLengthParsing:
    def test_a_negative_length_is_rejected(self):
        with pytest.raises(BadRequest):
            Request.from_environ(environ(b"payload", CONTENT_LENGTH="-1"))

    def test_a_large_negative_length_is_rejected(self):
        with pytest.raises(BadRequest):
            Request.from_environ(environ(b"payload", CONTENT_LENGTH="-9999999999"))

    def test_a_non_numeric_length_is_rejected(self):
        with pytest.raises(BadRequest):
            Request.from_environ(environ(b"payload", CONTENT_LENGTH="abc"))

    def test_a_float_length_is_rejected(self):
        with pytest.raises(BadRequest):
            Request.from_environ(environ(b"payload", CONTENT_LENGTH="12.5"))

    def test_a_length_with_a_sign_and_padding_still_parses(self):
        request = Request.from_environ(environ(b"hello", CONTENT_LENGTH=" 5 "))
        assert request.body == b"hello"

    def test_an_absent_length_reads_nothing(self):
        request = Request.from_environ(environ(b"ignored", CONTENT_LENGTH=""))
        assert request.body == b""

    def test_a_negative_length_never_drains_the_stream(self):
        """`read(-1)` on a socket-backed stream reads until EOF; that is the bug."""
        stream = io.BytesIO(b"x" * 10_000_000)
        with pytest.raises(BadRequest):
            Request.from_environ(environ(CONTENT_LENGTH="-1", **{"wsgi.input": stream}))
        assert stream.tell() == 0, "nothing may be read before the length is validated"


class TestRequestSizeLimit:
    def test_a_declared_oversize_body_is_refused(self):
        with pytest.raises(PayloadTooLarge):
            Request.from_environ(environ(b"x" * 100, CONTENT_LENGTH="999999999"), max_body_bytes=64)

    def test_a_lying_content_length_does_not_smuggle_a_large_body(self):
        """Declare 10 bytes, send 5000. The read, not the header, is the limit."""
        stream = io.BytesIO(b"x" * 5000)
        with pytest.raises(PayloadTooLarge):
            Request.from_environ(
                environ(CONTENT_LENGTH="10", **{"wsgi.input": stream}), max_body_bytes=10
            )

    def test_a_body_exactly_at_the_limit_is_accepted(self):
        payload = b"x" * 64
        request = Request.from_environ(environ(payload), max_body_bytes=64)
        assert len(request.body) == 64

    def test_the_limit_comes_from_configuration(self, config):
        assert config.max_request_bytes > 0

    def test_the_limit_cannot_be_configured_to_zero(self):
        from markna_server.config import ServerConfig

        with pytest.raises(ValueError):
            ServerConfig(max_request_bytes=0).validate()


class TestJsonContentType:
    def test_a_form_content_type_is_refused_on_a_json_endpoint(self):
        request = Request.from_environ(
            environ(b'{"a":1}', CONTENT_TYPE="application/x-www-form-urlencoded")
        )
        with pytest.raises(UnsupportedMediaType):
            request.json()

    def test_a_text_plain_body_is_refused(self):
        request = Request.from_environ(environ(b'{"a":1}', CONTENT_TYPE="text/plain"))
        with pytest.raises(UnsupportedMediaType):
            request.json()

    def test_a_missing_content_type_with_a_body_is_refused(self):
        request = Request.from_environ(environ(b'{"a":1}', CONTENT_TYPE=""))
        with pytest.raises(UnsupportedMediaType):
            request.json()

    def test_a_charset_parameter_is_tolerated(self):
        request = Request.from_environ(
            environ(b'{"a":1}', CONTENT_TYPE="application/json; charset=utf-8")
        )
        assert request.json() == {"a": 1}

    def test_malformed_json_is_a_bad_request_not_a_crash(self):
        request = Request.from_environ(environ(b"{not json"))
        with pytest.raises(BadRequest):
            request.json()


# --------------------------------------------------------------- M4 socket


class _QuietHandler(WSGIRequestHandler):
    def log_message(self, *args):  # noqa: A003 - silence the test server
        pass


@pytest.fixture
def server(app):
    """The real application, on a real socket."""
    httpd = make_server("127.0.0.1", 0, app, handler_class=_QuietHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield httpd.server_address
    httpd.shutdown()
    httpd.server_close()
    thread.join(timeout=5)


def raw_request(address: Tuple[str, int], payload: bytes, timeout: float = 5.0) -> bytes:
    connection = socket.create_connection(address, timeout=timeout)
    try:
        connection.sendall(payload)
        chunks = []
        while True:
            chunk = connection.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)
    except socket.timeout:  # pragma: no cover - a hung server is the failure
        return b"".join(chunks)
    finally:
        connection.close()


def status_of(response: bytes) -> int:
    if not response.startswith(b"HTTP/"):
        return 0
    return int(response.split(b" ", 2)[1])


class TestMalformedRequestsOverASocket:
    def test_a_negative_content_length_gets_a_status_line(self, server):
        response = raw_request(
            server,
            b"POST /api/v1/projects HTTP/1.1\r\nHost: x\r\nContent-Length: -1\r\n"
            b"Content-Type: application/json\r\n\r\n",
        )
        assert status_of(response) in (400, 411), (
            "a negative Content-Length must produce a clean response, not a dropped connection"
        )

    def test_a_non_numeric_content_length_gets_a_status_line(self, server):
        response = raw_request(
            server,
            b"POST /api/v1/projects HTTP/1.1\r\nHost: x\r\nContent-Length: abc\r\n"
            b"Content-Type: application/json\r\n\r\n",
        )
        assert status_of(response) in (400, 411)

    def test_an_oversize_declared_body_is_refused_before_it_is_read(self, server):
        response = raw_request(
            server,
            b"POST /api/v1/projects HTTP/1.1\r\nHost: x\r\n"
            b"Content-Length: 99999999\r\nContent-Type: application/json\r\n\r\n{}",
        )
        assert status_of(response) in (400, 401, 413)

    def test_the_server_survives_a_malformed_request(self, server):
        raw_request(
            server,
            b"POST /api/v1/projects HTTP/1.1\r\nHost: x\r\nContent-Length: -1\r\n\r\n",
        )
        healthy = raw_request(server, b"GET /api/v1/health HTTP/1.1\r\nHost: x\r\n\r\n")
        assert status_of(healthy) == 200, "one malformed request must not take the server down"

    def test_a_well_formed_request_still_works_over_the_socket(self, server):
        response = raw_request(server, b"GET /api/v1/health HTTP/1.1\r\nHost: x\r\n\r\n")
        assert status_of(response) == 200
        assert b'"status"' in response


# ----------------------------------------------------------------- M1 CSRF


class TestApiCsrf:
    def _session_client(self, app, services, organization) -> Client:
        from conftest import make_user

        make_user(services, organization, email="csrf@example.com", password="a-long-password")
        client = Client(app)
        client.login("csrf@example.com", "a-long-password")
        return client

    def test_a_session_write_without_the_header_is_refused(self, app, services, organization):
        client = self._session_client(app, services, organization)
        status, payload = client.post_json(
            "/api/v1/projects", {"slug": "x", "name": "X", "source": {"local_path": "demo"}}
        )
        assert status == 403
        assert payload["error"]["code"] == "csrf_failed"

    def test_a_session_write_with_the_header_is_allowed(self, app, services, organization):
        client = self._session_client(app, services, organization)
        status, _ = client.post_json(
            "/api/v1/projects",
            {"slug": "x", "name": "X", "source": {"local_path": "demo"}},
            headers={CSRF_HEADER: client.csrf()},
        )
        assert status in (200, 201)

    def test_a_forged_csrf_token_is_refused(self, app, services, organization):
        client = self._session_client(app, services, organization)
        status, _ = client.post_json(
            "/api/v1/projects",
            {"slug": "x", "name": "X", "source": {"local_path": "demo"}},
            headers={CSRF_HEADER: "not-the-token"},
        )
        assert status == 403

    def test_a_session_read_needs_no_header(self, app, services, organization):
        client = self._session_client(app, services, organization)
        assert client.get_json("/api/v1/projects")[0] == 200

    def test_a_token_client_is_exempt(self, api_client):
        """A bearer token is not an ambient credential, so it cannot be forged cross-site."""
        status, _ = api_client.post_json(
            "/api/v1/projects", {"slug": "tok", "name": "Tok", "source": {"local_path": "demo"}}
        )
        assert status in (200, 201)

    def test_a_cross_site_form_post_cannot_reach_a_json_endpoint(self, app, services, organization):
        client = self._session_client(app, services, organization)
        status, _, _ = client.request(
            "POST",
            "/api/v1/projects",
            b"slug=x&name=X",
            content_type="application/x-www-form-urlencoded",
        )
        assert status in (403, 415), "a simple cross-site form post must not be accepted"
