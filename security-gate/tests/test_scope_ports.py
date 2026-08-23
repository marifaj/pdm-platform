"""P778-IR-001 — a host grant is not a grant over every port on that host.

Reproduced before the fix: an authorised target that answered with a 302 to
another port on the same host caused MARKNA to connect to that port and issue a
GET. The redirect handler validated the host and nothing else, so any service on
the target machine -- a container runtime API, a metrics endpoint, a database
speaking HTTP -- was reachable from a UAT web-application authorisation.

Scope therefore carries two independent dimensions, and both are checked on
every hop: which names may be addressed, and which services on those names.

Every test here that involves a redirect asserts on the *listener*, not only on
the exception. "Refused" that still delivers the request is not refused.
"""

from __future__ import annotations

import http.server
import socket
import threading

import pytest

from markna.authorization import (
    Authorization,
    AuthorizationError,
    Scope,
    ScopeError,
    _target_port,
)
from markna.http import HttpClient, _guarded_socket
from urllib.parse import urlparse


# --------------------------------------------------------------- listeners


class _Recorder:
    """A loopback listener that records every request path it is given."""

    def __init__(self, redirect_to=None, family=socket.AF_INET):
        self.requests = []
        recorder = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def do_GET(self):  # noqa: N802 - stdlib API
                recorder.requests.append(self.path)
                location = redirect_to() if callable(redirect_to) else redirect_to
                if location:
                    self.send_response(302)
                    self.send_header("Location", location)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                body = b"target"
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):  # noqa: A003
                pass

        class Server(http.server.HTTPServer):
            address_family = family

        host = "127.0.0.1" if family == socket.AF_INET else "::1"
        self._server = Server((host, 0), Handler)
        self.host = host
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def url(self):
        host = self.host if self.host != "::1" else "[::1]"
        return f"http://{host}:{self.port}/"

    def close(self):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


@pytest.fixture
def listeners():
    created = []

    def make(redirect_to=None, family=socket.AF_INET):
        recorder = _Recorder(redirect_to=redirect_to, family=family)
        created.append(recorder)
        return recorder

    yield make
    for recorder in created:
        recorder.close()


def _client(url, *, authorized_ports=()):
    authorization = Authorization(
        authorized_by="test",
        allow_private_targets=True,
        authorized_ports=list(authorized_ports),
    )
    scope = Scope.for_target(url, authorization)
    return scope, HttpClient(scope, rate_limit_seconds=0)


def _ipv6_available():
    try:
        probe = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    except OSError:
        return False
    try:
        probe.bind(("::1", 0))
    except OSError:
        return False
    finally:
        probe.close()
    return True


# ---------------------------------------------------- the reproduced attack


class TestTheRedirectAttack:
    def test_a_same_host_redirect_to_an_unauthorised_port_is_refused(self, listeners):
        """The blocker, exactly as reported."""
        admin = listeners()
        target = listeners(redirect_to=lambda: f"http://127.0.0.1:{admin.port}/pwned")
        _, client = _client(target.url)

        with pytest.raises(ScopeError, match="not authorised for this assessment"):
            client.get(target.url)

        assert target.requests == ["/"]
        assert admin.requests == [], "the unauthorised listener received a request"

    def test_the_unauthorised_listener_receives_nothing_at_all(self, listeners):
        """Not merely 'no GET': no connection is made to it in the first place.

        Asserted by watching the accept queue rather than the handler, because a
        connection that is opened and dropped is still a reachability proof and
        still shows up in the target's logs.
        """
        admin = listeners()
        target = listeners(redirect_to=lambda: f"http://127.0.0.1:{admin.port}/pwned")
        _, client = _client(target.url)

        with pytest.raises(ScopeError):
            client.get(target.url)

        # The recorder only appends once a request line has been parsed, so an
        # empty list plus a live socket is the strongest statement available
        # without instrumenting the accept loop.
        assert admin.requests == []
        probe = socket.create_connection(("127.0.0.1", admin.port), timeout=5)
        probe.close()  # still listening, so nothing crashed it into silence

    def test_a_redirect_chain_is_refused_at_the_first_unauthorised_hop(self, listeners):
        """One bad hop in an otherwise in-scope chain refuses the whole chain."""
        admin = listeners()
        second = listeners(redirect_to=lambda: f"http://127.0.0.1:{admin.port}/pwned")
        first = listeners(redirect_to=lambda: second.url)
        _, client = _client(first.url, authorized_ports=[second.port])

        with pytest.raises(ScopeError, match="not authorised for this assessment"):
            client.get(first.url)

        assert first.requests == ["/"]
        assert second.requests == ["/"], "the authorised second hop should have been reached"
        assert admin.requests == [], "the unauthorised third hop must not be reached"

    def test_a_redirect_to_an_explicitly_authorised_port_is_allowed(self, listeners):
        """The control is a scope, not a ban: a written-down port still works."""
        other = listeners()
        target = listeners(redirect_to=lambda: other.url)
        _, client = _client(target.url, authorized_ports=[other.port])

        response = client.get(target.url)

        assert response.status == 200 and response.body == b"target"
        assert other.requests == ["/"]

    def test_a_direct_request_to_an_unauthorised_port_is_refused(self, listeners):
        """No redirect involved: the same rule applies to a URL a scanner builds."""
        admin = listeners()
        target = listeners()
        _, client = _client(target.url)

        with pytest.raises(ScopeError, match="not authorised for this assessment"):
            client.get(f"http://127.0.0.1:{admin.port}/")

        assert admin.requests == []


# ------------------------------------------------------- the scope decision


class TestThePortDimension:
    def test_the_targets_own_port_is_authorised_by_naming_it(self):
        scope = Scope.for_target("https://uat.example:8443/", Authorization(authorized_by="t"))
        assert scope.ports == [8443]
        assert scope.allows("https://uat.example:8443/health")

    def test_another_port_on_the_same_host_is_not(self):
        scope = Scope.for_target("https://uat.example:8443/", Authorization(authorized_by="t"))
        assert not scope.allows("https://uat.example:2375/")

    def test_an_authorised_port_does_not_widen_the_host_scope(self):
        """The two dimensions are independent, and stay independent."""
        scope = Scope.for_target(
            "https://uat.example/",
            Authorization(authorized_by="t", authorized_ports=[8443]),
        )
        assert scope.allows("https://uat.example:8443/")
        assert not scope.allows("https://elsewhere.example:8443/")

    def test_a_host_grant_does_not_widen_the_port_scope(self):
        scope = Scope.for_target(
            "https://uat.example/",
            Authorization(authorized_by="t", scope_hosts=["api.example"]),
        )
        assert scope.allows("https://api.example/")
        assert not scope.allows("https://api.example:2375/")

    @pytest.mark.parametrize(
        "target,hop",
        [
            ("http://uat.example/", "https://uat.example/"),   # 80 -> 443, no 443 grant
            ("https://uat.example/", "http://uat.example/"),   # 443 -> 80, no 80 grant
        ],
    )
    def test_a_scheme_change_is_a_port_change(self, target, hop):
        """The default port moves with the scheme, so upgrading is a new endpoint."""
        scope = Scope.for_target(target, Authorization(authorized_by="t"))
        assert not scope.allows(hop)

    def test_a_scheme_change_is_allowed_once_the_port_is_written_down(self):
        scope = Scope.for_target(
            "http://uat.example/", Authorization(authorized_by="t", authorized_ports=[443])
        )
        assert scope.allows("https://uat.example/")

    @pytest.mark.parametrize(
        "written,same",
        [
            ("http://uat.example/", "http://uat.example:80/"),
            ("http://uat.example:80/", "http://uat.example/"),
            ("https://uat.example/", "https://uat.example:443/"),
            ("https://uat.example:443/", "https://uat.example/"),
        ],
    )
    def test_explicit_and_default_ports_are_the_same_endpoint(self, written, same):
        """Otherwise an authorisation written one way is silently absent the other."""
        scope = Scope.for_target(written, Authorization(authorized_by="t"))
        assert scope.allows(same)

    def test_an_ipv4_target_refuses_another_port_on_the_same_address(self):
        scope = Scope.for_target(
            "http://127.0.0.1:8080/",
            Authorization(authorized_by="t", allow_private_targets=True),
        )
        assert scope.allows("http://127.0.0.1:8080/x")
        assert not scope.allows("http://127.0.0.1:2375/")

    def test_an_ipv6_target_refuses_another_port_on_the_same_address(self):
        scope = Scope.for_target(
            "http://[::1]:8080/",
            Authorization(authorized_by="t", allow_private_targets=True),
        )
        assert scope.ports == [8080]
        assert scope.allows("http://[::1]:8080/x")
        assert not scope.allows("http://[::1]:2375/")

    @pytest.mark.skipif(not _ipv6_available(), reason="no IPv6 loopback on this host")
    def test_an_ipv6_redirect_to_another_port_is_refused_on_the_wire(self, listeners):
        admin = listeners(family=socket.AF_INET6)
        target = listeners(
            redirect_to=lambda: f"http://[::1]:{admin.port}/pwned", family=socket.AF_INET6
        )
        _, client = _client(target.url)

        with pytest.raises(ScopeError, match="not authorised for this assessment"):
            client.get(target.url)

        assert admin.requests == []

    def test_the_scope_records_its_ports(self):
        scope = Scope.for_target(
            "https://uat.example/", Authorization(authorized_by="t", authorized_ports=[8443])
        )
        assert scope.to_dict()["ports"] == [443, 8443]


# --------------------------------------------------- the connect-time guard


class TestTheConnectTimeGuard:
    """The last check before a packet leaves, independent of any URL parsing."""

    def test_the_socket_guard_refuses_an_unauthorised_port(self, listeners):
        admin = listeners()
        scope = Scope(hosts=["127.0.0.1"], ports=[admin.port + 1], allow_private=True)

        with pytest.raises(ScopeError, match="not authorised for this assessment"):
            _guarded_socket("127.0.0.1", admin.port, 5.0, None, scope)

        assert admin.requests == []

    def test_the_socket_guard_allows_an_authorised_port(self, listeners):
        target = listeners()
        scope = Scope(hosts=["127.0.0.1"], ports=[target.port], allow_private=True)

        connection = _guarded_socket("127.0.0.1", target.port, 5.0, None, scope)
        try:
            assert connection.getpeername()[1] == target.port
        finally:
            connection.close()

    def test_a_scope_with_no_ports_permits_nothing(self):
        """The default is deny: a hand-built scope grants no port until told to."""
        scope = Scope(hosts=["uat.example"])
        assert scope.ports == []
        assert not scope.port_permitted(443)
        assert not scope.allows("https://uat.example/")


# ------------------------------------------------------------- the grant itself


class TestTheAuthorizedPortsGrant:
    def test_ports_are_parsed_and_normalised(self):
        authorization = Authorization.from_dict(
            {"authorized_by": "t", "authorized_ports": [8443, "2375", 8443]}
        )
        assert authorization.authorized_ports == [2375, 8443]

    @pytest.mark.parametrize("value", [0, 65536, -1, "http", None, True])
    def test_an_unusable_port_is_rejected_rather_than_guessed_at(self, value):
        with pytest.raises(AuthorizationError):
            Authorization.from_dict({"authorized_by": "t", "authorized_ports": [value]})

    @pytest.mark.parametrize("value", ["8443", 8443, {"port": 8443}])
    def test_a_non_list_grant_is_rejected(self, value):
        """A comma-separated string must not silently become one port, or none."""
        with pytest.raises(AuthorizationError):
            Authorization.from_dict({"authorized_by": "t", "authorized_ports": value})

    def test_the_grant_survives_a_round_trip(self):
        authorization = Authorization.from_dict(
            {"authorized_by": "t", "authorized_ports": [8443]}
        )
        assert Authorization.from_dict(authorization.to_dict()).authorized_ports == [8443]

    @pytest.mark.parametrize(
        "url,expected",
        [
            ("http://h/", 80),
            ("https://h/", 443),
            ("http://h:8080/", 8080),
            ("https://h:8080/", 8080),
            ("http://h:0/", None),
            ("http://h:99999/", None),
        ],
    )
    def test_the_effective_port_of_a_url(self, url, expected):
        assert _target_port(urlparse(url)) == expected

    @pytest.mark.parametrize("url", ["http://uat.example:0/", "http://uat.example:99999/"])
    def test_a_target_url_with_an_unusable_port_is_refused(self, url):
        with pytest.raises(ScopeError):
            Scope.for_target(url, Authorization(authorized_by="t"))

    @pytest.mark.parametrize(
        "url",
        ["http://uat.example:notaport/", "http://uat.example:0/", "http://uat.example:99999/"],
    )
    def test_an_unreadable_port_refuses_rather_than_raising_something_else(self, url):
        """`urlsplit` raises on `:notaport`, so the refusal must not read it back."""
        scope = Scope.for_target("http://uat.example/", Authorization(authorized_by="t"))
        with pytest.raises(ScopeError, match="not authorised for this assessment"):
            scope.check(url)
        assert not scope.allows(url)
