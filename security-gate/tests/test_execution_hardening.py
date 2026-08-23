"""H3 and H5 — destination enforcement at connect time, and scanner environments.

H3  A hostname check happens before name resolution, so it can be defeated by a
    name that answers differently the second time. The address check happens on
    the exact address the socket will dial, so it cannot.
H5  Scanners are large third-party programs that parse hostile input. They get
    an allow-listed environment, not the worker's.
"""

from __future__ import annotations

import http.server
import os
import socket
import threading

import pytest

from markna.authorization import Authorization, Scope, ScopeError
from markna.exec import SAFE_ENV_NAMES, SAFE_ENV_PREFIXES, ToolPath, run_command
from markna.http import HttpClient, _guarded_socket


# --------------------------------------------------------------------- H3


@pytest.fixture
def local_server():
    """A real HTTP server on loopback — a stand-in for an internal target."""

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib API
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"internal")

        def log_message(self, *args):  # noqa: A003
            pass

    httpd = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield httpd.server_address
    httpd.shutdown()
    httpd.server_close()
    thread.join(timeout=5)


class TestConnectTimeDestinationEnforcement:
    def test_a_loopback_address_is_refused_at_connect_time(self, local_server):
        """The hostname passed the earlier check; the resolved address does not."""
        host, port = local_server
        scope = Scope(hosts=["localhost", "127.0.0.1"], ports=[port], allow_private=False)
        with pytest.raises(ScopeError, match="non-public|out-of-scope address"):
            _guarded_socket("127.0.0.1", port, 5.0, None, scope)

    def test_a_name_resolving_to_loopback_is_refused(self, local_server):
        """This is the DNS-rebinding shape: the name is allowed, the answer is not."""
        _, port = local_server
        scope = Scope(hosts=["localhost"], ports=[port], allow_private=False)
        with pytest.raises(ScopeError, match="non-public|out-of-scope address"):
            _guarded_socket("localhost", port, 5.0, None, scope)

    def test_an_authorised_internal_target_still_connects(self, local_server):
        host, port = local_server
        scope = Scope(hosts=["127.0.0.1"], ports=[port], allow_private=True)
        connection = _guarded_socket(host, port, 5.0, None, scope)
        try:
            assert connection.getpeername()[1] == port
        finally:
            connection.close()

    def test_the_client_reaches_an_authorised_internal_target(self, local_server):
        host, port = local_server
        authorization = Authorization(authorized_by="test", allow_private_targets=True)
        url = f"http://{host}:{port}/"
        client = HttpClient(Scope.for_target(url, authorization), rate_limit_seconds=0)
        response = client.get(url)
        assert response.status == 200 and response.body == b"internal"

    def test_the_client_refuses_the_same_target_without_that_permission(self, local_server):
        host, port = local_server
        scope = Scope(hosts=[host], ports=[port], allow_private=False)
        client = HttpClient(scope, rate_limit_seconds=0)
        with pytest.raises(ScopeError, match="private, loopback"):
            client.get(f"http://{host}:{port}/")


class TestAddressPolicy:
    @pytest.mark.parametrize(
        "address",
        ["127.0.0.1", "10.0.0.5", "192.168.1.1", "172.16.0.1", "169.254.169.254", "::1", "fe80::1"],
    )
    def test_non_public_addresses_are_refused(self, address):
        assert not Scope(hosts=["x"], allow_private=False).address_allowed(address)

    @pytest.mark.parametrize("address", ["93.184.216.34", "2606:2800:220:1:248:1893:25c8:1946"])
    def test_public_addresses_are_allowed(self, address):
        assert Scope(hosts=["x"], allow_private=False).address_allowed(address)

    def test_the_cloud_metadata_address_is_refused(self):
        """169.254.169.254 is the address SSRF exists to reach."""
        assert not Scope(hosts=["x"], allow_private=False).address_allowed("169.254.169.254")

    def test_a_non_address_is_refused(self):
        assert not Scope(hosts=["x"], allow_private=False).address_allowed("not-an-ip")

    def test_an_authorised_internal_assessment_allows_them(self):
        assert Scope(hosts=["x"], allow_private=True).address_allowed("10.0.0.5")

    def test_require_address_names_what_it_refused(self):
        with pytest.raises(ScopeError) as excinfo:
            Scope(hosts=["x"], allow_private=False).require_address("uat.example", "10.1.2.3")
        assert "10.1.2.3" in str(excinfo.value)


# --------------------------------------------------------------------- H5

SENSITIVE = {
    "ANTHROPIC_API_KEY": "sk-ant-not-a-real-key",
    "MARKNA_DATABASE": "/var/lib/markna/markna.db",
    "MARKNA_SECRET_KEY": "server-signing-key",
    "AWS_SECRET_ACCESS_KEY": "aws-secret",
    "GITHUB_TOKEN": "ghp_token",
    "DATABASE_URL": "postgres://user:pw@host/db",
}


@pytest.fixture
def poisoned_environment(monkeypatch):
    for name, value in SENSITIVE.items():
        monkeypatch.setenv(name, value)
    return SENSITIVE


class TestScannerEnvironment:
    def test_no_sensitive_variable_is_passed_through(self, poisoned_environment):
        env = ToolPath().environ()
        leaked = sorted(name for name in poisoned_environment if name in env)
        assert leaked == [], f"scanner subprocesses must not see {leaked}"

    def test_the_environment_is_built_from_the_allow_list(self, poisoned_environment):
        env = ToolPath().environ()
        unexpected = [
            name
            for name in env
            if name not in SAFE_ENV_NAMES and not name.startswith(SAFE_ENV_PREFIXES)
        ]
        assert unexpected == [], f"unexpected names reached the scanner environment: {unexpected}"

    def test_path_is_present_so_tools_can_be_found(self):
        assert ToolPath().environ()["PATH"]

    def test_extra_tool_paths_are_prepended(self, tmp_path):
        env = ToolPath(extra_paths=[str(tmp_path)]).environ()
        assert env["PATH"].startswith(str(tmp_path))

    def test_tls_trust_survives(self, monkeypatch):
        monkeypatch.setenv("SSL_CERT_FILE", "/etc/ssl/cert.pem")
        assert ToolPath().environ()["SSL_CERT_FILE"] == "/etc/ssl/cert.pem"

    def test_per_tool_configuration_survives(self, monkeypatch):
        monkeypatch.setenv("SEMGREP_RULES_CACHE_DIR", "/cache")
        assert ToolPath().environ()["SEMGREP_RULES_CACHE_DIR"] == "/cache"

    def test_an_adapter_can_still_pass_what_it_needs(self):
        env = ToolPath().environ({"SEMGREP_ENABLE_VERSION_CHECK": "0"})
        assert env["SEMGREP_ENABLE_VERSION_CHECK"] == "0"

    def test_a_real_subprocess_does_not_receive_the_secret(self, poisoned_environment):
        """End to end: run a child and read back what it actually saw."""
        result = run_command(
            [
                "python3",
                "-c",
                "import os,json;print(json.dumps(sorted(os.environ)))",
            ],
            timeout=30,
        )
        assert result.ok, result.failure_message()
        seen = set(__import__("json").loads(result.stdout))
        assert not (seen & set(poisoned_environment)), "the child inherited a secret"

    def test_the_parent_environment_is_not_mutated(self, poisoned_environment):
        ToolPath().environ()
        assert os.environ["ANTHROPIC_API_KEY"] == SENSITIVE["ANTHROPIC_API_KEY"]
