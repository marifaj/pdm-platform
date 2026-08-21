"""Environment layer, exercised against a local throwaway HTTP server.

The server is deliberately insecure so each scanner has something to find, and
deliberately local so the tests never touch a third party.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from markna.authorization import Authorization, Scope
from markna.http import HttpClient
from markna.models import Severity
from markna.scanners.base import ScannerContext
from markna.scanners.env_exposure import ExposureScanner
from markna.scanners.env_http import CorsScanner, HttpHeadersScanner, HttpMethodsScanner
from markna.scanners.env_tls import TlsScanner

_HOME = b"<html><head><title>App</title></head><body>Hello</body></html>"


class _Handler(BaseHTTPRequestHandler):
    server_version = "TestServer/9.9.9"
    sys_version = ""

    def do_GET(self):  # noqa: N802 - stdlib API
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            self._send(200, _HOME, "text/html", cookie=True)
        elif path == "/.env":
            self._send(200, b"DATABASE_PASSWORD=fixture\nAPI_KEY=fixture\n", "text/plain")
        elif path == "/.git/HEAD":
            self._send(200, b"ref: refs/heads/main\n", "text/plain")
        elif path == "/metrics":
            self._send(200, b"# HELP up Up\n# TYPE up gauge\nup 1\n", "text/plain")
        else:
            self._send(404, b"not found", "text/plain")

    def do_HEAD(self):  # noqa: N802 - stdlib API
        self._send(200, b"", "text/html")

    def do_OPTIONS(self):  # noqa: N802 - stdlib API
        self.send_response(200)
        self.send_header("Allow", "GET, HEAD, OPTIONS, TRACE, PUT")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _send(self, status, body, content_type, cookie=False):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Powered-By", "TestFramework/1.2.3")
        if cookie:
            self.send_header("Set-Cookie", "sessionid=abc123; Path=/")
        origin = self.headers.get("Origin")
        if origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Access-Control-Allow-Credentials", "true")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def log_message(self, *args):  # keep the test output clean
        return


@pytest.fixture(scope="module")
def server():
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()
    httpd.server_close()


@pytest.fixture
def ctx(server, tmp_path) -> ScannerContext:
    authorization = Authorization.from_dict(
        {"authorized_by": "pytest", "allow_private_targets": True}
    )
    scope = Scope.for_target(server, authorization)
    return ScannerContext(
        workdir=tmp_path,
        target_url=server,
        authorization=authorization,
        scope=scope,
        http=HttpClient(scope, rate_limit_seconds=0.0),
    )


def rule_ids(findings) -> set:
    return {finding.rule_id for finding in findings}


class TestApplicability:
    def test_environment_scanners_refuse_to_run_without_authorization(self, tmp_path, server):
        bare = ScannerContext(workdir=tmp_path, target_url=server)
        applicable, reason = HttpHeadersScanner().applicable(bare)
        assert not applicable
        assert "authorisation" in reason

    def test_environment_scanners_skip_without_a_url(self, tmp_path):
        applicable, reason = TlsScanner().applicable(ScannerContext(workdir=tmp_path))
        assert not applicable
        assert "URL" in reason


class TestHeadersAndCookies:
    def test_missing_headers_are_reported(self, ctx):
        ids = rule_ids(HttpHeadersScanner().scan(ctx))
        assert "env-http/csp-missing" in ids
        assert "env-http/no-framing-protection" in ids
        assert "env-http/no-sniff-missing" in ids
        assert "env-http/referrer-policy-missing" in ids

    def test_hsts_is_not_expected_on_plaintext(self, ctx):
        assert "env-http/hsts-missing" not in rule_ids(HttpHeadersScanner().scan(ctx))

    def test_version_banners_are_reported(self, ctx):
        findings = [
            f for f in HttpHeadersScanner().scan(ctx) if f.rule_id == "env-http/version-disclosure"
        ]
        assert findings and "TestFramework/1.2.3" in findings[0].evidence

    def test_session_cookie_without_httponly_is_high(self, ctx):
        findings = [
            f
            for f in HttpHeadersScanner().scan(ctx)
            if f.rule_id == "env-http/cookie/httponly-missing"
        ]
        assert findings and findings[0].severity is Severity.HIGH

    def test_cookie_without_samesite_is_reported(self, ctx):
        assert "env-http/cookie/samesite-missing" in rule_ids(HttpHeadersScanner().scan(ctx))


class TestCors:
    def test_reflected_origin_with_credentials_is_high(self, ctx):
        findings = list(CorsScanner().scan(ctx))
        assert findings
        assert findings[0].rule_id == "env-cors/reflected-origin-with-credentials"
        assert findings[0].severity is Severity.HIGH
        assert "markna-security-gate.invalid" in findings[0].evidence


class TestMethods:
    def test_risky_methods_are_reported_without_being_invoked(self, ctx):
        findings = list(HttpMethodsScanner().scan(ctx))
        assert findings and "TRACE" in findings[0].title
        # Only safe verbs may ever be sent.
        assert {method for method, _, _ in ctx.http.request_log} <= {"GET", "HEAD", "OPTIONS"}


class TestExposure:
    def test_known_sensitive_paths_are_found(self, ctx):
        ids = rule_ids(ExposureScanner().scan(ctx))
        assert "env-exposure/.git/HEAD" in ids
        assert "env-exposure/.env" in ids
        assert "env-exposure/metrics" in ids

    def test_absent_paths_are_not_reported(self, ctx):
        ids = rule_ids(ExposureScanner().scan(ctx))
        assert "env-exposure/actuator/env" not in ids
        assert "env-exposure/phpinfo.php" not in ids

    def test_exposure_evidence_is_redacted(self, ctx):
        findings = [f for f in ExposureScanner().scan(ctx) if f.rule_id == "env-exposure/.env"]
        assert findings
        assert "DATABASE_PASSWORD=fixture" not in findings[0].evidence

    def test_missing_security_txt_is_informational(self, ctx):
        findings = [
            f for f in ExposureScanner().scan(ctx) if f.rule_id == "env-exposure/no-security-txt"
        ]
        assert findings and findings[0].severity is Severity.INFO

    def test_probes_stay_inside_the_authorised_scope(self, ctx):
        list(ExposureScanner().scan(ctx))
        assert all(ctx.scope.allows(url) for _, url, _ in ctx.http.request_log)


class TestTls:
    def test_plaintext_target_is_reported(self, ctx):
        findings = list(TlsScanner().scan(ctx))
        assert findings[0].rule_id == "env-tls/no-tls"
        assert findings[0].severity is Severity.HIGH


class TestHttpClientSafety:
    def test_state_changing_methods_are_rejected(self, ctx):
        with pytest.raises(ValueError, match="read-only"):
            ctx.http.request("POST", ctx.target_url)
        with pytest.raises(ValueError, match="read-only"):
            ctx.http.request("DELETE", ctx.target_url)

    def test_out_of_scope_requests_are_rejected(self, ctx):
        from markna.authorization import ScopeError

        with pytest.raises(ScopeError):
            ctx.http.get("http://example.com/")
