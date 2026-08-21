#!/usr/bin/env python3
"""A deliberately insecure demo server for exercising the environment layer.

Run it, then point the gate at it:

    python3 examples/insecure-uat-server.py --port 8099 &
    markna assess --url http://127.0.0.1:8099 --layers environment \\
        --authorized-by "you" --allow-private-targets --out /tmp/markna-demo

It serves plaintext HTTP with no security headers, a session cookie without
Secure/HttpOnly/SameSite, reflected CORS with credentials, an exposed .env and
.git/HEAD, a directory listing and a verbose error page — one instance of each
class of finding the environment scanners look for.

Never expose this to a network you do not control.
"""

from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_ENV_FILE = """\
DATABASE_URL=postgres://demo:demo-fixture-password@localhost:5432/telemetry
SECRET_KEY=fixture-value-not-a-real-secret
DEBUG=True
"""

_DIRECTORY_LISTING = """\
<html><head><title>Index of /files</title></head><body>
<h1>Index of /files</h1>
<pre><a href="backup.zip">backup.zip</a>   2026-08-01  14M
<a href="notes.txt">notes.txt</a>        2026-08-02   2K</pre>
</body></html>
"""

_ERROR_PAGE = """\
<html><body><h1>Internal Server Error</h1><pre>
Traceback (most recent call last):
  File "/srv/app/views.py", line 88, in dashboard
    rows = db.execute(query)
sqlite3.OperationalError: no such table: telemetry
</pre></body></html>
"""

_HOME = """\
<html><head><title>Telemetry UAT</title></head>
<body><h1>Telemetry UAT dashboard</h1><p>Demo target for the MARKNA gate.</p></body></html>
"""


class Handler(BaseHTTPRequestHandler):
    server_version = "DemoServer/1.4.2"
    sys_version = ""

    def do_GET(self) -> None:  # noqa: N802 - stdlib API
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            self._respond(200, _HOME, "text/html", set_cookie=True)
        elif path == "/.env":
            self._respond(200, _ENV_FILE, "text/plain")
        elif path == "/.git/HEAD":
            self._respond(200, "ref: refs/heads/main\n", "text/plain")
        elif path == "/files" or path == "/files/":
            self._respond(200, _DIRECTORY_LISTING, "text/html")
        elif path == "/metrics":
            self._respond(
                200,
                "# HELP http_requests_total Total requests\n"
                "# TYPE http_requests_total counter\n"
                'http_requests_total{route="/"} 42\n',
                "text/plain",
            )
        else:
            # Every unknown path returns a verbose 500, which is also what the
            # exposure scanner uses as its soft-404 baseline.
            self._respond(500, _ERROR_PAGE, "text/html")

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib API
        self._respond(200, "", "text/html")

    def do_OPTIONS(self) -> None:  # noqa: N802 - stdlib API
        self.send_response(200)
        self.send_header("Allow", "GET, HEAD, POST, PUT, DELETE, TRACE, OPTIONS")
        self.send_header("Content-Length", "0")
        self._cors()
        self.end_headers()

    # ------------------------------------------------------------------ helpers

    def _respond(self, status: int, body: str, content_type: str, set_cookie: bool = False) -> None:
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("X-Powered-By", "DemoFramework/3.2.1")
        if set_cookie:
            # No Secure, no HttpOnly, no SameSite.
            self.send_header("Set-Cookie", "sessionid=demo-session-value; Path=/")
        self._cors()
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def _cors(self) -> None:
        origin = self.headers.get("Origin")
        if origin:
            # Reflects any origin, and allows credentials with it.
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Access-Control-Allow-Credentials", "true")

    def log_message(self, fmt: str, *args) -> None:
        print(f"[demo-server] {self.address_string()} {fmt % args}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8099)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"[demo-server] listening on http://{args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
