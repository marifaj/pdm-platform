"""Subprocess helpers for driving external scanners.

MARKNA shells out to mature open-source tools rather than reimplementing them,
so this module is deliberately boring: run a command, bound its runtime and its
output, and never let a scanner failure take the gate down with it.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence

#: Environment variables a scanner subprocess may see. Everything else is
#: withheld — notably ANTHROPIC_API_KEY, MARKNA_* (which name the database and
#: the workspace) and any cloud credentials on the host.
SAFE_ENV_NAMES = frozenset({
    "PATH", "HOME", "USER", "LOGNAME", "SHELL", "PWD", "TMPDIR", "TEMP", "TMP",
    "LANG", "LANGUAGE", "TERM",
    # TLS trust, so tools can validate certificates behind a corporate CA.
    "SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE",
    "NODE_EXTRA_CA_CERTS",
    # Egress proxying, without which a networked scanner cannot reach its
    # advisory database at all.
    "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy",
    # Container runtime, for the ZAP baseline scanner.
    "DOCKER_HOST", "DOCKER_CONFIG", "DOCKER_CERT_PATH", "DOCKER_TLS_VERIFY",
    # Java tooling used by some scanners.
    "JAVA_HOME",
})

#: Prefixes kept as well: locale settings, and per-tool configuration that an
#: operator sets deliberately for a scanner rather than for MARKNA.
SAFE_ENV_PREFIXES = ("LC_", "SEMGREP_", "TRIVY_", "GRYPE_", "SYFT_", "BANDIT_", "CHECKOV_")


def _has_safe_prefix(name: str) -> bool:
    return name.startswith(SAFE_ENV_PREFIXES)


#: Hard cap on captured output per stream. Some scanners emit tens of MB of JSON
#: on large repositories; we stream those to a file instead (see ``output_file``).
MAX_CAPTURED_CHARS = 40_000_000


@dataclass
class CommandResult:
    command: List[str]
    returncode: int
    stdout: str = ""
    stderr: str = ""
    duration: float = 0.0
    timed_out: bool = False
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and not self.timed_out and self.error is None

    @property
    def display_command(self) -> str:
        return " ".join(_quote(part) for part in self.command)

    def failure_message(self) -> str:
        """Human-readable failure, including the command, for the report."""
        if self.timed_out:
            return f"`{self.display_command}` timed out after {self.duration:.0f}s"
        if self.error:
            return f"`{self.display_command}`: {self.error}"
        tail = (self.stderr or self.stdout or "").strip().splitlines()
        detail = " / ".join(line.strip() for line in tail[-5:]) if tail else "no output"
        return f"`{self.display_command}` exited {self.returncode}: {detail[:1500]}"


def _quote(part: str) -> str:
    return f'"{part}"' if " " in part else part


@dataclass
class ToolPath:
    """PATH resolution for scanner binaries.

    ``extra_paths`` lets an operator point MARKNA at a virtualenv or a directory
    of downloaded binaries without polluting the ambient PATH.
    """

    extra_paths: List[str] = field(default_factory=list)

    @classmethod
    def from_env(cls, extra: Optional[Sequence[str]] = None) -> "ToolPath":
        paths: List[str] = list(extra or [])
        env_value = os.getenv("MARKNA_TOOL_PATH", "")
        paths.extend(p for p in env_value.split(os.pathsep) if p)
        return cls(extra_paths=paths)

    def search_path(self) -> str:
        base = os.environ.get("PATH", "")
        if not self.extra_paths:
            return base
        return os.pathsep.join([*self.extra_paths, base])

    def which(self, executable: str) -> Optional[str]:
        return shutil.which(executable, path=self.search_path())

    def environ(self, extra_env: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Build the environment a scanner subprocess receives.

        Allow-listed, not inherited. Scanners are large third-party programs
        that parse hostile input from the repository under assessment; handing
        them the worker's whole environment would hand them its API keys,
        database path and cloud credentials as well. Anything a tool genuinely
        needs is either on this list or passed explicitly by its adapter.
        """
        env = {
            name: value
            for name, value in os.environ.items()
            if name in SAFE_ENV_NAMES or _has_safe_prefix(name)
        }
        env["PATH"] = self.search_path()
        env.setdefault("HOME", os.environ.get("HOME", "/tmp"))
        if extra_env:
            env.update(extra_env)
        return env


def run_command(
    command: Sequence[str],
    *,
    cwd: Optional[Path] = None,
    timeout: int = 900,
    tool_path: Optional[ToolPath] = None,
    extra_env: Optional[Dict[str, str]] = None,
    output_file: Optional[Path] = None,
    check: bool = False,
) -> CommandResult:
    """Run ``command``, capturing output and never raising on tool failure.

    When ``output_file`` is given, stdout is streamed to that path instead of
    being buffered in memory, and ``stdout`` on the result is left empty.
    """
    command = [str(part) for part in command]
    tool_path = tool_path or ToolPath()
    env = tool_path.environ(extra_env)
    started = time.monotonic()

    stdout_target = None
    try:
        if output_file is not None:
            output_file.parent.mkdir(parents=True, exist_ok=True)
            stdout_target = output_file.open("wb")

        completed = subprocess.run(  # noqa: S603 - commands are built internally
            command,
            cwd=str(cwd) if cwd else None,
            env=env,
            stdout=stdout_target if stdout_target is not None else subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        duration = time.monotonic() - started
        stdout = ""
        if stdout_target is None and completed.stdout:
            stdout = completed.stdout.decode("utf-8", errors="replace")[:MAX_CAPTURED_CHARS]
        stderr = ""
        if completed.stderr:
            stderr = completed.stderr.decode("utf-8", errors="replace")[:MAX_CAPTURED_CHARS]
        result = CommandResult(
            command=command,
            returncode=completed.returncode,
            stdout=stdout,
            stderr=stderr,
            duration=duration,
        )
    except subprocess.TimeoutExpired:
        result = CommandResult(
            command=command,
            returncode=-1,
            duration=time.monotonic() - started,
            timed_out=True,
        )
    except FileNotFoundError:
        result = CommandResult(
            command=command,
            returncode=-1,
            duration=time.monotonic() - started,
            error=f"executable not found: {command[0]}",
        )
    except OSError as exc:
        result = CommandResult(
            command=command,
            returncode=-1,
            duration=time.monotonic() - started,
            error=f"could not execute {command[0]}: {exc}",
        )
    finally:
        if stdout_target is not None:
            stdout_target.close()

    if check and not result.ok:
        raise ScannerExecutionError(result.failure_message(), result)
    return result


class ScannerExecutionError(RuntimeError):
    """Raised when a scanner invocation fails and the caller wants to abort."""

    def __init__(self, message: str, result: Optional[CommandResult] = None) -> None:
        super().__init__(message)
        self.result = result


def probe_version(
    executable: str,
    *,
    args: Sequence[str] = ("--version",),
    tool_path: Optional[ToolPath] = None,
    timeout: int = 60,
    extra_env: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Return the first line of ``executable --version``, or ``None``."""
    result = run_command(
        [executable, *args], timeout=timeout, tool_path=tool_path, extra_env=extra_env
    )
    text = (result.stdout or result.stderr).strip()
    if not text:
        return None
    return text.splitlines()[0].strip()[:120]
