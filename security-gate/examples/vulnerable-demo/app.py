"""Deliberately insecure sample application.

This file is a test fixture for the MARKNA Security Gate. It exists so the
scanners have something to find. Do not copy any of it into real code.
All credential-shaped values below are the vendors' documented example values.
"""

import hashlib
import os
import pickle
import random
import sqlite3
import subprocess

import requests
import yaml
from flask import Flask, request

app = Flask(__name__)

# Hard-coded credentials (fixture values, not real).
DB_PASSWORD = "s3cr3t-fixture-password"
AWS_ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE"
API_ENDPOINT = "http://telemetry.internal.example.com/ingest"


def run_report(report_name):
    # Command injection: the shell interprets report_name.
    return subprocess.check_output("generate-report " + report_name, shell=True)


def find_user(connection: sqlite3.Connection, username):
    # SQL injection: the value is interpolated into the statement.
    return connection.execute(
        "SELECT * FROM users WHERE username = '%s'" % username
    ).fetchall()


def hash_password(password):
    # MD5 is not a password hash.
    return hashlib.md5(password.encode()).hexdigest()


def new_session_token():
    # Predictable token source.
    return str(random.randint(0, 999999))


def load_profile(blob):
    # Deserialising untrusted input executes code.
    return pickle.loads(blob)


def load_settings(text):
    # yaml.load without SafeLoader instantiates arbitrary objects.
    return yaml.load(text)


def fetch_upstream(path):
    # Certificate verification disabled.
    return requests.get(API_ENDPOINT + path, verify=False, timeout=10)


@app.route("/render")
def render():
    template = request.args.get("template", "")
    # Arbitrary code execution from a request parameter.
    return str(eval(template))


if __name__ == "__main__":
    # Debug server bound to every interface.
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=True)
