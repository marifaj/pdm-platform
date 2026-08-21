"""Reports must never carry the credential they are reporting."""

from markna.redact import clean_evidence, redact, truncate


class TestRedaction:
    def test_aws_access_key_id(self):
        assert "AKIAIOSFODNN7EXAMPLE" not in redact("key = AKIAIOSFODNN7EXAMPLE")

    def test_github_token(self):
        assert "ghp_" not in redact("token: ghp_abcdefghijklmnopqrstuvwxyz0123456789")

    def test_private_key_block(self):
        text = "-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBAK\n-----END RSA PRIVATE KEY-----"
        assert "MIIBOgIBAAJBAK" not in redact(text)

    def test_jwt(self):
        token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dBjftJeZ4CVPmB92K27uhbUJU1p1r"
        assert token not in redact(f"Authorization: Bearer {token}")

    def test_assigned_secret_keeps_the_key_name(self):
        result = redact('password = "hunter2-is-not-safe"')
        assert "hunter2-is-not-safe" not in result
        assert "password" in result

    def test_url_credentials_keep_the_scheme_and_user(self):
        result = redact("postgres://appuser:sup3r-s3cret@db.internal:5432/app")
        assert "sup3r-s3cret" not in result
        assert "postgres://appuser" in result

    def test_caller_supplied_values_are_redacted(self):
        result = redact("cookie is opaque-value-123", extra_values=["opaque-value-123"])
        assert "opaque-value-123" not in result

    def test_ordinary_text_is_left_alone(self):
        text = "The service binds 0.0.0.0 and runs as root."
        assert redact(text) == text

    def test_empty_input(self):
        assert redact("") == ""
        assert redact(None) == ""


class TestTruncation:
    def test_short_text_is_untouched(self):
        assert truncate("abc", limit=10) == "abc"

    def test_long_text_is_marked_as_truncated(self):
        result = truncate("x" * 100, limit=10)
        assert result.startswith("x" * 10)
        assert "truncated" in result

    def test_clean_evidence_redacts_then_truncates(self):
        result = clean_evidence("AKIAIOSFODNN7EXAMPLE " + "y" * 100, limit=30)
        assert "AKIAIOSFODNN7EXAMPLE" not in result
        assert "truncated" in result
