"""Authorisation and scope: the environment layer must refuse to overreach."""

from datetime import date, timedelta

import pytest

from markna.authorization import Authorization, AuthorizationError, Scope, ScopeError


def authorization(**overrides) -> Authorization:
    data = {"authorized_by": "A. Reviewer", "allow_private_targets": True}
    data.update(overrides)
    return Authorization.from_dict(data)


class TestAuthorization:
    def test_authorized_by_is_required(self):
        with pytest.raises(AuthorizationError, match="authorized_by is required"):
            Authorization.from_dict({"reference": "CHG-1"})

    def test_expired_authorization_is_refused(self):
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        with pytest.raises(AuthorizationError, match="expired"):
            authorization(expires=yesterday).validate()

    def test_future_expiry_is_accepted(self):
        tomorrow = (date.today() + timedelta(days=1)).isoformat()
        authorization(expires=tomorrow).validate()  # must not raise

    def test_malformed_expiry_is_rejected(self):
        with pytest.raises(AuthorizationError, match="YYYY-MM-DD"):
            authorization(expires="next tuesday").validate()

    def test_round_trips_to_a_dict_for_the_report(self):
        record = authorization(reference="CHG-42").to_dict()
        assert record["authorized_by"] == "A. Reviewer"
        assert record["reference"] == "CHG-42"


class TestScope:
    def test_target_host_is_always_in_scope(self):
        scope = Scope.for_target("https://uat.example.com/app", authorization())
        assert scope.contains_host("uat.example.com")

    def test_other_hosts_are_refused(self):
        scope = Scope.for_target("https://uat.example.com", authorization())
        with pytest.raises(ScopeError, match="not in the authorised scope"):
            scope.check("https://other.example.com/")

    def test_extra_scope_hosts_are_honoured(self):
        scope = Scope.for_target(
            "https://uat.example.com", authorization(scope_hosts=["api.example.com"])
        )
        assert scope.allows("https://api.example.com/health")

    def test_wildcard_scope_hosts(self):
        scope = Scope.for_target(
            "https://uat.example.com", authorization(scope_hosts=["*.example.net"])
        )
        assert scope.allows("https://a.example.net/")
        assert not scope.allows("https://example.org/")

    def test_private_targets_are_refused_unless_permitted(self):
        with pytest.raises(ScopeError, match="private, loopback"):
            Scope.for_target("http://127.0.0.1:8080", authorization(allow_private_targets=False))

    def test_private_targets_are_allowed_when_declared(self):
        scope = Scope.for_target("http://127.0.0.1:8080", authorization())
        assert scope.allows("http://127.0.0.1:8080/health")

    def test_non_http_schemes_are_refused(self):
        with pytest.raises(ScopeError, match="http"):
            Scope.for_target("ssh://uat.example.com", authorization())

    def test_file_urls_are_refused_by_check(self):
        scope = Scope.for_target("http://127.0.0.1:9", authorization())
        with pytest.raises(ScopeError):
            scope.check("file:///etc/passwd")
