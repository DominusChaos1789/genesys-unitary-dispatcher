import pytest
import requests

from src.client_request import GenesysClient


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(str(self.status_code))


@pytest.fixture
def sent(monkeypatch):
    """Captures what GenesysClient hands to requests; `sent["response"]` sets the reply."""
    captured = {"response": FakeResponse(200, {"ok": True})}

    def fake_request(**kwargs):
        captured["kwargs"] = kwargs
        return captured["response"]

    monkeypatch.setattr(requests, "request", fake_request)
    return captured


def test_call_joins_the_base_url_and_forwards_params_and_body(sent):
    result = GenesysClient("https://login.usw2.pure.cloud/oauth/").call(
        {
            "url": "token",
            "method": "POST",
            "headers": {"Content-Type": "application/x-www-form-urlencoded"},
            "params_template": {"grant_type": "client_credentials"},
            "body_template": {"a": "b"},
        }
    )

    assert result == {"ok": True}
    assert sent["kwargs"] == {
        "method": "POST",
        "url": "https://login.usw2.pure.cloud/oauth/token",
        "headers": {"Content-Type": "application/x-www-form-urlencoded"},
        "params": {"grant_type": "client_credentials"},
        "data": {"a": "b"},
    }


def test_empty_templates_are_sent_as_none(sent):
    GenesysClient("https://api/").call({"url": "x", "method": "GET", "headers": {}, "params_template": {}})

    assert sent["kwargs"]["params"] is None
    assert sent["kwargs"]["data"] is None


def test_a_404_is_returned_without_raising(sent):
    sent["response"] = FakeResponse(404, {"message": "not found"})

    assert GenesysClient("https://api/").call({"url": "x", "method": "GET", "headers": {}}) == {
        "message": "not found"
    }


def test_other_http_errors_raise(sent):
    sent["response"] = FakeResponse(500)

    with pytest.raises(requests.exceptions.HTTPError, match="500"):
        GenesysClient("https://api/").call({"url": "x", "method": "GET", "headers": {}})
