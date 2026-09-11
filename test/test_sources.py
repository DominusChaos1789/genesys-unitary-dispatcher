import json

import pytest

from src.config import load_settings
from src.sources import EventError, resolve_ids_by_organization, resolve_tag
from test.conftest import LANDING_BUCKET


def test_tag_is_read_from_the_top_level():
    assert resolve_tag({"tag": "surveys"}) == "surveys"


def test_tag_is_read_from_the_eventbridge_detail():
    assert resolve_tag({"detail": {"tag": "funcionarios_adherencia"}}) == "funcionarios_adherencia"


@pytest.mark.parametrize("event", [{}, {"tag": ""}, {"detail": {}}, {"detail": None}])
def test_missing_tag_is_an_error(event):
    with pytest.raises(EventError, match="no 'tag'"):
        resolve_tag(event)


def test_inline_organizations_are_merged_deduplicated_and_sorted():
    event = {
        "organizations": [
            {"organization_id": "org-3", "ids": ["b", "a"]},
            {"organization_id": "org-3", "ids": ["a", "c"]},
            {"organization_id": "org-1", "ids": []},
        ]
    }

    # org-1 has no ids, so it has nothing to dispatch and is dropped.
    assert resolve_ids_by_organization(None, load_settings(), event) == {"org-3": ["a", "b", "c"]}


def test_an_entry_without_an_organization_id_is_an_error():
    with pytest.raises(EventError, match="without 'organization_id'"):
        resolve_ids_by_organization(None, load_settings(), {"organizations": [{"ids": ["a"]}]})


def test_an_entry_without_an_ids_list_is_an_error():
    # A misspelled key ("id") must not silently drop the organization.
    event = {"organizations": [{"organization_id": "org-3", "id": ["a"]}]}

    with pytest.raises(EventError, match="'org-3' has no 'ids' list"):
        resolve_ids_by_organization(None, load_settings(), event)


def test_ids_location_with_a_logical_bucket_name(aws):
    aws["s3"].put_object(
        Bucket=LANDING_BUCKET,
        Key="ids.json",
        Body=json.dumps({"organizations": [{"organization_id": "org-1", "ids": ["x"]}]}),
    )
    event = {"ids_location": {"bucket": "landing", "key": "ids.json"}}

    assert resolve_ids_by_organization(aws["s3"], load_settings(), event) == {"org-1": ["x"]}


def test_ids_location_with_a_full_bucket_name_and_a_bare_list(aws):
    aws["s3"].put_object(
        Bucket=LANDING_BUCKET,
        Key="ids.json",
        Body=json.dumps([{"organization_id": "org-1", "ids": ["x"]}]),
    )
    event = {"ids_location": {"bucket": LANDING_BUCKET, "key": "ids.json"}}

    assert resolve_ids_by_organization(aws["s3"], load_settings(), event) == {"org-1": ["x"]}


def test_eventbridge_s3_detail_points_at_the_ids_file(aws):
    aws["s3"].put_object(
        Bucket=LANDING_BUCKET,
        Key="drop/ids.json",
        Body=json.dumps([{"organization_id": "org-3", "ids": ["y"]}]),
    )
    event = {"detail": {"bucket": {"name": LANDING_BUCKET}, "object": {"key": "drop/ids.json"}}}

    assert resolve_ids_by_organization(aws["s3"], load_settings(), event) == {"org-3": ["y"]}


@pytest.mark.parametrize(
    "event", [{}, {"detail": {"bucket": {"name": "b"}}}, {"detail": {"object": {"key": "k"}}}]
)
def test_an_event_without_any_id_source_is_an_error(event):
    with pytest.raises(EventError, match="carries no ids"):
        resolve_ids_by_organization(None, load_settings(), event)
