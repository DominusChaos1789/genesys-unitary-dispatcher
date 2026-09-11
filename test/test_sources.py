import json

import pytest

from src.config import load_settings
from src.sources import ALL_TAGS, EventError, resolve_ids_by_organization, resolve_tag_selection
from test.conftest import LANDING_BUCKET


def test_a_single_tag_top_level():
    assert resolve_tag_selection({"tag": "surveys"}) == ["surveys"]


def test_a_single_tag_from_the_eventbridge_detail():
    assert resolve_tag_selection({"detail": {"tag": "funcionarios_adherencia"}}) == [
        "funcionarios_adherencia"
    ]


def test_a_list_of_tags_keeps_its_order_and_drops_repeats():
    assert resolve_tag_selection({"tags": ["surveys", "recordings", "surveys"]}) == ["surveys", "recordings"]


@pytest.mark.parametrize("event", [{"tags": "all"}, {"detail": {"tags": "all"}}])
def test_all_tags_is_left_for_expansion_against_the_catalog(event):
    assert resolve_tag_selection(event) == ALL_TAGS


@pytest.mark.parametrize("event", [{}, {"tag": ""}, {"tag": 3}, {"detail": {}}, {"detail": None}])
def test_missing_tag_is_an_error(event):
    with pytest.raises(EventError, match="no 'tag' or 'tags'"):
        resolve_tag_selection(event)


@pytest.mark.parametrize("tags", [[], "surveys", ["surveys", ""], [1], None])
def test_tags_must_be_all_or_a_list_of_names(tags):
    with pytest.raises(EventError, match="'tags' must be"):
        resolve_tag_selection({"tags": tags})


def test_tag_and_tags_together_is_an_error():
    with pytest.raises(EventError, match="both 'tag' and 'tags'"):
        resolve_tag_selection({"tag": "surveys", "tags": ["recordings"]})


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
