from src.templates import render_template


def test_strings_are_formatted_with_the_given_values():
    assert (
        render_template("https://api.{region_id}.pure.cloud", region_id="usw2")
        == "https://api.usw2.pure.cloud"
    )


def test_values_are_stringified():
    assert render_template("{n}", n=5) == "5"


def test_a_string_with_an_unknown_placeholder_is_left_entirely_unchanged():
    # All-or-nothing per string: mu_id is NOT filled in, because user_id is missing.
    # Harmless for the payload (headers and base_url only carry one placeholder
    # each), but a template mixing org-level and per-id placeholders in one
    # string would come out fully unrendered.
    assert render_template("/users/{user_id}/{mu_id}", mu_id="mu-1") == "/users/{user_id}/{mu_id}"


def test_dicts_and_lists_are_rendered_recursively():
    template = {"items": [{"id": "{mu_id}", "flag": True}], "timeZone": "America/Bogota"}

    assert render_template(template, mu_id="mu-1") == {
        "items": [{"id": "mu-1", "flag": True}],
        "timeZone": "America/Bogota",
    }


def test_non_string_scalars_pass_through():
    assert render_template(None) is None
    assert render_template(3.5, x="y") == 3.5
