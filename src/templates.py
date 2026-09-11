"""Placeholder rendering for endpoint and connection templates."""


def render_template(template, **kwargs):
    """Recursive str.format over strings, dicts and lists.

    Placeholders with no matching kwarg are left intact (the whole string is
    returned unchanged), which is what lets a payload fill in the token and
    region while leaving {conversationId} / {mu_id} / {jobId} for the Lambdas
    that execute the calls.
    """
    if isinstance(template, str):
        try:
            return template.format(**{key: str(value) for key, value in kwargs.items()})
        except KeyError:
            return template

    if isinstance(template, dict):
        return {key: render_template(value, **kwargs) for key, value in template.items()}

    if isinstance(template, list):
        return [render_template(item, **kwargs) for item in template]

    return template
