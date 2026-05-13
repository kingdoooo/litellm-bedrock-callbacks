"""
Shared helper to distinguish OpenAI Responses API traffic from other
endpoints inside streaming-iterator callbacks (which do not receive a
call_type argument in LiteLLM v1.83.14).

Discriminator: Responses API bodies carry "input"; Anthropic Messages
and Chat Completions carry "messages". Leading underscore marks this
module as internal — it is not a registered callback dotted path.
"""


def is_responses_api(request_data: dict) -> bool:
    return (
        isinstance(request_data, dict)
        and "input" in request_data
        and "messages" not in request_data
    )
