"""
LiteLLM CustomLogger that strips Bedrock-incompatible fields from
OpenAI Responses API traffic (the Codex CLI path) without affecting
the Anthropic Messages API path (Claude Code).

Three fixes, two hooks, both guarded to Responses API only.

1) async_pre_call_hook — runs on the raw request body:

   - client_metadata: Codex hardcodes this top-level field in
     codex-rs/core/src/client.rs:761. Real OpenAI silently ignores
     it; Bedrock Converse rejects with "Extra inputs are not
     permitted". Upstream issue: openai/codex#17910 (WONTFIX).

   - include: ["reasoning.encrypted_content"]: Codex also ships this;
     Bedrock likewise rejects.

2) async_pre_call_deployment_hook — runs after deployment selection,
   right before the wire serialization. Strips output_config that
   LiteLLM's own Anthropic transformation layer synthesizes from
   `reasoning.effort` (see llms/anthropic/chat/transformation.py:1108)
   for Claude 4.6/4.7 models. When the request flows through the
   Responses API path to a Bedrock deployment, the Bedrock adapter's
   pop of output_config does not catch this synthesized value, and
   Bedrock rejects it as "output_config.format: Extra inputs are not
   permitted". Anthropic-native /v1/messages traffic (Claude Code)
   keeps output_config so the xhigh effort config in config.yaml
   continues to work there.

LiteLLM's drop_params / additional_drop_params do not run on the
/v1/responses route in v1.83.14 — this is why proxy-side sanitation
is required.
"""
from litellm.integrations.custom_logger import CustomLogger


def _is_responses_call(call_type) -> bool:
    # call_type may be a str (async_pre_call_hook) or a CallTypes enum
    # (async_pre_call_deployment_hook). Accept either shape.
    name = getattr(call_type, "value", call_type)
    return name == "aresponses"


class CodexSanitizer(CustomLogger):
    DROP_INCLUDE_VALUES = frozenset({"reasoning.encrypted_content"})

    async def async_pre_call_hook(
        self, user_api_key_dict, cache, data, call_type
    ):
        if not _is_responses_call(call_type):
            return data

        data.pop("client_metadata", None)

        inc = data.get("include")
        if isinstance(inc, list):
            filtered = [v for v in inc if v not in self.DROP_INCLUDE_VALUES]
            if filtered:
                data["include"] = filtered
            else:
                data.pop("include", None)

        return data

    async def async_pre_call_deployment_hook(self, kwargs, call_type):
        if not _is_responses_call(call_type):
            return None

        kwargs.pop("output_config", None)
        optional_params = kwargs.get("optional_params")
        if isinstance(optional_params, dict):
            optional_params.pop("output_config", None)

        return kwargs


instance = CodexSanitizer()
