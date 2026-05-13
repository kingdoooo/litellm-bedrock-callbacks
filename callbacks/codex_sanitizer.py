"""
LiteLLM CustomLogger that strips Codex-CLI-specific fields from
OpenAI Responses API requests before they hit Bedrock Converse.

Codex Rust source (codex-rs/core/src/client.rs) hardcodes two fields
that the real OpenAI endpoint silently ignores but Bedrock Converse
rejects with "Extra inputs are not permitted":

  - client_metadata: {"x-codex-installation-id": "<uuid>"}
  - include: ["reasoning.encrypted_content", ...]

LiteLLM's drop_params / additional_drop_params do not run on the
/v1/responses route in v1.83.14, so sanitization must happen in a
pre_call_hook. The hook only activates for call_type='aresponses'
to guarantee Claude Code (/v1/messages → 'anthropic_messages') and
Chat Completions paths are untouched.
"""
from litellm.integrations.custom_logger import CustomLogger


class CodexSanitizer(CustomLogger):
    DROP_INCLUDE_VALUES = frozenset({"reasoning.encrypted_content"})

    async def async_pre_call_hook(
        self, user_api_key_dict, cache, data, call_type
    ):
        if call_type != "aresponses":
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


instance = CodexSanitizer()
