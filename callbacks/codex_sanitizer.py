"""
LiteLLM CustomLogger that strips Bedrock-incompatible fields from
OpenAI Responses API traffic (the Codex CLI path) without affecting
the Anthropic Messages API path (Claude Code).

Four concerns, two hooks, both guarded to Responses API only.

1) async_pre_call_hook — runs on the raw request body:

   - client_metadata: Codex hardcodes this top-level field in
     codex-rs/core/src/client.rs:761. Real OpenAI silently ignores
     it; Bedrock Converse rejects with "Extra inputs are not
     permitted". Upstream issue: openai/codex#17910 (WONTFIX).

   - include: ["reasoning.encrypted_content"]: Codex also ships this;
     Bedrock likewise rejects.

   - text.format.schema and strict-tool inputSchemas: Bedrock's
     native structured outputs accept only a subset of JSON Schema
     Draft 2020-12. Pure validation keywords (minimum, maxLength,
     pattern, format, multipleOf, …) raise 400 "property X is not
     supported for type Y". The codex-plugin-cc adversarial-review
     skill ships a schema with `{"type":"integer","minimum":1}` for
     line numbers and similar constraints on strings. We recursively
     strip the unsupported keywords; structural keywords (type,
     properties, required, enum, items, $ref, $defs, anyOf, …) are
     preserved so Bedrock still enforces shape at the protocol level.

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


# Bedrock's structured-outputs JSON Schema subset rejects all "pure validation"
# keywords. Keep structural keywords (type, properties, required, enum, const,
# items, $ref, $defs, anyOf, allOf, oneOf, additionalProperties, description).
_BEDROCK_UNSUPPORTED_SCHEMA_KEYS = frozenset({
    "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf",
    "minLength", "maxLength", "pattern", "format",
    "minItems", "maxItems", "uniqueItems", "minContains", "maxContains",
    "minProperties", "maxProperties", "unevaluatedProperties",
    "default", "examples", "title", "readOnly", "writeOnly", "$comment",
})

# Inside these maps, keys are arbitrary user-defined names (property names,
# pattern regexes, $defs ids), not JSON Schema keywords. Recurse into values
# but never strip the keys themselves — otherwise a field literally named
# "title" or "default" would be deleted from the schema.
_SCHEMA_NAME_BAGS = frozenset({"properties", "patternProperties", "$defs", "definitions"})


def _strip_unsupported_schema_keys(node) -> None:
    if isinstance(node, dict):
        for key in list(node.keys()):
            value = node[key]
            if key in _SCHEMA_NAME_BAGS and isinstance(value, dict):
                for subv in value.values():
                    _strip_unsupported_schema_keys(subv)
            elif key in _BEDROCK_UNSUPPORTED_SCHEMA_KEYS:
                del node[key]
            else:
                _strip_unsupported_schema_keys(value)
    elif isinstance(node, list):
        for item in node:
            _strip_unsupported_schema_keys(item)


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

        # Responses API structured output: data["text"]["format"]["schema"].
        text = data.get("text")
        if isinstance(text, dict):
            fmt = text.get("format")
            if isinstance(fmt, dict):
                schema = fmt.get("schema")
                if isinstance(schema, dict):
                    _strip_unsupported_schema_keys(schema)

        # Strict tools: parameters / input_schema / function.parameters.
        tools = data.get("tools")
        if isinstance(tools, list):
            for tool in tools:
                if not isinstance(tool, dict):
                    continue
                for key in ("parameters", "input_schema"):
                    sub = tool.get(key)
                    if isinstance(sub, dict):
                        _strip_unsupported_schema_keys(sub)
                fn = tool.get("function")
                if isinstance(fn, dict):
                    params = fn.get("parameters")
                    if isinstance(params, dict):
                        _strip_unsupported_schema_keys(params)

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
