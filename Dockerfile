FROM ghcr.io/berriai/litellm:v1.83.14-stable

# Apply BerriAI/litellm PR #28114: don't force tool_choice on adaptive-
# thinking models in the response_format synthetic-tool fallback. Required
# so Codex CLI structured-output requests (Opus 4.7 via Bedrock) can run
# with thinking.type="adaptive" without hitting "Thinking may not be
# enabled when tool_choice forces tool use".
#
# Drop both the patches/ directory and this RUN once the upstream PR is
# merged AND we upgrade past it.
USER root
COPY patches/ /tmp/patches/
RUN apk add --no-cache patch && \
    cd /app && \
    for p in /tmp/patches/*.patch; do \
        patch -p1 < "$p" || exit 1; \
    done && \
    rm -rf /tmp/patches && \
    apk del patch
