ARG UV_VERSION=0.12.13

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM golang:1.26.2-bookworm AS go-builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal/runner/*.go ./
RUN CGO_ENABLED=0 GOOS="${TARGETOS}" GOARCH="${TARGETARCH}" go build -trimpath -ldflags="-s -w" -o /out/agent-runner .

FROM node:24-bookworm-slim

ARG CODEX_ACP_VERSION=1.11.0
ARG CLAUDE_ACP_VERSION=0.79.0
ARG CLAUDE_CODE_VERSION=2.1.274
ARG GH_VERSION=2.100.0
ARG GOGCLI_VERSION=0.39.1
ARG GROK_VERSION=1.0.13
ARG OPENCODE_VERSION=1.18.27
ARG PI_ACP_VERSION=0.6.2
ARG PYTHON_VERSION=3.14
ARG TARGETARCH
ARG UV_VERSION

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    curl \
    fd-find \
    git \
    jq \
    less \
    ripgrep \
    sqlite3 \
    tzdata \
    unzip \
    vim \
    wget \
    && ln -s /usr/bin/fdfind /usr/local/bin/fd \
    && ln -s /usr/bin/sqlite3 /usr/local/bin/sqlite \
    && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /uvx /usr/local/bin/

RUN set -eux; \
    uv --version | grep --fixed-strings "uv ${UV_VERSION}"; \
    uvx --version | grep --fixed-strings "uvx ${UV_VERSION}"; \
    UV_PYTHON_INSTALL_DIR=/opt/uv/python \
      UV_PYTHON_BIN_DIR=/usr/local/bin \
      uv python install --default --no-cache "${PYTHON_VERSION}"; \
    python --version | grep --extended-regexp "^Python ${PYTHON_VERSION}\\.[0-9]+$"; \
    test -L /usr/local/bin/python; \
    test -L /usr/local/bin/python3; \
    test "$(readlink --canonicalize /usr/local/bin/python)" = "$(readlink --canonicalize /usr/local/bin/python3)"

RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64 | arm64) ;; \
      *) echo "Unsupported GitHub CLI architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    gh_archive="gh_${GH_VERSION}_linux_${TARGETARCH}.tar.gz"; \
    curl --fail --show-error --location --retry 5 --retry-all-errors \
      "https://github.com/cli/cli/releases/download/v${GH_VERSION}/${gh_archive}" \
      --output "/tmp/${gh_archive}"; \
    tar --extract --gzip --file "/tmp/${gh_archive}" --directory /usr/local/bin \
      --strip-components=2 "gh_${GH_VERSION}_linux_${TARGETARCH}/bin/gh"; \
    rm "/tmp/${gh_archive}"; \
    gh version | grep --fixed-strings "gh version ${GH_VERSION}"

RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64 | arm64) ;; \
      *) echo "Unsupported gogcli architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    gog_archive="gogcli_${GOGCLI_VERSION}_linux_${TARGETARCH}.tar.gz"; \
    curl --fail --show-error --location --retry 5 --retry-all-errors \
      "https://github.com/openclaw/gogcli/releases/download/v${GOGCLI_VERSION}/${gog_archive}" \
      --output "/tmp/${gog_archive}"; \
    curl --fail --show-error --location --retry 5 --retry-all-errors \
      "https://github.com/openclaw/gogcli/releases/download/v${GOGCLI_VERSION}/checksums.txt" \
      --output /tmp/gogcli-checksums.txt; \
    cd /tmp; \
    grep "  ${gog_archive}$" gogcli-checksums.txt | sha256sum --check --strict -; \
    tar --extract --gzip --file "${gog_archive}" --directory /tmp ./gog; \
    install --mode=0755 gog /usr/local/bin/gog; \
    rm "${gog_archive}" gog gogcli-checksums.txt; \
    gog version | grep --fixed-strings "v${GOGCLI_VERSION}"

COPY scripts/patch-codex-acp-cache-write.mjs scripts/probe-claude-code-model-capabilities.mjs /usr/local/libexec/

RUN npm install --global "@agentclientprotocol/codex-acp@${CODEX_ACP_VERSION}" \
    && node /usr/local/libexec/patch-codex-acp-cache-write.mjs \
      "$(npm root --global)/@agentclientprotocol/codex-acp" \
    && codex-acp --version \
    && npm cache clean --force

RUN npm install --global "@agentclientprotocol/claude-agent-acp@${CLAUDE_ACP_VERSION}" \
    && test "$(claude-agent-acp --version)" = "${CLAUDE_ACP_VERSION}" \
    && claude_path="$(find "$(npm root --global)/@agentclientprotocol/claude-agent-acp" \
      -type f -path '*/claude-agent-sdk-linux-x64/claude' -print -quit)" \
    && test -n "${claude_path}" \
    && test -x "${claude_path}" \
    && ln --symbolic "${claude_path}" /usr/local/bin/claude \
    && test "$(claude --version)" = "${CLAUDE_CODE_VERSION} (Claude Code)" \
    && test "$(CLAUDE_CODE_EXECUTABLE=/usr/local/bin/claude claude-agent-acp --cli --version)" = "${CLAUDE_CODE_VERSION} (Claude Code)" \
    && node /usr/local/libexec/probe-claude-code-model-capabilities.mjs \
      /usr/local/bin/claude-agent-acp \
      /usr/local/bin/claude \
    && test -z "$(find /tmp -maxdepth 1 -type f -name 'claude-settings-*.json' -print -quit)" \
    && npm cache clean --force

RUN npm install --global "opencode-ai@${OPENCODE_VERSION}" \
    # && test -x /usr/local/lib/node_modules/opencode-ai/node_modules/opencode-linux-x64/bin/opencode \
    && npm cache clean --force

RUN npm install --global "@automatalabs/pi-acp@${PI_ACP_VERSION}" \
    && pi-acp --version \
    && npm cache clean --force

RUN curl -fsSL https://x.ai/cli/install.sh -o /tmp/install-grok.sh \
    && GROK_BIN_DIR=/usr/local/bin bash /tmp/install-grok.sh "${GROK_VERSION}" \
    && grok --version \
    && cp --dereference /usr/local/bin/grok /usr/local/bin/grok.bin \
    && mv --force /usr/local/bin/grok.bin /usr/local/bin/grok \
    && ln --symbolic --force /usr/local/bin/grok /usr/local/bin/agent \
    && rm -f /tmp/install-grok.sh \
    && rm -rf /root/.grok/downloads

RUN useradd --uid 10001 --create-home --shell /bin/bash runner \
    && mkdir -p /workspace /tmp/agent-runner /tmp/claude-home /tmp/codex-home /tmp/grok-home /tmp/opencode-home /tmp/pi-home /etc/agent-runner \
    && chown -R runner:runner /workspace /tmp/agent-runner /tmp/claude-home /tmp/codex-home /tmp/grok-home /tmp/opencode-home /tmp/pi-home /home/runner

COPY --from=go-builder /out/agent-runner /usr/local/bin/agent-runner
RUN ln --symbolic /usr/local/bin/agent-runner /usr/local/bin/flow-metric

EXPOSE 8080
USER runner
ENTRYPOINT ["/usr/local/bin/agent-runner"]
