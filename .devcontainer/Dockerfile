# FROM mcr.microsoft.com/devcontainers/go:1-1.24-bookworm
FROM debian:bookworm

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends git ca-certificates curl

RUN git clone --depth 1 https://github.com/marzer/tomlplusplus.git \
    && cp -r tomlplusplus/include/toml++ /usr/local/include/ \
    && rm -rf tomlplusplus

RUN curl -fsSL https://pixi.sh/install.sh | bash


ENV PATH="/root/.pixi/bin:${PATH}"

RUN pixi global install go gopls
