#
# Copyright 2017 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM docker.io/library/golang:1.25.11@sha256:00feed335fe561979f2cdcc30a5191231977c5631fe79c40f7d3ab63b4fa222f AS builder

WORKDIR /workspace

# Copy the module manifests and download dependencies first, so this layer is
# cached unless go.mod/go.sum change. (COPY rather than a context bind mount so
# the build works under both Docker/BuildKit and rootless Podman/buildah, which
# cannot read bind-mounted context files.) The module cache is baked into this
# image layer rather than a --mount=type=cache: under Podman/buildah a module
# cache populated in this RUN is not reliably visible to the build RUN below.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV GOCACHE=/root/.cache/go-build

ARG TARGETARCH

# Build metadata. When unset, the Makefile derives these from the git tree
# inside the build context, preserving the behaviour of a plain `docker build`.
ARG VERSION=
ARG GIT_COMMIT=
ARG GIT_TREE_STATE=
ARG SOURCE_DATE_EPOCH=

RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} GO111MODULE=on \
    make build-operator \
      ${VERSION:+VERSION=$VERSION} \
      ${GIT_COMMIT:+GIT_COMMIT=$GIT_COMMIT} \
      ${GIT_TREE_STATE:+GIT_TREE_STATE=$GIT_TREE_STATE} \
      ${SOURCE_DATE_EPOCH:+SOURCE_DATE_EPOCH=$SOURCE_DATE_EPOCH}

# Export-only stage. `docker build` targets the final stage by default, so this
# is never built unless requested via --target=artifacts. CI uses it to extract
# the exact binary that ships in the image.
FROM scratch AS artifacts
COPY --from=builder /workspace/bin/spark-operator /spark-operator

# Runtime image. The operator is a fully static (CGO_ENABLED=0) Go binary and no
# longer shells out to spark-submit on the native/REST submission paths, so it
# needs neither Spark nor a JVM at runtime — Spark is supplied entirely by the
# user's driver/executor image. distroless/static gives us just libc-free glibc
# bits, CA certificates, and tzdata, running as the built-in nonroot user (65532).
# No shell, no package manager: nothing for the operator's CVE surface to inherit.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /workspace/bin/spark-operator /usr/bin/spark-operator

ENTRYPOINT ["/usr/bin/spark-operator"]
