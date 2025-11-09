# syntax=docker/dockerfile:1.7

############################
# Builder
############################
FROM --platform=$BUILDPLATFORM golang:1.23.4-alpine@sha256:c23339199a08b0e12032856908589a6d41a0dab141b8b3b21f156fc571a3f1d3 AS builder

ARG TARGETOS TARGETARCH
ARG SKAFFOLD_GO_GCFLAGS=""
# Если main не в корне, переопредели при сборке:
#   --build-arg BUILD_PKG=./cmd/orders-event-service
ARG BUILD_PKG=.

WORKDIR /src

# инструменты и сертификаты для git/https
RUN apk add --no-cache git ca-certificates && update-ca-certificates

# кеш модулей/сборки
ENV GOMODCACHE=/go/pkg/mod \
    GOCACHE=/root/.cache/go-build

# сначала зависимости (лучший кеш)
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# затем исходники
COPY . .

# добираем недостающие записи в go.sum (как раз твой случай с go-spew)
RUN --mount=type=cache,target=/go/pkg/mod go mod tidy

# сборка статического бинаря
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    set -eux; \
    export GOOS="${TARGETOS:-linux}" GOARCH="${TARGETARCH:-amd64}" CGO_ENABLED=0; \
    if [ -n "${SKAFFOLD_GO_GCFLAGS}" ]; then \
      go build -trimpath -ldflags="-s -w" -gcflags="${SKAFFOLD_GO_GCFLAGS}" -o /out/app "${BUILD_PKG}"; \
    else \
      go build -trimpath -ldflags="-s -w" -o /out/app "${BUILD_PKG}"; \
    fi

############################
# Final (minimal, non-root)
############################
FROM scratch

WORKDIR /src
# корневые сертификаты для HTTPS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
# бинарь
COPY --from=builder /out/app /src/orders-event-service

ENV PORT=8080 \
    GOTRACEBACK=single

EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/src/orders-event-service"]
