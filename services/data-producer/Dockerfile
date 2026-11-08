# syntax=docker/dockerfile:1
# Build from the repository root: docker compose build data-producer
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY gen/go ./gen/go
COPY services/data-producer ./services/data-producer
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/data-producer ./services/data-producer

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/data-producer /data-producer
ENTRYPOINT ["/data-producer"]
