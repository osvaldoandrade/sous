# syntax=docker/dockerfile:1.7
FROM golang:1.25-alpine AS build
ARG SOUS_COMPONENT
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN case "$SOUS_COMPONENT" in \
      cs-control|cs-http-gateway|cs-invoker-pool|cs-scheduler) ;; \
      *) echo "unsupported SOUS_COMPONENT" >&2; exit 1 ;; \
    esac && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/sous "./cmd/$SOUS_COMPONENT"

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/sous /app/sous
USER nonroot:nonroot
ENTRYPOINT ["/app/sous"]
