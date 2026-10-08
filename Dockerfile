# syntax=docker/dockerfile:1
FROM golang:1.27.1-alpine3.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY migrations/ ./migrations/
COPY queries/ ./queries/
COPY cmd/loadtest/ ./cmd/loadtest/

FROM build AS test
RUN go test -p 1 ./...

FROM build AS binary
# Keep compiler caches out of image layers; these temporary mounts use build-time RAM.
RUN --mount=type=tmpfs,target=/root/.cache/go-build,size=1073741824 --mount=type=tmpfs,target=/tmp,size=536870912 CGO_ENABLED=0 go build -p 1 -trimpath -ldflags="-s -w" -o /out/janus .

FROM build AS load-binary
RUN --mount=type=tmpfs,target=/root/.cache/go-build,size=1073741824 --mount=type=tmpfs,target=/tmp,size=536870912 CGO_ENABLED=0 go build -p 1 -trimpath -o /out/loadtest ./cmd/loadtest

FROM alpine:3.24 AS loadtool
RUN apk add --no-cache ca-certificates && addgroup -g 10001 janus && adduser -D -H -u 10001 -G janus janus
COPY --from=load-binary /out/loadtest /app/loadtest
USER 10001:10001
ENTRYPOINT ["/app/loadtest"]

FROM alpine:3.24 AS runtime
RUN apk add --no-cache ca-certificates && addgroup -g 10001 janus && adduser -D -H -u 10001 -G janus janus
RUN mkdir -p /var/lib/janus/outbox && chown -R 10001:10001 /var/lib/janus
WORKDIR /app
COPY --from=binary /out/janus /app/janus
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/app/janus"]
