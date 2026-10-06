# syntax=docker/dockerfile:1
FROM golang:1.27.1-alpine3.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY migrations/ ./migrations/
COPY queries/ ./queries/

FROM build AS test
RUN go test -p 1 ./...

FROM build AS binary
RUN CGO_ENABLED=0 go build -p 1 -trimpath -ldflags="-s -w" -o /out/janus .

FROM alpine:3.24 AS runtime
RUN apk add --no-cache ca-certificates && addgroup -g 10001 janus && adduser -D -H -u 10001 -G janus janus
WORKDIR /app
COPY --from=binary /out/janus /app/janus
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/app/janus"]
