## Building Image
FROM gcr.io/gcp-runtimes/go1-builder:1.15 as builder

WORKDIR /go/src/app
ENV CGO_ENABLED=0
COPY go.mod go.sum ./
RUN /usr/local/go/bin/go mod download

COPY . ./
RUN /usr/local/go/bin/go build -o bolt-proxy .

## App Image
FROM gcr.io/distroless/base:latest
COPY --from=builder /go/src/app/bolt-proxy /usr/local/bin/bolt-proxy
EXPOSE 7687/tcp
ENTRYPOINT ["/usr/local/bin/bolt-proxy"]
