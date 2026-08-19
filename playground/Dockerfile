# syntax=docker/dockerfile:1.7

ARG GO_IMAGE=golang:1.27rc2-bookworm@sha256:a2f9daa5dbd9f7a68eb3c32cf91e4f9fc50a11a07f8b9cd9ffa542d2298d9f82
ARG RUNTIME_IMAGE=gcr.io/distroless/static-debian12:nonroot@sha256:1b7b9f0f0e0a1d2155f531db587cc48ec26aaf97ab64364225f5bf18a054e66a

FROM ${GO_IMAGE} AS build
WORKDIR /src

COPY go.mod go.sum ./
COPY playground/go.mod playground/go.sum ./playground/
RUN go mod download && go mod download -C playground

COPY . .
RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags='-s -w' \
    -o /out/compiler \
    ./playground/cmd/compiler && \
    mkdir -p /out/tmp /out/cord/internal && \
    cp go.mod go.sum ./*.go /out/cord/ && \
    cp -R internal/backoff internal/serialization internal/storage /out/cord/internal/ && \
    rm -rf /out/cord/internal/storage/conformance && \
    find /out/cord -name '*_test.go' -delete && \
    find /out/cord -depth -type d -empty -delete && \
    rm -rf \
        /usr/local/go/api \
        /usr/local/go/doc \
        /usr/local/go/misc \
        /usr/local/go/src/cmd \
        /usr/local/go/test && \
    rm -rf /go/pkg/mod && \
    mkdir -p /go/pkg/mod /tmp/prewarm && \
    cp playground/examples/linear.go.txt /tmp/prewarm/main.go && \
    printf '%s\n' \
        'module playground.prewarm' \
        'go 1.27rc2' \
        'require github.com/omarluq/cord v0.0.0' \
        'replace github.com/omarluq/cord => /out/cord' \
        > /tmp/prewarm/go.mod && \
    cd /tmp/prewarm && \
    GOOS=js GOARCH=wasm CGO_ENABLED=0 GOTOOLCHAIN=local \
        go build -mod=mod -trimpath -o /out/prewarm.wasm . && \
    rm -rf /tmp/prewarm /out/prewarm.wasm

FROM ${RUNTIME_IMAGE}

ENV GOMODCACHE=/go/pkg/mod \
    GOPATH=/go \
    PATH=/usr/local/go/bin \
    GOTOOLCHAIN=local \
    PORT=8080 \
    TMPDIR=/var/tmp/cord

WORKDIR /app
COPY --from=build /out/compiler /app/compiler
COPY --from=build /usr/local/go /usr/local/go
COPY --from=build /go/pkg/mod /go/pkg/mod
COPY --from=build --chown=nonroot:nonroot /out/tmp /var/tmp/cord
COPY --from=build /out/cord /src/cord

EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app/compiler", "-cord-dir", "/src/cord"]
