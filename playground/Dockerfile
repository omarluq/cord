# syntax=docker/dockerfile:1.26

ARG GO_IMAGE=golang@sha256:65b6f280bf050ec5af12716857e8ea8439d694dbba8f31ceeb7630670071f2bb
ARG RUNTIME_IMAGE=gcr.io/distroless/static-debian12@sha256:d75cdd72874d4790092fcb1b058493ecf6bb5bf2b2b897045b00ff01d91843f2

FROM ${GO_IMAGE} AS build
WORKDIR /src

COPY go.mod go.sum ./
COPY playground/go.mod playground/go.sum ./playground/
RUN go mod download && go mod download -C playground

COPY . .
# After creating the minimal Cord bundle, prune the module cache and rebuild a
# representative module. The runtime's offline cache then contains only the
# dependencies needed by playground compilation, and the build validates the bundle.
RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags='-s -w' \
    -o /out/compiler \
    ./playground/cmd/compiler && \
    mkdir -p /out/tmp /out/cord/internal && \
    cp go.mod go.sum ./*.go /out/cord/ && \
    cp -R internal/backoff internal/hashframe internal/serialization internal/storage /out/cord/internal/ && \
    rm -rf /out/cord/internal/storage/conformance && \
    find /out/cord -name '*_test.go' -delete && \
    find /out/cord -depth -type d -empty -delete && \
    chmod -R a+rX /out/cord && \
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
        'go 1.27.0' \
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
