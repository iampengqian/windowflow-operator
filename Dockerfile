FROM --platform=$BUILDPLATFORM golang:1.26 AS builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags='-s -w' -o /windowflow ./cmd/windowflow

FROM gcr.io/distroless/static:nonroot
LABEL org.opencontainers.image.source="https://github.com/iampengqian/windowflow-operator"
LABEL org.opencontainers.image.licenses="Apache-2.0"
COPY --from=builder /windowflow /windowflow
USER 1000:1000
ENTRYPOINT ["/windowflow"]
