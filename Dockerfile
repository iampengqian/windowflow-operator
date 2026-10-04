FROM golang:1.26 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /windowflow ./cmd/windowflow

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /windowflow /windowflow
USER 1000:1000
ENTRYPOINT ["/windowflow"]
