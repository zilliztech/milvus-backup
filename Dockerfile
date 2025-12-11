FROM golang:1.25 AS builder

ENV CGO_ENABLED=0

ARG VERSION=0.0.1
ARG COMMIT=unknown

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN make build

FROM alpine:3.17

WORKDIR /app
RUN apk update && apk add --no-cache \
    ca-certificates \
    curl

COPY --from=builder /app/milvus-backup .
COPY --from=builder /app/configs ./configs
EXPOSE 8080
ENTRYPOINT ["/app/milvus-backup", "server"]
