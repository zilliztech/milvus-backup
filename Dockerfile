FROM golang:1.18 AS builder

ENV CGO_ENABLED=0
WORKDIR /app
COPY . .
RUN go mod tidy
RUN go build -ldflags="-s -w" -o /app/milvus-backup
RUN wget -P /app https://dl.min.io/client/mc/release/linux-amd64/mc
RUN chmod +x /app/mc

FROM alpine:3.17
WORKDIR /app
COPY --from=builder /app/milvus-backup .
COPY --from=builder /app/configs ./configs
COPY --from=builder /app/mc .
EXPOSE 8080
ENTRYPOINT ["/app/milvus-backup", "server"]
