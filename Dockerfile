# Multi-stage Dockerfile for Relay Daemon & CLI
FROM golang:alpine AS builder

WORKDIR /app

ENV GOTOOLCHAIN=auto

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/relayd ./cmd/relayd
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/relay-cli ./cmd/relay-cli

FROM alpine:latest

RUN apk --no-cache add ca-certificates curl

COPY --from=builder /app/bin/relayd /usr/local/bin/relayd
COPY --from=builder /app/bin/relay-cli /usr/local/bin/relay-cli

EXPOSE 7000 50051 8080

ENTRYPOINT ["relayd"]
