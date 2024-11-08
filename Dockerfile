FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o archiver

FROM alpine:latest
COPY --from=builder /app/archiver /archiver
ENTRYPOINT ["/archiver"]