FROM golang:1.17-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o minidfs ./src

FROM alpine:edge

COPY --from=builder ["/build/minidfs", "/"]
RUN mkdir data

ENTRYPOINT ["/minidfs"]
