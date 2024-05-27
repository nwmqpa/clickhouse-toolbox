##
## Builder
##
FROM golang:1.22-alpine as build

WORKDIR /go/src/app

COPY go.mod go.sum ./

RUN go mod download

COPY *.go ./
COPY cmd ./cmd
COPY pkg ./pkg

RUN CGO_ENABLED=0 GOOS=linux go build -o clickhouse-toolbox

CMD [ "/usr/bin/clickhouse-toolbox" ]

##
## Certificates
##
FROM alpine as certificates

RUN apk add -U --no-cache ca-certificates

##
## Deploy
##
FROM alpine as final
LABEL maintainer="thomas.nicollet@nebulis.io"
LABEL author="thomas.nicollet@nebulis.io"

WORKDIR /

COPY --from=build /go/src/app/clickhouse-toolbox /clickhouse-toolbox
COPY --from=certificates /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

CMD ["/clickhouse-toolbox"]
