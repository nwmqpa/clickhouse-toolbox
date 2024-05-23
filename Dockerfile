FROM golang:1.22-alpine

LABEL maintainer="thomas.nicollet@nebulis.io"

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY *.go ./
COPY cmd ./cmd
COPY pkg ./pkg

RUN CGO_ENABLED=0 GOOS=linux go build -o /usr/bin/clickhouse-toolbox

CMD [ "/usr/bin/clickhouse-toolbox" ]
