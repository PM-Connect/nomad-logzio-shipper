FROM golang:1.11-alpine as dev

RUN apk --no-cache --no-progress add git bash gcc musl-dev make curl tar \
    && mkdir -p /go/src/github.com/pm-connect/tent

RUN go get -u golang.org/x/lint/golint \
    && go get -u github.com/golang/dep/cmd/dep

WORKDIR /go/src/github.com/pm-connect/tent

FROM dev as build

COPY . /go/src/github.com/pm-connect/tent