FROM golang:1.17-alpine as builder
RUN go install go.k6.io/xk6/cmd/xk6@latest
RUN apk --no-cache add git
WORKDIR $GOPATH/src/go.k6.io/k6
ADD . .
RUN xk6 build --output /go/bin/k6 \
	--with github.com/grafana/xk6-output-influxdb=.

FROM alpine:3.14
RUN apk add --no-cache ca-certificates && \
    adduser -D -u 12345 -g 12345 k6
COPY --from=builder /go/bin/k6 /usr/bin/k6

USER 12345
WORKDIR /home/k6
ENTRYPOINT ["k6"]
