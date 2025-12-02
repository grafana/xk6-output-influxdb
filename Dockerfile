FROM golang:1.25-alpine3.21@sha256:3289aac2aac769e031d644313d094dbda745f28af81cd7a94137e73eefd58b33 AS builder
WORKDIR $GOPATH/src/go.k6.io/k6
ADD . .
RUN apk --no-cache add git
RUN go install go.k6.io/xk6/cmd/xk6@latest
RUN xk6 build --with github.com/grafana/xk6-output-influxdb=. --output /tmp/k6

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && \
    adduser -D -u 12345 -g 12345 k6
COPY --from=builder /tmp/k6 /usr/bin/k6

USER 12345
WORKDIR /home/k6
ENTRYPOINT ["k6"]
