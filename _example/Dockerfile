# context: ..
FROM golang:1.18 AS builder

ADD . /go/src
WORKDIR /go/src/_example

ARG CGO_ENABLED=0
RUN go build -o /go/bin/worker ./main.go

#--- runner ---
FROM scratch

COPY --from=builder /go/bin/worker /bin/worker

ENTRYPOINT ["/bin/worker"]
