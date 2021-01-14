FROM golang:1.13 AS build
WORKDIR /go/src
COPY src/iotdbrestimpl ./iotdbrestimpl
COPY src/main.go .
COPY src/go.mod ./go.mod
COPY target/gen/iotdbrest ../target/gen/iotdbrest

# If you compile the dockerfile in China, uncomment the followings:
ENV GOPROXY=https://goproxy.io,direct

ENV CGO_ENABLED=0
RUN go get -d -v ./...

RUN go build -a -installsuffix cgo -o iotdb-rest .

FROM scratch AS runtime
COPY --from=build /go/src/iotdb-rest ./
EXPOSE 8080/tcp
ENTRYPOINT ["./iotd-brest"]
