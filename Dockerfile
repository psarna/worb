FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /worb .

FROM debian:bookworm-slim
COPY --from=build /worb /usr/local/bin/worb
EXPOSE 8080
ENTRYPOINT ["worb", "--host", "0.0.0.0"]
