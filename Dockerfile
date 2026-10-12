FROM golang:1.27.0-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/wagering ./cmd/wagering \
    && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate

FROM alpine:3.20
RUN apk add --no-cache ca-certificates \
    && adduser -D -H -u 65532 app
COPY --from=build /out/wagering /out/migrate /app/
USER app
EXPOSE 8080
ENTRYPOINT ["/app/wagering"]
