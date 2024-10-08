# syntax=docker/dockerfile:1

FROM golang:alpine AS build-stage
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
COPY pkg pkg
RUN CGO_ENABLED=0 GOOS=linux go build -o /slask-tracker

FROM gcr.io/distroless/base-debian11 
WORKDIR /

#EXPOSE 25
EXPOSE 8080

#COPY *.html /
COPY --from=build-stage /slask-tracker /slask-tracker
ENTRYPOINT ["/slask-tracker"]