FROM golang:1.24-alpine AS builder
RUN apk update && apk upgrade --no-cache
WORKDIR /app
COPY . .
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown
RUN CGO_ENABLED=0 GOOS=linux go build -v \
  -ldflags "-s -w \
  -X 'github.com/pgflo/pg_flo/cmd.version=${VERSION}' \
  -X 'github.com/pgflo/pg_flo/cmd.commit=${COMMIT}' \
  -X 'github.com/pgflo/pg_flo/cmd.date=${DATE}'" \
  -o pg_flo .

FROM alpine:latest
RUN apk update && apk upgrade --no-cache && \
  apk add --no-cache postgresql15-client
COPY --from=builder /app/pg_flo /usr/local/bin/
ENTRYPOINT ["pg_flo"]
