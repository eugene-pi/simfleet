# syntax=docker/dockerfile:1

# ---- build stage ----
FROM golang:1.27 AS build
WORKDIR /src

# Dependencies first: this layer is cached and only rebuilds when go.mod or
# go.sum change, so an ordinary code edit skips the module download entirely.
# Сначала зависимости: этот слой кешируется и пересобирается только при
# изменении go.mod или go.sum, поэтому обычная правка кода полностью
# пропускает скачивание модулей.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG CMD
ARG VERSION=dev

# CGO_ENABLED=0 produces a statically linked binary with no libc dependency,
# which is what makes the scratch base below possible.
# -w -s strip debug info and the symbol table: a few MB smaller, and nothing
# in production reads them.
# CGO_ENABLED=0 даёт статически скомпонованный файл без зависимости от libc —
# именно это позволяет использовать пустой базовый образ ниже.
# -w -s убирают отладочную информацию и таблицу символов: на несколько
# мегабайт меньше, а в рабочей среде их всё равно никто не читает.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags "-w -s -X main.version=${VERSION}" \
    -o /out/app ./cmd/${CMD}

# ---- runtime stage ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
