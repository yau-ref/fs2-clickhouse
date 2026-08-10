# fs2-clickhouse

[![Continuous Integration](https://github.com/yau-ref/fs2-clickhouse/actions/workflows/scala.yml/badge.svg)](https://github.com/yau-ref/fs2-clickhouse/actions/workflows/scala.yml)

An fs2 / cats-effect streaming client for ClickHouse, built on its HTTP interface.

## Requirements

- Scala 2.13 or 3
- JDK 11+

## Features

- Stream query results from ClickHouse
- Stream inserts into ClickHouse, auto-batched by size and time
- JSON row encoding/decoding, with optional circe-based codecs
- Request/response compression (gzip, LZ4, ZSTD)

## Modules

- [core](core/README.md) — the client itself: querying, inserting, JSON encoding, and gzip compression
- [circe](circe/README.md) — JSON row encoders/decoders based on circe
- [compression](compression/README.md) — additional compression codecs (LZ4, ZSTD)
- [tests](tests/README.md) — unit and testcontainers-based integration tests
