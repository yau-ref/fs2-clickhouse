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
- [tests](tests/README.md) — unit and testcontainers-based integration tests, including property-based tests

## Building an assembly jar

Each published module (`core`, `circe`, `compression`) is set up with
[sbt-assembly](https://github.com/sbt/sbt-assembly) to produce a fat jar
bundling its dependencies. Build one with:

```sh
sbt core/assembly
sbt circe/assembly
sbt compression/assembly
```

or build all of them at once:

```sh
sbt assembly
```

The resulting jars are written to each module's `target/scala-<version>/`
directory, named `<module-name>-<version>.jar` (e.g.
`fs2-clickhouse-core-0.1.0.jar`).

## Usage

```scala
import cats.effect.IO
import fs2.clickhouse.ClickhouseStream
import fs2.clickhouse.circe._
import io.circe.generic.auto._

case class User(name: String, age: Int)
```

### Connecting

`ClickhouseStream.http` returns a `Resource[F, ClickhouseClient[F]]`: acquiring
it creates a Java `HttpClient` (and its connection pool), and the client is
only valid for the lifetime of that `Resource`. There's no explicit `close` —
the underlying `HttpClient` is left for the GC to reclaim once the resource
scope ends (Java's `HttpClient` only became `AutoCloseable` in 21, and this
library targets JDK 11+), but the `Resource` boundary is still what you
should treat as the connection's lifetime: acquire once per logical
connection/session, reuse it for all queries and inserts within that scope,
and let it close when you're done rather than re-acquiring per call.

```scala
ClickhouseStream.http[IO]("localhost").use { clickhouse =>
  clickhouse.query[User]("select * from users").compile.toList
}
```

or, composed into a larger stream via `fs2.Stream.resource` as in the
examples below.

By default connections are unauthenticated (`auth = NoAuth`). Pass
`Credentials(user, password)` for explicit credentials, or `FromEnv` to read
`CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD` from the environment:

```scala
import fs2.clickhouse.{Credentials, FromEnv}

ClickhouseStream.http[IO]("localhost", auth = Credentials("default", Some("secret")))
ClickhouseStream.http[IO]("localhost", auth = FromEnv)
```

### Query

```scala
fs2.Stream
  .resource(ClickhouseStream.http[IO]("localhost"))
  .flatMap(_.query[User]("select * from users"))
  .compile
  .toList
```

Pass a `timeout` to bound how long the query is allowed to run:

```scala
import scala.concurrent.duration._

fs2.Stream
  .resource(ClickhouseStream.http[IO]("localhost"))
  .flatMap(_.query[User]("select * from users", timeout = Some(30.seconds)))
  .compile
  .toList
```

### Insert

```scala
val users = List(User("Alice", 30), User("Bob", 25))

fs2.Stream
  .resource(ClickhouseStream.http[IO]("localhost"))
  .flatMap(clickhouse =>
    fs2.Stream
      .emits(users)
      .through(clickhouse.insert[User]("insert into users"))
  )
  .compile
  .drain
```

`insert` batches the input stream before sending it: rows are buffered until
either `maxBatchSize` rows have accumulated or `maxBatchWait` has elapsed
since the last batch (whichever comes first, default `1000`/`1.second`), and
each batch is sent as its own HTTP request. This bounds the size/duration of
any single request and keeps a failure from invalidating rows already
flushed in earlier batches:

```scala
import scala.concurrent.duration._

fs2.Stream
  .emits(users)
  .through(clickhouse.insert[User]("insert into users", maxBatchSize = 500, maxBatchWait = 5.seconds))
```

### Compression

By default requests/responses are gzip-compressed (`compression = GZIP`). Pass a different `Compression` to pick another codec, e.g. LZ4 or ZSTD from the [compression](compression/README.md) module, or `NoCompression` to disable it:

```scala
import fs2.clickhouse.compression.{LZ4, NoCompression}

fs2.Stream
  .resource(ClickhouseStream.http[IO]("localhost", compression = LZ4))
  .flatMap(_.query[User]("select * from users"))
  .compile
  .toList
```

### Error handling

Failures raised by the client are subtypes of `FS2ClickhouseException`:

- `FS2CHQueryFailed(statusCode, message, cause)` — Clickhouse rejected the
  query/insert, or the response stream failed partway through after some
  rows had already been decoded. `statusCode` is the HTTP status Clickhouse
  returned (or the status the failure was detected under), and `message`
  carries the error body Clickhouse sent back.
- `FS2CHConnectionException(cause)` — the client couldn't reach Clickhouse at
  all (wraps a `java.net.ConnectException`).
- `FS2CHDecompressionException(cause)` — decompressing the response body
  failed.

These are regular exceptions surfaced through the `F[_]` effect, so handle
them the same way you'd handle any other failure in your stream, e.g. with
`handleErrorWith`:

```scala
import fs2.clickhouse.exceptions.FS2CHQueryFailed

fs2.Stream
  .resource(ClickhouseStream.http[IO]("localhost"))
  .flatMap(_.query[User]("select * from users"))
  .handleErrorWith {
    case FS2CHQueryFailed(status, message, _) =>
      fs2.Stream.eval(IO.println(s"query failed ($status): $message")).drain
  }
  .compile
  .toList
```
