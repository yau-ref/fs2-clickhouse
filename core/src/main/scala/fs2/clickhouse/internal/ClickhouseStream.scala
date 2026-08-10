package fs2.clickhouse.internal

import cats.effect.{Async, Resource}
import fs2.clickhouse.compression.{Compression, GZIP}

import java.net.http.HttpClient

object ClickhouseStream {

  /** Usage sample
    *
    * {{{
    * fs2.Stream
    * .resource(ClickhouseStream.http[IO]("localhost"))
    * .flatMap(_.query("select * from users"))
    * }}}
    */
  def http[F[_]: Async](
    host: String,
    port: Int = 8123,
    auth: Auth = NoAuth,
    compression: Compression = GZIP
  ): Resource[F, ClickhouseClient[F]] =
    // not using fromAutoClosable because HttpClient has been updated to be
    // AutoCloseable in Java 21 only and since people using JDK > 8 is a fable we have to
    // use legacy approach here
    Resource
      .fromAutoCloseable(Async[F].delay(HttpClient.newHttpClient()))
      .map(javaHttpClient =>
        new ClickhouseHTTPClient[F](
          javaHttpClient,
          host,
          port,
          auth,
          compression
        )
      )

  }
}
