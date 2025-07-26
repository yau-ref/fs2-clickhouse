package fs2.clickhouse.internal

import cats.effect.{Async, Resource}

import java.net.http.HttpClient

object ClickhouseStream {
  
  def http[F[_]: Async](
    host: String,
    port: Int = 8123,
    auth: Auth = NoAuth
  ): Resource[F, ClickhouseClient[F]] = {
    // this can cause problems when compiling on older jdks
    // because HttpClient has been updated to be AutoCloseable in Java 21 only
    // so if you see 'HttpClient is not AutoClosable' error you know why
    Resource
      .fromAutoCloseable(Async[F].delay(HttpClient.newHttpClient()))
      .map(javaHttpClient => new ClickhouseHTTPClient[F](javaHttpClient, host, port, auth))
  }

}
