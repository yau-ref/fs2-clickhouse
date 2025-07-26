package fs2.clickhouse.internal

import cats.effect.{Async, IO, Resource}
import fs2.clickhouse.internal.{Auth, ClickhouseClient }

import java.net.http.HttpClient

object ClickhouseStream {
  
  def http[F[_]: Async](
    host: String,
    port: Int,
    auth: Auth 
  ): Resource[F, ClickhouseClient[F]] = 
    Resource
      .fromAutoCloseable(Async[F].delay(HttpClient.newHttpClient()))
      .map( httpClient =>
        ???
      )
      
}
