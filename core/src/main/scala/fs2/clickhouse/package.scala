package fs2.clickhouse

import cats.effect.IO
import cats.effect.kernel.Async
import fs2.Pipe
import fs2.clickhouse.internal.{ClickhouseClient, ClickhouseHTTPClient}

package object clickhouse {
  
  // public api
  
  type ClickhouseStream[F[_]] = ClickhouseClient[F]
  val ClickhouseStream = internal.ClickhouseStream

  type ClickhouseSink[F[_], I] = Pipe[F, I, Nothing]

  
}
