package fs2.clickhouse.internal

import fs2.Pipe

import scala.concurrent.duration.FiniteDuration

trait ClickhouseClient[F[_]] {

  def query[T](q: String, timeout: Option[FiniteDuration] = None)(implicit decoder: JsonRowDecoder[F, T]): fs2.Stream[F, T] 
  
  def insert[T](statement: String): Pipe[F, T, Nothing]
  
}
