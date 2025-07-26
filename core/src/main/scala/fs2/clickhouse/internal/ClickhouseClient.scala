package fs2.clickhouse.internal

import cats.effect.Async
import fs2.Pipe

import scala.concurrent.duration.FiniteDuration

trait ClickhouseClient[F[_]] {
  
  def query(q: String, timeout: Option[FiniteDuration] = None): fs2.Stream[F, String]
  
  def insert[T](statement: String): Pipe[F, T, Nothing]
  
  
}
