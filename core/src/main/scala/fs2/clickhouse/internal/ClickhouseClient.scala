package fs2.clickhouse.internal

import fs2.Pipe

import scala.concurrent.duration.{FiniteDuration, DurationInt}

trait ClickhouseClient[F[_]] {

  def query[T](q: String, timeout: Option[FiniteDuration] = None)(implicit
    decoder: JsonRowDecoder[F, T]
  ): fs2.Stream[F, T]

  /** Batches the input stream before inserting: rows are buffered until
    * either `maxBatchSize` rows have accumulated or `maxBatchWait` has
    * elapsed since the last batch, whichever comes first, and each batch
    * is sent as its own HTTP request. This bounds the size/duration of any
    * single request and keeps a failure from invalidating rows already
    * flushed in earlier batches.
    */
  def insert[T](
    statement: String,
    maxBatchSize: Int = 1000,
    maxBatchWait: FiniteDuration = 1.second
  )(implicit
    encoder: JsonRowEncoder[F, T]
  ): Pipe[F, T, Nothing]

}
