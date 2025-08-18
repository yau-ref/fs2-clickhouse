package fs2.clickhouse.compression

import cats.effect.Async
import fs2.Pipe

/**
 * Clickhouse http api supports a number of compression algorithms
 * https://clickhouse.com/docs/interfaces/http#compression
 */
trait Compression {
  def acceptEncoding: Option[String]
  def decompress[F[_]: Async]: Pipe[F, Byte, Byte]
  def compress[F[_]: Async]: Pipe[F, Byte, Byte]
}
