package fs2.clickhouse.internal

import cats.effect.Async
import fs2.Pipe
import fs2.io.compression.fs2ioCompressionForAsync

trait Compression {
  def acceptEncoding: String
  def decompress[F[_]: Async]: Pipe[F, Byte, Byte]
  def compress[F[_]: Async]: Pipe[F, Byte, Byte]
}

object GZIP extends Compression {

  override val acceptEncoding = "gzip"

  override def decompress[F[_]: Async]: Pipe[F, Byte, Byte] =
    fs2ioCompressionForAsync[F]
      .gunzip()
      .andThen(_.flatMap(_.content))

  override def compress[F[_]: Async]: Pipe[F, Byte, Byte] = ??? // TODO: impl
  
}

