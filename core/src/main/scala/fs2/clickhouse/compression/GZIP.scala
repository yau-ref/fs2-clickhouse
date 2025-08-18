package fs2.clickhouse.compression

import cats.effect.Async
import fs2.Pipe
import fs2.io.compression.fs2ioCompressionForAsync

/**
 * The simplest one, requires no extra dependencies
 * so could be a part of the core 
 */
object GZIP extends Compression {

  override val acceptEncoding: Option[String] = Some("gzip")

  override def decompress[F[_]: Async]: Pipe[F, Byte, Byte] =
    fs2ioCompressionForAsync[F]
      .gunzip()
      .andThen(_.flatMap(_.content))

  override def compress[F[_]: Async]: Pipe[F, Byte, Byte] = ??? // TODO: impl

}
