package fs2.clickhouse.compression

import cats.effect.Async
import fs2.Pipe
import fs2.io.compression.fs2ioCompressionForAsync

/**
 * Clickhouse http api supports a number of compression algorithms
 * https://clickhouse.com/docs/interfaces/http#compression
 */
trait Compression {
  def acceptEncoding: Option[String]
  def decompress[F[_]: Async]: Pipe[F, Byte, Byte]
  def compress[F[_]: Async]: Pipe[F, Byte, Byte]
}

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

/**
 * In case you hate compression
 */
object NoCompression extends Compression {

  override def acceptEncoding: Option[String] = None

  override def decompress[F[_] : Async]: Pipe[F, Byte, Byte] = identity

  override def compress[F[_] : Async]: Pipe[F, Byte, Byte] = ???

}

