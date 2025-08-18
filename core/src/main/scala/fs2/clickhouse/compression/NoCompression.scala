package fs2.clickhouse.compression

import cats.effect.Async
import fs2.Pipe

/**
 * In case you hate compression
 */
object NoCompression extends Compression {

  override def acceptEncoding: Option[String] = None

  override def decompress[F[_] : Async]: Pipe[F, Byte, Byte] = identity

  override def compress[F[_] : Async]: Pipe[F, Byte, Byte] = ???

}
