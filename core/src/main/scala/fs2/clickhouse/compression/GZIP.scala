package fs2.clickhouse.compression

import cats.effect.Async
import fs2.Pipe
import fs2.io.compression.fs2ioCompressionForAsync

/** The simplest one, requires no extra dependencies so could be a part of the
  * core
  */
object GZIP extends Compression {

  override val acceptEncoding: Option[String] = Some("gzip")

  // gunzip() throws on empty input (it expects at least a gzip header), but
  // an empty body is a valid, expected response (e.g. a successful insert),
  // so an empty stream has to be short-circuited before reaching it.
  override def decompress[F[_]: Async]: Pipe[F, Byte, Byte] =
    _.pull.peek1.flatMap {
      case None => fs2.Pull.done
      case Some((_, stream)) =>
        stream
          .through(fs2ioCompressionForAsync[F].gunzip())
          .flatMap(_.content)
          .pull
          .echo
    }.stream

  override def compress[F[_]: Async]: Pipe[F, Byte, Byte] =
    fs2ioCompressionForAsync[F].gzip()

}
