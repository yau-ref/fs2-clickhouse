package fs2.clickhouse.compression

import cats.effect.Async
import fs2.Pipe
import fs2.io.{readOutputStream, writeOutputStream}

import java.io.OutputStream

/** Clickhouse http api supports a number of compression algorithms
  * https://clickhouse.com/docs/interfaces/http#compression
  */
trait Compression {
  def acceptEncoding: Option[String]
  def decompress[F[_]: Async]: Pipe[F, Byte, Byte]
  def compress[F[_]: Async]: Pipe[F, Byte, Byte]
}

object Compression {

  /** Many decompressors validate their header eagerly and throw on empty input,
    * but an empty body is a valid, expected response (e.g. a successful
    * insert's response) - this wraps a decompress `Pipe` so an empty stream
    * short-circuits to an empty stream instead of reaching it.
    */
  private[compression] def skipIfEmpty[F[_]](
    pipe: Pipe[F, Byte, Byte]
  ): Pipe[F, Byte, Byte] =
    _.pull.peek1.flatMap {
      case None              => fs2.Pull.done
      case Some((_, stream)) => stream.through(pipe).pull.echo
    }.stream

  /** Adapts a `java.io.OutputStream`-based compressor into a compress `Pipe`,
    * by piping bytes through an instance of it wrapping an fs2-managed
    * `OutputStream`. `chunkSize` sizes that internal transfer buffer, not the
    * compressor's own block size.
    */
  private[compression] def compressWith[F[_]: Async](
    chunkSize: Int
  )(wrap: OutputStream => OutputStream): Pipe[F, Byte, Byte] =
    in =>
      readOutputStream[F](chunkSize) { os =>
        in.through(writeOutputStream[F](Async[F].delay(wrap(os)))).compile.drain
      }

}
