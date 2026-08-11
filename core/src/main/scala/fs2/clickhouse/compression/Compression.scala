package fs2.clickhouse.compression

import cats.effect.Async
import fs2.Pipe

/** Clickhouse http api supports a number of compression algorithms
  * https://clickhouse.com/docs/interfaces/http#compression
  */
trait Compression {
  def acceptEncoding: Option[String]
  def decompress[F[_]: Async]: Pipe[F, Byte, Byte]
  def compress[F[_]: Async]: Pipe[F, Byte, Byte]
}

object Compression {

  /** Many decompressors validate their header eagerly and throw on empty
    * input, but an empty body is a valid, expected response (e.g. a
    * successful insert's response) - this wraps a decompress `Pipe` so an
    * empty stream short-circuits to an empty stream instead of reaching it.
    */
  private[compression] def skipIfEmpty[F[_]](
    pipe: Pipe[F, Byte, Byte]
  ): Pipe[F, Byte, Byte] =
    _.pull.peek1.flatMap {
      case None               => fs2.Pull.done
      case Some((_, stream)) => stream.through(pipe).pull.echo
    }.stream

}
