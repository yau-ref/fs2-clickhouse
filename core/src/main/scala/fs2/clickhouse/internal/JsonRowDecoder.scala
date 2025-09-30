package fs2.clickhouse.internal

import cats.data.EitherT
import cats.effect.Async

import scala.annotation.implicitNotFound
import scala.language.implicitConversions

// TODO: same for encoding
@implicitNotFound(
  "You should either implement row decoder or provide conversion from existing one"
)
trait JsonRowDecoder[F[_], T] {
  type DecodedRow = EitherT[F, Throwable, T] 
  def decode(json: String): DecodedRow
}

object JsonRowDecoder {

  // TODO: maybe better call it stringifyingDecoder or something
  /** Default decoder which just returns json lines as is in case you don't care
    * about decoding them
    *
    * Example:
    * {{{
    *    clickhouse
    *      .query[String]("select * from test.users")
    *      .through(Files[IO].writeUtf8Lines(Path("out.txt")))
    * }}}
    */
  implicit def stringDecoder[F[_]: Async]: JsonRowDecoder[F, String] =
    new JsonRowDecoder[F, String] {
      override def decode(json: String): DecodedRow =
        EitherT.right(Async[F].pure(json))
    }
}
