package fs2.clickhouse.internal

import cats.data.EitherT
import io.circe

import scala.annotation.implicitNotFound
import scala.language.implicitConversions
import io.circe.parser.{decode => circeDecode}
import cats.effect.kernel.Sync

// TODO: same for encoding
@implicitNotFound("You should either implement row decoder or provide conversion from existing one")
trait JsonRowDecoder[F[_], T] {

  type Err <: Throwable
  type DecodedRow = EitherT[F, Err, T]
  def decode(json: String): DecodedRow

}

object JsonRowDecoder {

  // TODO: impl and move to own sub project to keep pluggable

  /** Conversion from existing circe decoder
   *
   * Example:
   * {{{
   *   import io.circe.generic.auto._
   *   import JsonRowDecoder.circeWrapper
   *
   *   case class User(name: String, age: Int)
   *
   *   val stream =
   *       fs2.Stream
   *         .resource(ClickhouseStream.http[IO](host))
   *         .flatMap(_.query[User]("select * from users"))
   * }}}
   *
   */
  implicit def circeWrapper[F[_]: Sync, T: io.circe.Decoder]: JsonRowDecoder[F, T] =
    new JsonRowDecoder[F, T] {
      override type Err = circe.Error
      override def decode(json: String): DecodedRow =
        EitherT(Sync[F].delay(circeDecode[T](json)))
    }

}
