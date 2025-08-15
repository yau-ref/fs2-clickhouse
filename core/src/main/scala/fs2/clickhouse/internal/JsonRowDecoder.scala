package fs2.clickhouse.internal

import cats.data.EitherT
import cats.effect.Async

import scala.annotation.implicitNotFound
import scala.language.implicitConversions

// TODO: same for encoding
@implicitNotFound("You should either implement row decoder or provide conversion from existing one")
trait JsonRowDecoder[F[_], T] {

  type Err <: Throwable
  type DecodedRow = EitherT[F, Err, T]
  def decode(json: String): DecodedRow

}

object JsonRowDecoder {
  implicit def stringDecoder[F[_] : Async]: JsonRowDecoder[F, String] =
    new JsonRowDecoder[F, String] {
      override type Err = IllegalArgumentException

      override def decode(json: String): DecodedRow =
        EitherT.right(Async[F].pure(json))
    }
}
