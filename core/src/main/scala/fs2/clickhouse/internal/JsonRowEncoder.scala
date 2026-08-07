package fs2.clickhouse.internal

import cats.data.EitherT
import cats.effect.Async

import scala.annotation.implicitNotFound
import scala.language.implicitConversions

@implicitNotFound(
  "You should either implement row encoder or provide conversion from existing one"
)
trait JsonRowEncoder[F[_], T] {
  type EncodedRow = EitherT[F, Throwable, String]
  def encode(row: T): EncodedRow
}

object JsonRowEncoder {

  /** Default encoder which just passes strings through as is in case you don't care
    * about encoding them
    */
  implicit def stringEncoder[F[_]: Async]: JsonRowEncoder[F, String] =
    new JsonRowEncoder[F, String] {
      override def encode(row: String): EncodedRow =
        EitherT.right(Async[F].pure(row))
    }
}
