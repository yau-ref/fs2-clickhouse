package fs2.clickhouse.internal

import cats.data.EitherT

import scala.annotation.implicitNotFound
import scala.language.implicitConversions

// TODO: same for encoding
@implicitNotFound("You should either implement row decoder or provide conversion from existing one")
trait JsonRowDecoder[F[_], T] {

  type Err <: Throwable
  type DecodedRow = EitherT[F, Err, T]
  def decode(json: String): DecodedRow

}
