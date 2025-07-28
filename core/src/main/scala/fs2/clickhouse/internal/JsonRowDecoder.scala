package fs2.clickhouse.internal

import cats.data.EitherT

// TODO: same for encoding
trait JsonRowDecoder[F[_], T] {

  type Err <: Throwable
  type DecodedRow = EitherT[F, T, Err]
  def decode(json: String): DecodedRow

}

object JsonRowDecoder {

  // TODO: impl and move to own sub project to keep pluggable
  implicit def circeWrapper[F[_], T](): JsonRowDecoder[F, T] = ???

}
