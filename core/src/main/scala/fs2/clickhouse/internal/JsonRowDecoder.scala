package fs2.clickhouse.internal

// TODO: need MonadicError here or some other way to represent fail
// TODO: same for encoding
trait JsonRowDecoder[F[_], T] {

  def decode(json: String): F[T]

}


object JsonRowDecoder {


  // TODO: impl and move to own sub project to keep pluggable
  implicit def circeWrapper[F[_], T](): JsonRowDecoder[F, T] = ???

}