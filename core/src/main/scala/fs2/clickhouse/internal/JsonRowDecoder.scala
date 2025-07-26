package fs2.clickhouse.internal

// TODO: need MonadicError here or some other way to represent fail
trait JsonRowDecoder[F[_], T] {

  def decode(json: String): F[T]
  
}
