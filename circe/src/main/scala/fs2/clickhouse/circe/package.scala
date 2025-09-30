package fs2.clickhouse

import cats.data.EitherT
import cats.effect.kernel.Sync
import fs2.clickhouse.internal.JsonRowDecoder
import io.circe

package object circe {

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
    */
  implicit def circeDecoderWrapper[F[_]: Sync, T: circe.Decoder]
    : JsonRowDecoder[F, T] =
    new JsonRowDecoder[F, T] {
      override def decode(json: String): DecodedRow =
        EitherT(Sync[F].delay(circe.parser.decode[T](json)))
    }

}
