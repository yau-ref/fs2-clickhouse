package fs2.clickhouse

import cats.data.EitherT
import cats.effect.kernel.Sync
import fs2.clickhouse.internal.JsonRowDecoder
import fs2.clickhouse.internal.JsonRowEncoder
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

  /** Conversion from existing circe encoder
    *
    * Example:
    * {{{
    *   import io.circe.generic.auto._
    *   import fs2.clickhouse.circe.circeEncoderWrapper
    *
    *   case class User(name: String, age: Int)
    *
    *   val stream: Pipe[F, User, Nothing] =
    *     clickhouse.insert[User]("insert into test.users format JSONEachRow")
    * }}}
    */
  implicit def circeEncoderWrapper[F[_]: Sync, T: circe.Encoder]
    : JsonRowEncoder[F, T] =
    new JsonRowEncoder[F, T] {
      override def encode(row: T): EncodedRow =
        EitherT.right(Sync[F].delay(circe.Encoder[T].apply(row).noSpaces))
    }

}
