package fs2.clickhouse 

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.clickhouse.internal.{ClickhouseStream, JsonRowDecoder}
import io.circe.Decoder
import io.circe.generic.auto._
import fs2.clickhouse.circe._

// tmp main for dev purposes
object Main {

  def main(args: Array[String]): Unit = {

    case class User(name: String, age: Int)

    val stream =
      fs2.Stream
        .resource(ClickhouseStream.http[IO]("localhost"))
        .flatMap(_.query[User]("select * from users"))

    val exec: IO[List[String]] = stream.compile.toList
    println(exec.unsafeRunSync())

  }

}
