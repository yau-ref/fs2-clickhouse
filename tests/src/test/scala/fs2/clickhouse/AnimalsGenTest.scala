package fs2.clickhouse

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import fs2.clickhouse.TestData.Animal
import fs2.clickhouse.circe.*
import fs2.clickhouse.compression.NoCompression
import fs2.clickhouse.internal.Credentials
import io.circe.generic.auto.*
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AnimalsGenTest
  extends AnyWordSpec
    with Matchers
    with Inspectors
    with TestContainerForAll
    with TestContainerHelpers
    with WithConnection {

  "client" should {
    
    "insert property-generated data and query it back" in withContainers { implicit clickHouseContainer =>
      val animals = TestData.animals()

      withConnection { connection =>
        val statement = connection.createStatement()
        statement.execute("create database if not exists test")
        statement.execute(
          """create table test.animals (
            |  name String,
            |  animalType String,
            |  whenLived String,
            |  sizeInMeters Float64,
            |  whenDiscovered Int32,
            |  location String,
            |  color Nullable(String),
            |  food String
            |) Engine = MergeTree() order by name""".stripMargin
        )
      }

      fs2.Stream
        .resource(
          ClickhouseStream
            .http[IO](
              hostName,
              httpApiPort,
              Credentials(username, Some(password)),
              compression = NoCompression
            )
        )
        .flatMap(clickhouse =>
          fs2.Stream
            .emits(animals)
            .through(clickhouse.insert[Animal]("insert into test.animals"))
        )
        .compile.drain.unsafeRunSync()

      val result =
        fs2.Stream
          .resource(
            ClickhouseStream
              .http[IO](
                hostName,
                httpApiPort,
                Credentials(username, Some(password)),
                compression = NoCompression
              )
          ).flatMap(clickhouse =>
            clickhouse.query[Animal]("select * from test.animals")
          ).compile.toVector.unsafeRunSync()

      result shouldNot have size 0  // to make sure there was any data
      result should have size animals.size.toLong
      result should contain allElementsOf animals
    }
  }

}
