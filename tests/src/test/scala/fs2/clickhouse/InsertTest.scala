package fs2.clickhouse

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import fs2.clickhouse.TestData.User
import fs2.clickhouse.circe.*
import fs2.clickhouse.compression.NoCompression
import fs2.clickhouse.internal.Credentials
import io.circe.generic.auto.*
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class InsertTest
  extends AnyWordSpec
    with Matchers
    with Inspectors
    with TestContainerForAll
    with TestContainerHelpers
    with WithConnection {

  "client" should {
    "insert data" in withContainers { implicit clickHouseContainer =>
      val users = TestData.users()

      withConnection { connection =>
        val statement = connection.createStatement()
        statement.execute("create database if not exists test")
        statement.execute("create table test.users (name text, age Int8) Engine = MergeTree() order by name")
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
            .emits(users)
            .through(clickhouse.insert[User]("insert into test.users format JSONEachRow"))
        )
        .compile.drain.unsafeRunSync()

      val inserted = withConnection { connection =>
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery("select name, age from test.users")
        val buffer = scala.collection.mutable.ArrayBuffer.empty[User]
        while (resultSet.next())
          buffer += User(resultSet.getString("name"), resultSet.getInt("age"))
        buffer.toVector
      }
      inserted.foreach(println)
      inserted should contain allElementsOf users
    }
  }

}
