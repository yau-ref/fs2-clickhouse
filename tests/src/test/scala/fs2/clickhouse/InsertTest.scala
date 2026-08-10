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
      inserted should contain allElementsOf users
    }

    "insert data via a statement containing reserved URI characters" in withContainers { implicit clickHouseContainer =>
      withConnection { connection =>
        val statement = connection.createStatement()
        statement.execute("create database if not exists test")
        statement.execute("create table test.reserved_chars_users (name text, age Int8) Engine = MergeTree() order by name")
      }

      val user = User("a&b=c d", 42)
      // the `&`/`=` here (in a comment) exercise the same body-construction
      // path as the query string itself, not just the row data. The comment
      // has to come *before* `format JSONEachRow`: Clickhouse starts reading
      // row data from the byte right after that clause, so anything
      // trailing it (even just a comment) gets mistaken for a malformed row.
      val insertStatement =
        "insert into test.reserved_chars_users /* a&b=c */ format JSONEachRow"

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
            .emit(user)
            .through(clickhouse.insert[User](insertStatement))
        )
        .compile.drain.unsafeRunSync()

      val inserted = withConnection { connection =>
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery("select name, age from test.reserved_chars_users")
        val buffer = scala.collection.mutable.ArrayBuffer.empty[User]
        while (resultSet.next())
          buffer += User(resultSet.getString("name"), resultSet.getInt("age"))
        buffer.toVector
      }

      inserted should contain(user)
    }

    "insert data spanning multiple batches" in withContainers { implicit clickHouseContainer =>
      val users = TestData.users(250)

      withConnection { connection =>
        val statement = connection.createStatement()
        statement.execute("create database if not exists test")
        statement.execute("create table test.batched_users (name text, age Int8) Engine = MergeTree() order by name")
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
            // a batch size that doesn't evenly divide the number of rows,
            // so the last batch is partial and still has to be flushed
            .through(clickhouse.insert[User]("insert into test.batched_users format JSONEachRow", maxBatchSize = 37))
        )
        .compile.drain.unsafeRunSync()

      val inserted = withConnection { connection =>
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery("select name, age from test.batched_users")
        val buffer = scala.collection.mutable.ArrayBuffer.empty[User]
        while (resultSet.next())
          buffer += User(resultSet.getString("name"), resultSet.getInt("age"))
        buffer.toVector
      }

      inserted should contain allElementsOf users
      inserted should have size users.size.toLong
    }
  }

}
