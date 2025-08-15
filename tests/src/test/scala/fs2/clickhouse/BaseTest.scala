package fs2.clickhouse

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.dimafeng.testcontainers.ClickHouseContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import fs2.clickhouse.circe._
import fs2.clickhouse.internal.Credentials
import io.circe.generic.auto._
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BaseTest
  extends AnyWordSpec
    with Matchers
    with Inspectors
    with TestContainerForAll 
    with TestContainerHelpers 
    with WithConnection {

  override val containerDef: ClickHouseContainer.Def =
    ClickHouseContainer.Def()
  
  "client" should {
    "read data" in withContainers { implicit clickHouseContainer =>

      case class User(name: String, age: Int)
      val users =
        (0 to 10000)
          .map(i => User(s"user-$i", i % 100))
          .toVector

      withConnection { connection =>
        val statement = connection.createStatement()
        statement.execute("create table users ( name text, age Int8) Engine = MergeTree() order by name")
        users.foreach( user =>
          statement.execute(s"insert into users values ('${user.name}', ${user.age})")
        )
      }
      println("Inserted")

//      println("http://localhost:" + clickHouseContainer.mappedPort(8123))
//      Thread.sleep(10 * 60 * 1000)

      val result =
        fs2.Stream
          .resource(
            ClickhouseStream
              .http[IO](
                hostName,
                httpApiPort,
                Credentials(username, Some(password))
              )
          ).flatMap(clickhouse =>
            clickhouse.query[User]("select * from test.users")
          ).compile.toList.unsafeRunSync()


      result should contain allElementsOf users
    }
  }

}
