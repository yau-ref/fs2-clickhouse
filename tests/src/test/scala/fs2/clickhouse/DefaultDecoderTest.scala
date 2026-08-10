package fs2.clickhouse

import cats.effect.IO
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DefaultDecoderTest extends AnyWordSpec with Matchers with Inspectors {

  private val clickhouse: ClickhouseStream[IO] =
    null // null doesn't matter for this group of tests

  "default decoder" should {
    "be provided" when {
      "T is a String" in {
        """clickhouse.query[String]("select * from test.users")""" should compile
      }
    }

    "not be provided" when {
      "T is a custom type" in {
        case class Users(name: String, age: Int) // TODO: dry
        """clickhouse.query[Users]("select * from test.users")""" shouldNot typeCheck
      }
    }
  }

}
