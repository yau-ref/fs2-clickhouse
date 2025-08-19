package fs2.clickhouse.compression

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import fs2.clickhouse.internal.{ClickhouseStream, Credentials}
import fs2.clickhouse.{TestContainerHelpers, TestData, WithConnection}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

trait CompressionBehaviors {
  self: AsyncWordSpec
    with AsyncIOSpec
    with Matchers
    with TestContainerForAll
    with TestContainerHelpers
    with WithConnection =>

  // TODO: split
  def decompressionBehavior(
    compression: Compression,
    sampleData: String,
    samplePreCompressedData: String
  ) = {

    "decompress pre-compressed data" in {
      // Checks whether provided compression is actually capable of decompression the data
      // by applying it to sample pre-compressed data known to be valid
      val pipe = compression.decompress[IO]
      fs2.Stream
        .emits(List(samplePreCompressedData))
        .through(fs2.text.base64.decode[IO])
        .through(pipe)
        .compile
        .toVector
        .map(bytes => new String(bytes.toArray))
        .map(_ shouldBe sampleData)
    }

  }

  def integratedDecompressionBehavior(compression: Compression) = {
    "decompress data from clickhouse" in withContainers { implicit clickHouseContainer =>
      // Checks that provided compression can decompress data compressed by clickhouse.
      // This helps to check whether compression uses parameters compatible
      val users = TestData.users()
      withConnection { connection =>
        val statement = connection.createStatement()
        statement.execute("create table users ( name text, age Int8) Engine = MergeTree() order by name")
        users.foreach(user =>
          statement.execute(s"insert into users values ('${user.name}', ${user.age})")
        )
      }
      fs2.Stream
        .resource(
          ClickhouseStream
            .http[IO](
              hostName,
              httpApiPort,
              Credentials(username, Some(password)),
              compression = compression
            )
        )
        .flatMap(_.query("select * from test.users"))
        .compile
        .toList
        .map(_ should have size (users.length))
    }
  }

}
