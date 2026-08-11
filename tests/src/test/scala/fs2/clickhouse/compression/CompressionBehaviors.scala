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

    "decompress pre-compressed data" in
      // Checks whether provided compression is actually capable of decompression the data
      // by applying it to sample pre-compressed data known to be valid
      fs2.Stream
        .emits(List(samplePreCompressedData))
        .through(fs2.text.base64.decode[IO])
        .through(compression.decompress[IO])
        .compile
        .toVector
        .map(bytes => new String(bytes.toArray))
        .map(_ shouldBe sampleData)

    "throw exception if garbage in" in
      // TODO: wrap exception in some lib specific for easier handling
      assertThrows[Exception] {
        fs2.Stream
          .emits(scala.util.Random.nextString(256).getBytes())
          .through(compression.decompress[IO])
          .compile
          .drain
          .unsafeRunSync()
      }

  }

  def emptyDecompressionBehavior(compression: Compression) =
    "decompress an empty stream to an empty stream" in
      // Insert responses are expected to have an empty body on success, so
      // decompress has to handle zero bytes in without erroring.
      fs2.Stream
        .empty[IO]
        .through(compression.decompress[IO])
        .compile
        .toVector
        .map(_ shouldBe empty)

  def compressionRoundTripBehavior(compression: Compression, sampleData: String) =
    "round-trip compress and decompress data" in
      // Checks that data compressed by this codec can be decompressed back
      // to itself, catching codecs whose compress side is broken/unimplemented.
      fs2.Stream
        .emits(List(sampleData))
        .through(fs2.text.utf8.encode[IO])
        .through(compression.compress[IO])
        .through(compression.decompress[IO])
        .compile
        .toVector
        .map(bytes => new String(bytes.toArray))
        .map(_ shouldBe sampleData)

  def integratedDecompressionBehavior(compression: Compression) =
    "decompress data from clickhouse" in withContainers {
      implicit clickHouseContainer =>
        // Checks that provided compression can decompress data compressed by clickhouse.
        // This helps to check whether compression uses parameters compatible
        val users = TestData.users()
        withConnection { connection =>
          val statement = connection.createStatement()
          statement.execute(
            "create table users ( name text, age Int8) Engine = MergeTree() order by name"
          )
          users.foreach(user =>
            statement.execute(
              s"insert into users values ('${user.name}', ${user.age})"
            )
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
