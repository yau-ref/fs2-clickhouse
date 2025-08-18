package fs2.clickhouse.compression

import cats.effect.{IO, unsafe}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

trait SimpleDecompressionTest { self: Matchers =>

  def simpleDecompressionTest(
    compression: Compression,
    encodedData: String,
    expected: String
  )(implicit runtime: unsafe.IORuntime): Assertion = {
    val pipe = compression.decompress[IO]
    val decoded =
      fs2.Stream
        .emits(List(encodedData))
        .through(fs2.text.base64.decode[IO])
        .through(pipe)
        .compile
        .toVector
        .map(bytes => new String(bytes.toArray))
        .unsafeRunSync()
    decoded shouldBe expected
  }

}
