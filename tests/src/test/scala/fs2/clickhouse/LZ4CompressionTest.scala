package fs2.clickhouse

import cats.effect.IO
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import cats.effect.unsafe.implicits.global
import compression.LZ4

class LZ4CompressionTest
  extends AnyWordSpec
    with Matchers
    with Inspectors
    with TestContainerForAll
    with TestContainerHelpers
    with WithConnection {

  "LZ4Compression" should {
    "compress data" in {
      val pipe = LZ4.decompress[IO]
      val testData = "BCJNGGRApwsAAIBoZWxsb3dvcmxkCgAAAADxpNsz"
      val decoded =
        fs2.Stream
          .emits(List(testData))
          .through(fs2.text.base64.decode[IO])
          .through(pipe)
          .compile
          .toVector
          .map(bytes => new String(bytes.toArray))
          .unsafeRunSync()

      decoded shouldBe "helloworld\n"
    }
  }

}
