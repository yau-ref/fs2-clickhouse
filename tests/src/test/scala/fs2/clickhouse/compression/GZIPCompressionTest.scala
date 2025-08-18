package fs2.clickhouse.compression

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import fs2.clickhouse.{TestContainerHelpers, WithConnection}
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class GZIPCompressionTest
  extends AnyWordSpec
    with Matchers
    with Inspectors
    with TestContainerForAll
    with TestContainerHelpers
    with WithConnection {

  "GZIPCompressions" should {
    "decompress data" in {
      val pipe = ZSTD.decompress[IO]
      val testData = "KLUv/QRYWQAAaGVsbG93b3JsZAp/WzH0"
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

