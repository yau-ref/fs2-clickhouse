package fs2.clickhouse

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import fs2.clickhouse.internal.ZSTDCompression
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ZSTDCompressionTest
  extends AnyWordSpec
    with Matchers
    with Inspectors
    with TestContainerForAll
    with TestContainerHelpers
    with WithConnection {

  "ZSTDCompressions" should {
    "compress data" in {
      val pipe =
        ZSTDCompression
          .defaultInstance
          .decompress[IO]

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
