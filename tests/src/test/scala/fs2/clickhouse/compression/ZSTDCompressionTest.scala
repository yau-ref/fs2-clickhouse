package fs2.clickhouse.compression

import cats.effect.testing.scalatest.AsyncIOSpec
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import fs2.clickhouse.{TestContainerHelpers, WithConnection}
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class ZSTDCompressionTest
    extends AsyncWordSpec
    with AsyncIOSpec
    with Matchers
    with Inspectors
    with TestContainerForAll
    with TestContainerHelpers
    with WithConnection
    with CompressionBehaviors {

  "ZSTDCompression" should {
    behave like decompressionBehavior(
      ZSTD,
      "helloworld\n",
      "KLUv/QRYWQAAaGVsbG93b3JsZAp/WzH0"
    )
    behave like integratedDecompressionBehavior(ZSTD)
    behave like emptyDecompressionBehavior(ZSTD)
    behave like compressionRoundTripBehavior(ZSTD, "helloworld\n")
    behave like integratedInsertBehavior(ZSTD)
  }

}
