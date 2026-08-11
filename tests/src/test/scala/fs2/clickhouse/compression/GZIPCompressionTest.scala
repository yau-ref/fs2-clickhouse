package fs2.clickhouse.compression

import cats.effect.testing.scalatest.AsyncIOSpec
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import fs2.clickhouse.{TestContainerHelpers, WithConnection}
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class GZIPCompressionTest
    extends AsyncWordSpec
    with AsyncIOSpec
    with Matchers
    with Inspectors
    with TestContainerForAll
    with TestContainerHelpers
    with WithConnection
    with CompressionBehaviors {

  "GZIPCompression" should {
    behave like decompressionBehavior(
      GZIP,
      "helloworld\n",
      "H4sIAAAAAAAAA8tIzcnJL88vyknhAgDmMkmaCwAAAA=="
    )
    behave like integratedDecompressionBehavior(GZIP)
    behave like emptyDecompressionBehavior(GZIP)
    behave like compressionRoundTripBehavior(GZIP, "helloworld\n")
    behave like integratedInsertBehavior(GZIP)
  }

}
