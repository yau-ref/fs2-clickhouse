package fs2.clickhouse.compression

import cats.effect.testing.scalatest.AsyncIOSpec
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import fs2.clickhouse.{TestContainerHelpers, WithConnection}
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class LZ4CompressionTest
  extends AsyncWordSpec
    with AsyncIOSpec
    with Matchers
    with Inspectors
    with TestContainerForAll
    with TestContainerHelpers
    with WithConnection
    with CompressionBehaviors {

  "LZ4Compression" should {
    behave like decompressionBehavior(LZ4, "helloworld\n", "BCJNGGRApwsAAIBoZWxsb3dvcmxkCgAAAADxpNsz")
    behave like integratedDecompressionBehavior(LZ4)
  }

}
