package fs2.clickhouse.compression

import cats.effect.unsafe.implicits.global
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LZ4CompressionTest
  extends AnyWordSpec
    with SimpleDecompressionTest
    with Matchers
    with Inspectors {

  "LZ4Compression" should {
    "decompress data" in 
      simpleDecompressionTest(LZ4, "BCJNGGRApwsAAIBoZWxsb3dvcmxkCgAAAADxpNsz", "helloworld\n")
  }

}
