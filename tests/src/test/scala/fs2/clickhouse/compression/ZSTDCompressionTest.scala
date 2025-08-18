package fs2.clickhouse.compression

import cats.effect.unsafe.implicits.global
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ZSTDCompressionTest
  extends AnyWordSpec
    with SimpleDecompressionTest
    with Matchers
    with Inspectors {

  "ZSTDCompressions" should {
    "decompress data" in 
      simpleDecompressionTest(ZSTD, "KLUv/QRYWQAAaGVsbG93b3JsZAp/WzH0", "helloworld\n")
    
  }

}
