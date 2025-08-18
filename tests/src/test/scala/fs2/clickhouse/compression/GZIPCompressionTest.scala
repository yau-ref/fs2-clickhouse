package fs2.clickhouse.compression

import cats.effect.unsafe.implicits.global
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class GZIPCompressionTest
  extends AnyWordSpec
    with SimpleDecompressionTest
    with Matchers
    with Inspectors {

  "GZIPCompressions" should {
    "decompress data" in 
      simpleDecompressionTest(GZIP, "H4sIAAAAAAAAA8tIzcnJL88vyknhAgDmMkmaCwAAAA==", "helloworld\n")
  }

}

