package fs2.clickhouse.compression

import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import fs2.clickhouse.{TestContainerHelpers, WithConnection}
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CompressionTest
    extends AnyWordSpec
    with Matchers
    with Inspectors
    with TestContainerForAll
    with TestContainerHelpers
    with WithConnection {

  // check integration:
  // 0. compressions should use correct accept-types
  // 1. the client actually uses compression if provided
  // 2. ch returns and accepts compressed data
  // 3. http client correctly handles wrong compression types

}
