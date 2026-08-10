package fs2.clickhouse

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import fs2.clickhouse.compression.NoCompression
import fs2.clickhouse.exceptions.FS2CHQueryFailed
import fs2.clickhouse.internal.Credentials
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

// exercises ClickhouseHTTPClient's error handling against a real Clickhouse
// server, not hand-built string fixtures - complements the unit-level
// internal.ClickhouseHTTPClientErrorHandlingTest
class QueryErrorHandlingTest
  extends AnyWordSpec
    with Matchers
    with TestContainerForAll
    with TestContainerHelpers {

  "client" should {
    "fail a query against a nonexistent table with the real status code and error body" in withContainers {
      implicit clickHouseContainer =>
        val result =
          fs2.Stream
            .resource(
              ClickhouseStream
                .http[IO](
                  hostName,
                  httpApiPort,
                  Credentials(username, Some(password)),
                  compression = NoCompression
                )
            )
            .flatMap(_.query[String]("select * from does_not_exist_QueryErrorHandlingTest"))
            .compile
            .drain
            .attempt
            .unsafeRunSync()

        result match {
          case Left(e: FS2CHQueryFailed) =>
            e.statusCode should not be 200
            e.getMessage should include("does_not_exist_QueryErrorHandlingTest")
          case other =>
            fail(s"expected Left(FS2CHQueryFailed), got $other")
        }
    }

    // forces a response that commits to HTTP 200 and only fails partway through
    // the body - Clickhouse's documented "HTTP response codes caveats" scenario -
    // by making throwIf fire after a few rows have already been flushed
    // (max_block_size = 1 forces row-by-row blocks, and
    // output_format_parallel_formatting = 0 forces each block to be formatted
    // and flushed to the response as it's produced instead of being buffered
    // for parallel formatting - otherwise the not-yet-flushed rows can be
    // discarded when the query aborts, before the client ever sees them;
    // http_write_exception_in_output_format = 0, needed for the client to
    // recognize the mid-stream failure at all, is forced by the client
    // itself on every request - see clickhouseUri)
    "surface a real mid-stream failure without losing the rows already streamed before it" in withContainers {
      implicit clickHouseContainer =>
        val query =
          """select number, throwIf(number = 5, 'boom') as guard
            |from system.numbers
            |limit 10
            |settings max_block_size = 1, output_format_parallel_formatting = 0""".stripMargin

        val emitted = scala.collection.mutable.ArrayBuffer.empty[String]

        val result =
          fs2.Stream
            .resource(
              ClickhouseStream
                .http[IO](
                  hostName,
                  httpApiPort,
                  Credentials(username, Some(password)),
                  compression = NoCompression
                )
            )
            .flatMap(_.query[String](query))
            .evalTap(row => IO(emitted += row))
            .compile
            .drain
            .attempt
            .unsafeRunSync()

        emitted should not be empty
        result match {
          case Left(e: FS2CHQueryFailed) =>
            e.getMessage should include("boom")
          case other =>
            fail(s"expected Left(FS2CHQueryFailed), got $other")
        }
    }
  }

}
