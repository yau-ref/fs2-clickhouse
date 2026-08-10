package fs2.clickhouse.internal

import cats.data.EitherT
import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import fs2.clickhouse.compression.NoCompression
import fs2.clickhouse.exceptions.FS2CHQueryFailed
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.net.http.HttpClient

class ClickhouseHTTPClientErrorHandlingTest
  extends AnyWordSpec
    with Matchers {

  // never actually sends a request in this suite - decodeRows/drainOrFail
  // only operate on already-materialized line streams
  private val client = new ClickhouseHTTPClient[IO](
    HttpClient.newHttpClient(),
    "localhost",
    8123,
    NoAuth,
    NoCompression
  )

  private def failingDecoder(err: Throwable): JsonRowDecoder[IO, String] =
    new JsonRowDecoder[IO, String] {
      override def decode(json: String): DecodedRow = EitherT.leftT[IO, String](err)
    }

  "decodeRows" should {
    "emit nothing and not fail for an empty body" in {
      implicit val decoder: JsonRowDecoder[IO, String] =
        JsonRowDecoder.stringDecoder[IO]

      client
        .decodeRows[String](200, fs2.Stream.empty)
        .compile
        .toList
        .unsafeRunSync() shouldBe Nil
    }

    "decode every line when they're all JSON objects" in {
      implicit val decoder: JsonRowDecoder[IO, String] =
        JsonRowDecoder.stringDecoder[IO]
      val lines = fs2.Stream("""{"a":1}""", """{"a":2}""")

      client
        .decodeRows[String](200, lines)
        .compile
        .toList
        .unsafeRunSync() shouldBe List("""{"a":1}""", """{"a":2}""")
    }

    "emit rows decoded before the failing line, then raise FS2CHQueryFailed with the joined remainder" in {
      implicit val decoder: JsonRowDecoder[IO, String] =
        JsonRowDecoder.stringDecoder[IO]
      val lines = fs2.Stream(
        """{"a":1}""",
        """{"a":2}""",
        "Code: 241. DB::Exception: boom",
        "extra context"
      )
      val emitted = Ref.unsafe[IO, List[String]](Nil)

      val result = client
        .decodeRows[String](200, lines)
        .evalTap(row => emitted.update(_ :+ row))
        .compile
        .drain
        .attempt
        .unsafeRunSync()

      emitted.get.unsafeRunSync() shouldBe List("""{"a":1}""", """{"a":2}""")
      result match {
        case Left(e: FS2CHQueryFailed) =>
          e.statusCode shouldBe 200
          e.getMessage should include("Code: 241. DB::Exception: boom")
          e.getMessage should include("extra context")
        case other =>
          fail(s"expected Left(FS2CHQueryFailed), got $other")
      }
    }

    "attach the original failure as cause when the connection fails after rows have already been emitted" in {
      implicit val decoder: JsonRowDecoder[IO, String] =
        JsonRowDecoder.stringDecoder[IO]
      val boom = new RuntimeException("connection reset")
      val lines = fs2.Stream("""{"a":1}""") ++ fs2.Stream.raiseError[IO](boom)
      val emitted = Ref.unsafe[IO, List[String]](Nil)

      val result = client
        .decodeRows[String](200, lines)
        .evalTap(row => emitted.update(_ :+ row))
        .compile
        .drain
        .attempt
        .unsafeRunSync()

      emitted.get.unsafeRunSync() shouldBe List("""{"a":1}""")
      result match {
        case Left(e: FS2CHQueryFailed) =>
          e.statusCode shouldBe 200
          e.getMessage should include("response stream failed after some rows had already been decoded")
          e.cause shouldBe Some(boom)
        case other =>
          fail(s"expected Left(FS2CHQueryFailed), got $other")
      }
    }

    "propagate a decoder failure on a JSON-object line unchanged, without relabeling it as a Clickhouse error" in {
      val boom = new RuntimeException("bad row shape")
      implicit val decoder: JsonRowDecoder[IO, String] = failingDecoder(boom)
      val lines = fs2.Stream("""{"a":1}""")

      val result =
        client.decodeRows[String](200, lines).compile.drain.attempt.unsafeRunSync()

      result shouldBe Left(boom)
    }

    "recognize a well-formed __exception__ block confirmed by the known exception tag, surfacing just the message" in {
      implicit val decoder: JsonRowDecoder[IO, String] =
        JsonRowDecoder.stringDecoder[IO]
      val lines = fs2.Stream(
        """{"a":1}""",
        "__exception__",
        "abc123tag",
        "Code: 395. DB::Exception: boom",
        "31 abc123tag",
        "__exception__"
      )
      val emitted = Ref.unsafe[IO, List[String]](Nil)

      val result = client
        .decodeRows[String](200, lines, exceptionTag = Some("abc123tag"))
        .evalTap(row => emitted.update(_ :+ row))
        .compile
        .drain
        .attempt
        .unsafeRunSync()

      emitted.get.unsafeRunSync() shouldBe List("""{"a":1}""")
      result shouldBe Left(
        FS2CHQueryFailed(200, "Code: 395. DB::Exception: boom")
      )
    }

    "fall back to raising the raw block as opaque error text when no exception tag header was captured" in {
      implicit val decoder: JsonRowDecoder[IO, String] =
        JsonRowDecoder.stringDecoder[IO]
      val lines = fs2.Stream(
        "__exception__",
        "abc123tag",
        "Code: 395. DB::Exception: boom",
        "31 abc123tag",
        "__exception__"
      )

      val result =
        client.decodeRows[String](200, lines).compile.drain.attempt.unsafeRunSync()

      result match {
        case Left(e: FS2CHQueryFailed) =>
          e.statusCode shouldBe 200
          e.getMessage should include("__exception__")
          e.getMessage should include("abc123tag")
          e.getMessage should include("Code: 395. DB::Exception: boom")
        case other =>
          fail(s"expected Left(FS2CHQueryFailed), got $other")
      }
    }

    "fall back to raising the raw block as opaque error text when the tag after __exception__ doesn't match the known exception tag" in {
      implicit val decoder: JsonRowDecoder[IO, String] =
        JsonRowDecoder.stringDecoder[IO]
      val lines = fs2.Stream(
        "__exception__",
        "abc123tag",
        "Code: 395. DB::Exception: boom",
        "31 abc123tag",
        "__exception__"
      )

      val result = client
        .decodeRows[String](200, lines, exceptionTag = Some("some-other-tag"))
        .compile
        .drain
        .attempt
        .unsafeRunSync()

      result match {
        case Left(e: FS2CHQueryFailed) =>
          e.statusCode shouldBe 200
          e.getMessage should include("__exception__")
          e.getMessage should include("abc123tag")
          e.getMessage should include("Code: 395. DB::Exception: boom")
        case other =>
          fail(s"expected Left(FS2CHQueryFailed), got $other")
      }
    }

    "fall back to raising the raw block as opaque error text when the line after __exception__ doesn't look like a tag" in {
      implicit val decoder: JsonRowDecoder[IO, String] =
        JsonRowDecoder.stringDecoder[IO]
      val lines = fs2.Stream(
        "__exception__",
        "this is not a tag",
        "more context"
      )

      val result =
        client.decodeRows[String](200, lines).compile.drain.attempt.unsafeRunSync()

      result match {
        case Left(e: FS2CHQueryFailed) =>
          e.statusCode shouldBe 200
          e.getMessage should include("__exception__")
          e.getMessage should include("this is not a tag")
          e.getMessage should include("more context")
        case other =>
          fail(s"expected Left(FS2CHQueryFailed), got $other")
      }
    }
  }

  "drainOrFail" should {
    "succeed for an empty body" in {
      client.drainOrFail(200, fs2.Stream.empty).unsafeRunSync() shouldBe (())
    }

    "raise FS2CHQueryFailed for any content in an otherwise-empty success body" in {
      val lines = fs2.Stream("Code: 241. DB::Exception: boom", "more context")

      val result = client.drainOrFail(200, lines).attempt.unsafeRunSync()

      result match {
        case Left(e: FS2CHQueryFailed) =>
          e.statusCode shouldBe 200
          e.getMessage should include("Code: 241. DB::Exception: boom")
          e.getMessage should include("more context")
        case other =>
          fail(s"expected Left(FS2CHQueryFailed), got $other")
      }
    }

    "attach the original failure as cause when the connection fails while draining error content" in {
      val boom = new RuntimeException("connection reset")
      val lines =
        fs2.Stream("Code: 241. DB::Exception: boom") ++ fs2.Stream.raiseError[IO](boom)

      val result = client.drainOrFail(200, lines).attempt.unsafeRunSync()

      result match {
        case Left(e: FS2CHQueryFailed) =>
          e.statusCode shouldBe 200
          e.getMessage should include("Code: 241. DB::Exception: boom")
          e.cause shouldBe Some(boom)
        case other =>
          fail(s"expected Left(FS2CHQueryFailed), got $other")
      }
    }

    "recognize a well-formed __exception__ block confirmed by the known exception tag, surfacing just the message" in {
      val lines = fs2.Stream(
        "__exception__",
        "abc123tag",
        "Code: 395. DB::Exception: boom",
        "31 abc123tag",
        "__exception__"
      )

      val result = client
        .drainOrFail(200, lines, exceptionTag = Some("abc123tag"))
        .attempt
        .unsafeRunSync()

      result shouldBe Left(
        FS2CHQueryFailed(200, "Code: 395. DB::Exception: boom")
      )
    }

    "fall back to raising the raw block as opaque error text when no exception tag header was captured" in {
      val lines = fs2.Stream(
        "__exception__",
        "abc123tag",
        "Code: 395. DB::Exception: boom",
        "31 abc123tag",
        "__exception__"
      )

      val result = client.drainOrFail(200, lines).attempt.unsafeRunSync()

      result match {
        case Left(e: FS2CHQueryFailed) =>
          e.statusCode shouldBe 200
          e.getMessage should include("__exception__")
          e.getMessage should include("abc123tag")
          e.getMessage should include("Code: 395. DB::Exception: boom")
        case other =>
          fail(s"expected Left(FS2CHQueryFailed), got $other")
      }
    }

    "fall back to raising the raw block as opaque error text when the tag after __exception__ doesn't match the known exception tag" in {
      val lines = fs2.Stream(
        "__exception__",
        "abc123tag",
        "Code: 395. DB::Exception: boom",
        "31 abc123tag",
        "__exception__"
      )

      val result = client
        .drainOrFail(200, lines, exceptionTag = Some("some-other-tag"))
        .attempt
        .unsafeRunSync()

      result match {
        case Left(e: FS2CHQueryFailed) =>
          e.statusCode shouldBe 200
          e.getMessage should include("__exception__")
          e.getMessage should include("abc123tag")
          e.getMessage should include("Code: 395. DB::Exception: boom")
        case other =>
          fail(s"expected Left(FS2CHQueryFailed), got $other")
      }
    }

    "fall back to raising the raw block as opaque error text when the line after __exception__ doesn't look like a tag" in {
      val lines = fs2.Stream(
        "__exception__",
        "this is not a tag",
        "more context"
      )

      val result = client.drainOrFail(200, lines).attempt.unsafeRunSync()

      result match {
        case Left(e: FS2CHQueryFailed) =>
          e.statusCode shouldBe 200
          e.getMessage should include("__exception__")
          e.getMessage should include("this is not a tag")
          e.getMessage should include("more context")
        case other =>
          fail(s"expected Left(FS2CHQueryFailed), got $other")
      }
    }
  }

}
