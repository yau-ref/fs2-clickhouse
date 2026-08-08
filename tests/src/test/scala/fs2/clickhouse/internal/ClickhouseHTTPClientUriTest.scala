package fs2.clickhouse.internal

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.clickhouse.compression.NoCompression
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.ByteArrayOutputStream
import java.net.http.{HttpClient, HttpRequest}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.{CompletableFuture, Flow, TimeUnit}

class ClickhouseHTTPClientUriTest
  extends AnyWordSpec
    with Matchers {

  // never actually sends a request in this suite - prepareRequest/
  // prepareInsertRequest only build the HttpRequest, they don't call send
  private val client = new ClickhouseHTTPClient[IO](
    HttpClient.newHttpClient(),
    "localhost",
    8123,
    NoAuth,
    NoCompression
  )

  private def paramsOf(rawQuery: String): List[String] =
    rawQuery.split("&", -1).toList

  private def bodyText(request: HttpRequest): String = {
    val publisher = request.bodyPublisher().orElseThrow()
    val collected = new ByteArrayOutputStream()
    val done = new CompletableFuture[Array[Byte]]()

    publisher.subscribe(new Flow.Subscriber[ByteBuffer] {
      override def onSubscribe(subscription: Flow.Subscription): Unit =
        subscription.request(Long.MaxValue)

      override def onNext(item: ByteBuffer): Unit = {
        val bytes = new Array[Byte](item.remaining())
        item.get(bytes)
        collected.write(bytes)
      }

      override def onError(t: Throwable): Unit = done.completeExceptionally(t)

      override def onComplete(): Unit = done.complete(collected.toByteArray)
    })

    new String(done.get(5, TimeUnit.SECONDS), StandardCharsets.UTF_8)
  }

  "clickhouseUri" should {
    "default `enable_http_compression` to enabled" in {
      val uri = client.clickhouseUri()
      paramsOf(uri.getRawQuery) shouldBe List("enable_http_compression=1")
    }

    "honor an explicit `enableHttpCompression = false`" in {
      val uri = client.clickhouseUri(enableHttpCompression = false)
      paramsOf(uri.getRawQuery) shouldBe List("enable_http_compression=0")
    }
  }

  "prepareRequest" should {
    "send the query as the POST body, untouched, instead of a URL param" in {
      val reserved = "select 1&2 as x -- 100% not a=param"
      val request = client.prepareRequest(reserved, NoAuth, None).unsafeRunSync()

      request.method() shouldBe "POST"
      paramsOf(request.uri().getRawQuery) shouldBe List("enable_http_compression=1")
      bodyText(request) shouldBe reserved
    }
  }

  "prepareInsertRequest" should {
    "only ever carry the fixed, known-safe URL params" in {
      val builder = client.prepareInsertRequest(NoAuth).unsafeRunSync()
      val uri = builder.build().uri()

      paramsOf(uri.getRawQuery) shouldBe List("enable_http_compression=1")
    }
  }

  "insertBodyLines" should {
    "put the statement first, ahead of the encoded rows" in {
      val lines =
        client
          .insertBodyLines("insert into t format JSONEachRow", fs2.Stream("row1", "row2"))
          .compile
          .toList
          .unsafeRunSync()

      lines shouldBe List(
        Right("insert into t format JSONEachRow"),
        Right("row1"),
        Right("row2")
      )
    }
  }

  "compress" should {
    "join the statement and rows with newlines into a single body" in {
      val body =
        client
          .compress[String](
            client.insertBodyLines(
              "insert into t format JSONEachRow -- a&b=c",
              fs2.Stream("row1", "row2")
            )
          )
          .through(fs2.text.utf8.decode)
          .compile
          .string
          .unsafeRunSync()

      body shouldBe "insert into t format JSONEachRow -- a&b=c\nrow1\nrow2"
    }
  }

}
