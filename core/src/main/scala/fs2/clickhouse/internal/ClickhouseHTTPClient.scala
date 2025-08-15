package fs2.clickhouse.internal

import cats.MonadError
import cats.effect.Async
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import fs2.Pipe
import fs2.clickhouse.internal.ClickhouseHTTPClient.{
  ClickhousePasswordHeader,
  ClickhouseUserHeader
}

import java.io.InputStream
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace

/**
 * Implements Clickhouse HTTP API
 * https://clickhouse.com/docs/en/interfaces/http
 */
class ClickhouseHTTPClient[F[_]: Async] private[internal] (
  javaHttpClient: HttpClient,
  host: String,
  port: Int,
  auth: Auth
) extends ClickhouseClient[F] {

  private val requestReadChunkSize = 1

  override def query[T](q: String, timeout: Option[FiniteDuration] = None)(implicit decoder: JsonRowDecoder[F, T]): fs2.Stream[F, T] =
    for {
      request: HttpRequest <- fs2.Stream.eval(prepareRequest(q, auth, timeout))
      bodyByteStream = bodyStream(request)
      bodyLine <- decompress(bodyByteStream) if bodyLine.nonEmpty
      decoded <- fs2.Stream.eval(decoder.decode(bodyLine).value)
      result <- fs2.Stream.fromEither(decoded)
    } yield result

  private def bodyStream(request: HttpRequest, chunkSize: Int = 100): fs2.Stream[F, Byte] =
    fs2.io.readInputStream[F](sendRequest(request), chunkSize)

  private def decompress(stream: fs2.Stream[F, Byte]): fs2.Stream[F, String] =
    stream
      .through(fs2.io.compression.fs2ioCompressionForAsync[F].gunzip())
      .flatMap[F, String](gzip =>
        gzip.content.through(fs2.text.utf8.decode)
          // decoded chunks could start and end in the middle of a line,
          // dividing row into parts which should be merged before they reach decoding;
          // fs2.text.lines does it well
          .through(fs2.text.lines)
      )

  private def sendRequest(request: HttpRequest): F[InputStream] =
    for {
      sent: HttpResponse[InputStream] <-
        Async[F].blocking(
          javaHttpClient
            .send(request, HttpResponse.BodyHandlers.ofInputStream())
        )
      status = sent.statusCode()
      bodyStream <-
        if (status != 200)
          Async[F].delay(sent.body().close()) *>
            MonadError[F, Throwable]
              .raiseError(new IllegalArgumentException(s"Response code was not 200: ${status}") with NoStackTrace)
        else
          // TODO: response code 200 does not guarantee that a query was executed successfully
          // https://clickhouse.com/docs/interfaces/http#http_response_codes_caveats
          Async[F].delay(sent.body())
    } yield bodyStream

  private def timeoutToJavaTime(timeout: FiniteDuration) =
    java.time.Duration.ofNanos(timeout.toNanos)

  private def withTimeout(requestBuilder: HttpRequest.Builder, timeout: Option[FiniteDuration]) =
    timeout
      .map(timeoutToJavaTime)
      .fold(requestBuilder)(requestBuilder.timeout)

  private def withAuthHeaders(requestBuilder: HttpRequest.Builder, auth: Auth): F[HttpRequest.Builder] =
    auth match {
      case NoAuth =>
        Async[F].pure(requestBuilder)
      case Credentials(user, password) =>
        val headers =
          List(ClickhouseUserHeader, user) ++
            password.fold(List.empty[String])(pwd => List(ClickhousePasswordHeader, pwd))
        Async[F].pure(requestBuilder.headers(headers: _*))
      case FromEnv =>
        Auth
          .fromEnv
          .flatMap(withAuthHeaders(requestBuilder, _))
    }
  
  private def prepareRequest(
    q: String,
    auth: Auth,
    timeout: Option[FiniteDuration]
  ): F[HttpRequest] = {
    // TODO: there's non-documented way to pass params via POST
    // https://github.com/ClickHouse/ClickHouse/issues/8842
    val uri = new URI("http", "", host, port, "/", s"enable_http_compression=1&query=$q", "")
    val builder =
      HttpRequest
        .newBuilder()
        .GET()
        .uri(uri)
        .expectContinue(true)
    val builderWithTimeout = withTimeout(builder, timeout)
    val builderWithHeaders: F[HttpRequest.Builder] =
      withAuthHeaders(builderWithTimeout, auth)
      // JSONEachRowWithProgress allows getting progress data, would be cool
      // to take it and provide as a side-stream
      .map(_.header("X-ClickHouse-Format", "JSONEachRow"))
      .map(_.header("Accept-Encoding", "gzip")) // TODO: this should be configurable
    builderWithHeaders.map(_.build())
  }

  override def insert[T](statement: String): Pipe[F, T, Nothing] = ???

}


object ClickhouseHTTPClient {

  private val ClickhouseUserHeader = "X-ClickHouse-User"
  private val ClickhousePasswordHeader = "X-ClickHouse-Key"
  
}