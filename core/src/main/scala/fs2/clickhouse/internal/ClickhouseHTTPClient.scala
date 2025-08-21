package fs2.clickhouse.internal

import cats.effect.Async
import cats.syntax.all._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import fs2.Pipe
import fs2.clickhouse.compression.Compression
import fs2.clickhouse.internal.ClickhouseHTTPClient.{
  ClickhousePasswordHeader,
  ClickhouseUserHeader,
  FS2CHDecompressionException,
  FS2ClickhouseException
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
  auth: Auth,
  compression: Compression
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
      .through(compression.decompress)
      .handleErrorWith {
        case e: FS2ClickhouseException =>
          fs2.Stream.raiseError(e)
        case e =>
          // decompressors are expected to be polite
          // and wrap errors by FS2CHDecompressionException
          // but let's be realistic :)
          fs2.Stream.raiseError(new FS2CHDecompressionException(e))
      }
      // decoded chunks could start and end in the middle of a line,
      // dividing row into parts which should be merged before they reach decoding;
      // fs2.text.lines does it well
      .through(fs2.text.utf8.decode)
      .through(fs2.text.lines)

  private def sendRequest(request: HttpRequest): F[InputStream] =
    for {
      sent: HttpResponse[InputStream] <-
        Async[F].blocking(
          javaHttpClient.send(request, HttpResponse.BodyHandlers.ofInputStream())
        ).handleErrorWith( e =>
          // TODO: make it detailed
          Async[F].raiseError(
            new FS2ClickhouseException("Request to Clickhouse HTTP API failed", Some(e))
          )
        )

      status = sent.statusCode()

      bodyStream <-
        if (status != 200)
          Async[F].delay(sent.body().close()) *>
            Async[F].raiseError(new IllegalArgumentException(s"Response code was not 200: ${status}") with NoStackTrace)
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
      .map( builder =>
        compression
          .acceptEncoding
          .fold(builder)(builder.header("Accept-Encoding", _))
      )
    builderWithHeaders.map(_.build())
  }

  override def insert[T](statement: String): Pipe[F, T, Nothing] = ???

}


object ClickhouseHTTPClient {

  private val ClickhouseUserHeader = "X-ClickHouse-User"
  private val ClickhousePasswordHeader = "X-ClickHouse-Key"

  // TODO: would be cool to have info about query here
  // TODO: move it to better place
  class FS2ClickhouseException(
    message: String,
    cause: Option[Throwable] = None
  ) extends Exception(message, cause.orNull)

  class FS2CHDecompressionException(cause: Throwable)
    extends FS2ClickhouseException(s"Decompression failed: ${cause.getMessage}", Some(cause))
    with NoStackTrace

}