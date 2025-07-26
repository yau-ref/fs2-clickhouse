package fs2.clickhouse.internal

import cats.MonadError
import cats.effect.Async
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import fs2.Pipe
import fs2.clickhouse.internal.ClickhouseHTTPClient.{ClickhousePasswordHeader, ClickhouseUserHeader}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.{Duration => JDuration}
import java.util.stream
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
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

  override def query[T](q: String, timeout: Option[FiniteDuration] = None)(implicit decoder: JsonRowDecoder[F, T]): fs2.Stream[F, String] =
    for {
      request <- fs2.Stream.eval(prepareRequest(q, auth, timeout))
      responseStream: stream.Stream[String] <-
        fs2.Stream
          .bracket(
            sendRequest(request)
          )(stream => Async[F].delay(stream.close()))
      itt = responseStream.iterator().asScala
      responseLine <- fs2.Stream.fromBlockingIterator[F](itt, requestReadChunkSize)
    } yield responseLine

  private def sendRequest(request: HttpRequest): F[stream.Stream[String]] =
    for {
      sent: HttpResponse[stream.Stream[String]] <-
        Async[F].blocking(
          javaHttpClient
            .send[stream.Stream[String]](request, HttpResponse.BodyHandlers.ofLines())
        )
      status = sent.statusCode()
      bodyStream: stream.Stream[String] <-
        //  TODO: HTTP 200 response code does not guarantee that a query was successful
        if (status != 200)
          Async[F].delay(sent.body().close()) *> //
            MonadError[F, Throwable]
              .raiseError(new IllegalArgumentException(s"Response code was not 200: ${status}") with NoStackTrace)
        else
          Async[F].delay(sent.body())
    } yield bodyStream


  private def timeoutToJavaTime(timeout: FiniteDuration): JDuration =
    JDuration.ofNanos(timeout.toNanos)

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

    val uri = new URI("http", "", host, port, "/", s"query=$q", "")
    val builder =
      HttpRequest
        .newBuilder()
        .GET()
        .uri(uri)
        .expectContinue(true)
    val builderWithTimeout = withTimeout(builder, timeout)
    val builderWithHeaders: F[HttpRequest.Builder] = withAuthHeaders(builderWithTimeout, auth)
    builderWithHeaders.map(_.build())
  }

  override def insert[T](statement: String): Pipe[F, T, Nothing] = ???

}


object ClickhouseHTTPClient {

  private val ClickhouseUserHeader = "X-ClickHouse-User"
  private val ClickhousePasswordHeader = "X-ClickHouse-Key"
  
}