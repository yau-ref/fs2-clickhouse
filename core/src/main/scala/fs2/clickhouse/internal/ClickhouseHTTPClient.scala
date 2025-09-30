package fs2.clickhouse.internal

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import fs2.Pipe
import fs2.clickhouse.compression.Compression
import fs2.clickhouse.exceptions._
import fs2.clickhouse.internal.ClickhouseHTTPClient.{ClickhousePasswordHeader, ClickhouseUserHeader, ErrorMessage, Http, errorDecoder}

import java.io.InputStream
import java.net.{ConnectException, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import scala.concurrent.duration.FiniteDuration

/** Implements Clickhouse HTTP API
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
  private val chunkSize = 100

  override def query[T](q: String, timeout: Option[FiniteDuration] = None)(
    implicit decoder: JsonRowDecoder[F, T]
  ): fs2.Stream[F, T] =
    for {
      request: HttpRequest <- fs2.Stream.eval(prepareRequest(q, auth, timeout))
      response: HttpResponse[InputStream] <- fs2.Stream.eval(sendRequest(request))
      status = response.statusCode()
      bodyByteStream = fs2.io.readInputStream[F](Async[F].delay(response.body()), chunkSize)
      bodyLineStream = decompress(bodyByteStream).filterNot(_.isBlank)
      // TODO: if status != 200 then set decoder to be just err else set a combination
      decodedElement <-
        if (status != Http.Ok)
          readErrorAndDrain(bodyLineStream)
        else {
          // TODO: 200 != all good, need to handle errors here too
          bodyLineStream
            .map(decoder.decode)
            .evalMap(_.value)
            .flatMap(decoded => fs2.Stream.fromEither(decoded))
        }
    } yield decodedElement

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

  private def sendRequest(request: HttpRequest): F[HttpResponse[InputStream]] =
    Async[F]
      .blocking(
        javaHttpClient
          .send(request, HttpResponse.BodyHandlers.ofInputStream())
      )
      .handleErrorWith {
        case e: ConnectException =>
          FS2CHConnectionException(e).raiseError
        case e =>
          FS2ClickhouseException(
            "Request to Clickhouse HTTP API failed",
            e
          ).raiseError
      }

  /**
   *  If it's not 200 let's read the rest of body and try to decode it
   */
  private def readErrorAndDrain(bodyInputStream: fs2.Stream[F, String]): fs2.Stream[F, Nothing] =
    for {
      errors: List[String] <- fs2.Stream.eval(bodyInputStream.compile.toList)
      firstErr = errors.head
      // TODO: use error decoder
      //   errDec.decode(bodyLine)
      nope <- fs2.Stream.raiseError(FS2CHQueryFailed(0, s"Booom! $firstErr"))
    } yield nope

  // TODO: let's make a compositional decoder which first tries to decode product and if fails proceeds to
  //  decoding error (or visa versa); Also it should not just decoder errors but meta info too (and for now ignore it)
  private val errDec: JsonRowDecoder[F, ErrorMessage] = errorDecoder[F]

  private def timeoutToJavaTime(timeout: FiniteDuration) =
    java.time.Duration.ofNanos(timeout.toNanos)

  private def withTimeout(
    requestBuilder: HttpRequest.Builder,
    timeout: Option[FiniteDuration]
  ) =
    timeout
      .map(timeoutToJavaTime)
      .fold(requestBuilder)(requestBuilder.timeout)

  private def withAuthHeaders(
    requestBuilder: HttpRequest.Builder,
    auth: Auth
  ): F[HttpRequest.Builder] =
    auth match {
      case NoAuth =>
        Async[F].pure(requestBuilder)
      case Credentials(user, password) =>
        val headers =
          List(ClickhouseUserHeader, user) ++
            password.fold(List.empty[String])(pwd =>
              List(ClickhousePasswordHeader, pwd)
            )
        Async[F].pure(requestBuilder.headers(headers: _*))
      case FromEnv =>
        Auth.fromEnv
          .flatMap(withAuthHeaders(requestBuilder, _))
    }

  private def prepareRequest(
    q: String,
    auth: Auth,
    timeout: Option[FiniteDuration]
  ): F[HttpRequest] = {
    // TODO: there's non-documented way to pass params via POST
    // https://github.com/ClickHouse/ClickHouse/issues/8842
    val uri = new URI(
      "http",
      "",
      host,
      port,
      "/",
      s"enable_http_compression=1&query=$q",
      ""
    )
    val builder =
      HttpRequest
        .newBuilder()
        .GET()
        .uri(uri)
        .expectContinue(true)
    val builderWithTimeout = withTimeout(builder, timeout)
    val builderWithHeaders: F[HttpRequest.Builder] =
      withAuthHeaders(builderWithTimeout, auth)
        // TODO: JSONEachRowWithProgress allows getting progress data, would be cool
        //   to take it and provide as a side-stream
        .map(_.header("X-ClickHouse-Format", "JSONEachRow"))
        .map(builder =>
          compression.acceptEncoding
            .fold(builder)(builder.header("Accept-Encoding", _))
        )
    builderWithHeaders.map(_.build())
  }

  override def insert[T](statement: String): Pipe[F, T, Nothing] = ???

}

object ClickhouseHTTPClient {

  object Http {
    val Ok = 200
    val BadRequest = 400
  }

  private val ClickhouseUserHeader = "X-ClickHouse-User"
  private val ClickhousePasswordHeader = "X-ClickHouse-Key"

  def errorDecoder[F[_]: Async]: JsonRowDecoder[F, ErrorMessage] =
    new JsonRowDecoder[F, ErrorMessage] {
      override def decode(json: String): DecodedRow =
        EitherT.right(
          ErrorMessage(json).pure[F]
        )
    }

  case class ErrorMessage(error: String)

}
