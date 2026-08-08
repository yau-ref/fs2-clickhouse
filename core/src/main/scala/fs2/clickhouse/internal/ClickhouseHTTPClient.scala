package fs2.clickhouse.internal

import cats.data.EitherT
import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all._
import fs2.Pipe
import fs2.clickhouse.compression.Compression
import fs2.clickhouse.exceptions._
import fs2.clickhouse.internal.ClickhouseHTTPClient.{
  errorDecoder,
  ClickhousePasswordHeader,
  ClickhouseUserHeader,
  ErrorMessage,
  Http
}

import java.io.InputStream
import java.net.{ConnectException, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.{FiniteDuration, DurationInt}

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
      response: HttpResponse[InputStream] <- fs2.Stream.eval(
        sendRequest(request)
      )
      status = response.statusCode()
      bodyByteStream = fs2.io
        .readInputStream[F](Async[F].delay(response.body()), chunkSize)
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

  private[internal] def compress[T](
    stream: fs2.Stream[F, Either[Throwable, String]]
  )(implicit encoder: JsonRowEncoder[F, T]): fs2.Stream[F, Byte] =
    stream
      .flatMap(encoded => fs2.Stream.fromEither(encoded))
      .intersperse("\n") // TODO: double check this
      .through(fs2.text.utf8.encode)
      .through(compression.compress)

  private def bodyPublisher(
    stream: fs2.Stream[F, Byte]
  ): Resource[F, HttpRequest.BodyPublisher] =
    stream
      .through(fs2.io.toInputStream[F])
      .compile
      .resource
      .lastOrError
      .map(inputStream =>
        HttpRequest.BodyPublishers.ofInputStream(() => inputStream)
      )

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

  /** If it's not 200 let's read the rest of body and try to decode it
    */
  private def readErrorAndDrain(
    bodyInputStream: fs2.Stream[F, String]
  ): fs2.Stream[F, Nothing] =
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

  private[internal] def clickhouseUri(
    enableHttpCompression: Boolean = true
  ): URI =
    new URI(
      "http",
      "",
      host,
      port,
      "/",
      s"enable_http_compression=${if (enableHttpCompression) 1 else 0}",
      ""
    )

  private[internal] def prepareRequest(
    q: String,
    auth: Auth,
    timeout: Option[FiniteDuration]
  ): F[HttpRequest] = {
    val uri = clickhouseUri()
    val builder =
      HttpRequest
        .newBuilder()
        .POST(HttpRequest.BodyPublishers.ofString(q, StandardCharsets.UTF_8))
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

  private[internal] def prepareInsertRequest(
    auth: Auth
  ): F[HttpRequest.Builder] = {
    val uri = clickhouseUri()
    val builder =
      HttpRequest
        .newBuilder()
        .uri(uri)
        .expectContinue(true)
    withAuthHeaders(builder, auth)
      .map(builder =>
        compression.acceptEncoding
          .fold(builder)(builder.header("Content-Encoding", _))
      )
  }

  // compose the body stream from the statement and the values
  private[internal] def insertBodyLines[T](
    statement: String,
    stream: fs2.Stream[F, T]
  )(implicit encoder: JsonRowEncoder[F, T]) =
    fs2.Stream.emit(Right(statement)) ++
      stream.map(encoder.encode).evalMap(_.value)

  override def insert[T](
    statement: String,
    maxBatchSize: Int = 1000,
    maxBatchWait: FiniteDuration = 1.second
  )(implicit encoder: JsonRowEncoder[F, T]): Pipe[F, T, Nothing] =
    stream =>
      stream
        .groupWithin(maxBatchSize, maxBatchWait)
        .evalMap(batch => insertBatch(statement, fs2.Stream.chunk(batch)))
        .drain

  // sends a single batch as one HTTP request and drains/decodes its response;
  // used to send one request per batch produced by `groupWithin` in `insert`
  private def insertBatch[T](
    statement: String,
    batch: fs2.Stream[F, T]
  )(implicit encoder: JsonRowEncoder[F, T]): F[Unit] =
    for {
      requestBuilder <- prepareInsertRequest(auth)
      compressedStream = compress(insertBodyLines(statement, batch))
      result <- fs2.Stream
        .resource(bodyPublisher(compressedStream))
        .evalMap { publisher =>
          val request = requestBuilder.POST(publisher).build()
          for {
            response <- sendRequest(request)
            status = response.statusCode()
            bodyByteStream = fs2.io
              .readInputStream[F](Async[F].delay(response.body()), chunkSize)
            bodyLineStream = decompress(bodyByteStream).filterNot(_.isBlank)
            _ <-
              if (status != Http.Ok)
                readErrorAndDrain(bodyLineStream).compile.drain
              else
                bodyLineStream.compile.drain
          } yield ()
        }
        .compile
        .lastOrError
    } yield result

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
        EitherT.right(ErrorMessage(json).pure[F])
    }

  case class ErrorMessage(error: String)

}
