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
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.OptionConverters._

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
      tag = exceptionTag(response)
      bodyByteStream = fs2.io
        .readInputStream[F](Async[F].delay(response.body()), chunkSize)
      bodyLineStream = decompress(bodyByteStream).filterNot(_.isBlank)
      decodedElement <-
        if (status != Http.Ok)
          readErrorAndDrain(status, bodyLineStream)
        else
          decodeRows(status, bodyLineStream, tag)
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

  private[internal] def joinLines(lines: fs2.Stream[F, String]): F[String] =
    lines.compile.toList.map(_.mkString("\n"))

  /** If it's not 200 let's read the rest of body and try to decode it
    */
  private def readErrorAndDrain(
    status: Int,
    bodyInputStream: fs2.Stream[F, String]
  ): fs2.Stream[F, Nothing] =
    fs2.Stream
      .eval(joinLines(bodyInputStream))
      .flatMap(fullErr =>
        fs2.Stream.raiseError(FS2CHQueryFailed(status, s"Booom! $fullErr"))
      )

  /** When http_write_exception_in_output_format=0 (the default), Clickhouse
    * wraps a mid-stream error in a self-delimiting block, regardless of output
    * format:
    * {{{
    * __exception__
    * <TAG>
    * <error message>
    * <message_length> <TAG>
    * __exception__
    * }}}
    * `<TAG>` is a random token, echoed up front in the
    * `X-ClickHouse-Exception-Tag` response header - so it's known before the
    * body is read, and can be used to confirm a `__exception__` line is really
    * the protocol marker rather than coincidental row content. See "HTTP
    * response codes caveats" at https://clickhouse.com/docs/interfaces/http
    */
  private[internal] val ExceptionMarker = "__exception__"
  private val ExceptionTagHeader = "X-ClickHouse-Exception-Tag"

  private[internal] def exceptionTag(
    response: HttpResponse[_]
  ): Option[String] =
    response
      .headers()
      .firstValue(ExceptionTagHeader)
      .toScala
      .filterNot(_.isBlank)

  private def parseExceptionBlock[O](
    status: Int,
    expectedTag: Option[String],
    afterMarker: fs2.Stream[F, String]
  ): fs2.Pull[F, O, Unit] =
    afterMarker.pull.uncons1.flatMap {
      case None =>
        fs2.Pull.raiseError[F](
          FS2CHQueryFailed(status, s"Booom! $ExceptionMarker")
        )
      case Some((tagLine, tail)) =>
        expectedTag match {
          case Some(tag) if tagLine.trim == tag =>
            collectExceptionMessage(status, tag, Vector.empty, tail)
          case _ =>
            fs2.Pull
              .eval(joinLines(fs2.Stream(ExceptionMarker, tagLine) ++ tail))
              .flatMap(fullErr =>
                fs2.Pull
                  .raiseError[F](FS2CHQueryFailed(status, s"Booom! $fullErr"))
              )
        }
    }

  /** Accumulates the lines between the tag line and the closing line:
    *
    * {{{
    * <valid data>
    *
    * __exception__
    * <TAG>
    * <error message>
    * <message_length> <TAG>
    * __exception__
    * }}}
    * the tag line (`<TAG>`) and the closing line (`<message_length> <TAG>`) are
    * delimiters and `<error message>` is an actual error
    */
  private def collectExceptionMessage[O](
    status: Int,
    tag: String,
    messageLines: Vector[String],
    remaining: fs2.Stream[F, String]
  ): fs2.Pull[F, O, Unit] =
    remaining.pull.uncons1.flatMap {
      case Some((line, _)) if line.trim.endsWith(s" $tag") =>
        // closing line
        fs2.Pull.raiseError[F](
          FS2CHQueryFailed(status, messageLines.mkString("\n"))
        )
      case Some((line, tail)) =>
        collectExceptionMessage(status, tag, messageLines :+ line, tail)
      case None =>
        fs2.Pull.raiseError[F](
          FS2CHQueryFailed(status, messageLines.mkString("\n"))
        )
    }

  /** Decodes each line of a 200-status body as a row of `T`, but stops at the
    * first line that doesn't look like a JSONEachRow object. Clickhouse can
    * commit to a 200 status and then fail mid-query, appending its exception
    * text instead of a JSON row since it can no longer switch the response
    * status. Rows decoded before that line have already been emitted; the
    * offending line and everything after it are collected and raised as
    * FS2CHQueryFailed, without being fed to the decoder.
    *
    * A `__exception__` line is recognized as Clickhouse's documented mid-stream
    * error marker (see `parseExceptionBlock`) and, when its shape checks out,
    * only the actual error message is surfaced. Anything else that doesn't look
    * like a JSON row - including a `__exception__` line whose shape doesn't
    * check out - is treated as opaque error text, same as older Clickhouse
    * versions that predate the marker.
    *
    * A `{`-prefixed line that fails to decode is left alone: that failure
    * propagates as-is (not relabeled as a server-side error).
    */
  private[internal] def decodeRows[T](
    status: Int,
    lines: fs2.Stream[F, String],
    exceptionTag: Option[String] = None
  )(implicit decoder: JsonRowDecoder[F, T]): fs2.Stream[F, T] = {

    def process(stream: fs2.Stream[F, String]): fs2.Pull[F, T, Unit] =
      stream.pull.uncons1.flatMap {
        case None =>
          fs2.Pull.done
        case Some((line, tail)) =>
          line.trim match {
            case trimmed if trimmed.startsWith("{") =>
              fs2.Pull.eval(decoder.decode(line).value).flatMap {
                case Right(value) => fs2.Pull.output1(value) >> process(tail)
                case Left(err)    => fs2.Pull.raiseError[F](err)
              }
            case ExceptionMarker =>
              parseExceptionBlock(status, exceptionTag, tail)
            case _ =>
              // it's not a valid data but also is not an exception marker,
              // so must be older exception format, going to drain it
              fs2.Pull
                .eval(joinLines(fs2.Stream.emit(line) ++ tail))
                .flatMap(fullErr =>
                  fs2.Pull
                    .raiseError[F](FS2CHQueryFailed(status, s"Booom! $fullErr"))
                )
          }
      }

    process(lines).stream
  }

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
  private def insertBatch[T](statement: String, batch: fs2.Stream[F, T])(
    implicit encoder: JsonRowEncoder[F, T]
  ): F[Unit] =
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
            tag = exceptionTag(response)
            bodyByteStream =
              fs2.io.readInputStream[F](
                Async[F].delay(response.body()),
                chunkSize
              )
            bodyLineStream = decompress(bodyByteStream).filterNot(_.isBlank)
            _ <-
              if (status != Http.Ok)
                readErrorAndDrain(status, bodyLineStream).compile.drain
              else
                drainOrFail(status, bodyLineStream, tag)
          } yield ()
        }
        .compile
        .lastOrError
    } yield result

  /** Insert responses are expected to have an empty body on success. Same
    * underlying failure mode as `decodeRows` (Clickhouse committing to 200 and
    * then failing mid-stream) but response body is supposed to be empty, so if
    * it's not - something went wrong
    */
  private[internal] def drainOrFail(
    status: Int,
    bodyLineStream: fs2.Stream[F, String],
    exceptionTag: Option[String] = None
  ): F[Unit] = {
    def process(stream: fs2.Stream[F, String]): fs2.Pull[F, Nothing, Unit] =
      stream.pull.uncons1.flatMap {
        case None =>
          fs2.Pull.done
        case Some((line, tail)) if line.trim == ExceptionMarker =>
          parseExceptionBlock(status, exceptionTag, tail)
        case Some((line, tail)) =>
          fs2.Pull.eval(
            joinLines(fs2.Stream.emit(line) ++ tail).flatMap(fullErr =>
              FS2CHQueryFailed(status, s"Booom! $fullErr").raiseError[F, Unit]
            )
          )
      }

    process(bodyLineStream).stream.compile.drain
  }

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
