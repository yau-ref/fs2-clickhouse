package fs2.clickhouse.exceptions

import java.net.ConnectException
import scala.util.control.NoStackTrace

// TODO: would be cool to have info about query here
class FS2ClickhouseException(message: String, cause: Option[Throwable])
    extends Exception(message, cause.orNull)

object FS2ClickhouseException {
  def apply(message: String, cause: Throwable) =
    new FS2ClickhouseException(message, Some(cause))

  def apply(message: String) =
    new FS2ClickhouseException(message, None)
}

case class FS2CHDecompressionException(cause: Throwable)
    extends FS2ClickhouseException(s"Decompression failed", Some(cause))
    with NoStackTrace

case class FS2CHConnectionException(cause: ConnectException)
    extends FS2ClickhouseException(
      "Failed to connect to Clickhouse HTTP Api",
      Some(cause)
    )
    with NoStackTrace

// TODO: need more info in here
case class FS2CHQueryFailed(statusCode: Int)
    extends FS2ClickhouseException(
      s"Query execution failed, status code = $statusCode",
      None
    )
    with NoStackTrace
