package fs2.clickhouse.compression.zstd

import cats.effect.Async
import fs2.Pipe
import fs2.clickhouse.compression.Compression
import fs2.io.{
  readInputStream,
  readOutputStream,
  toInputStream,
  writeOutputStream
}
import org.apache.commons.compress.compressors.zstandard.{
  ZstdCompressorInputStream,
  ZstdCompressorOutputStream
}

import java.io.InputStream

// TODO: set better default block size
class ZSTDCompression private (chunkSize: Int = 1000) extends Compression {

  override def acceptEncoding: Option[String] = Some("zstd")

  // in case ZstdCompressorInputStream validates the frame header eagerly -
  // see Compression.skipIfEmpty
  override def decompress[F[_]: Async]: Pipe[F, Byte, Byte] =
    Compression.skipIfEmpty { stream =>
      stream.through(toInputStream[F]).flatMap { inputStream =>
        val decompressed: F[InputStream] =
          Async[F].delay(new ZstdCompressorInputStream(inputStream))
        readInputStream[F](decompressed, chunkSize)
      }
    }

  override def compress[F[_]: Async]: Pipe[F, Byte, Byte] =
    in =>
      readOutputStream[F](chunkSize) { os =>
        in.through(
          writeOutputStream[F](
            Async[F].delay(new ZstdCompressorOutputStream(os))
          )
        ).compile
          .drain
      }
}

object ZSTDCompression {

  lazy val defaultInstance = new ZSTDCompression

  def apply(chunkSize: Int): Compression = new ZSTDCompression(chunkSize)

}
