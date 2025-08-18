package fs2.clickhouse.compression.zstd

import cats.effect.Async
import fs2.Pipe
import fs2.clickhouse.internal.Compression
import fs2.io.{readInputStream, toInputStream}
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream

import java.io.InputStream

// TODO: set better default block size
class ZSTDCompression private (chunkSize: Int = 1000) extends Compression {

  override def acceptEncoding: Option[String] = Some("zstd")

  override def decompress[F[_] : Async]: Pipe[F, Byte, Byte] =
    _
      .through(toInputStream[F])
      .flatMap { inputStream =>
        val decompressed: F[InputStream] =
          Async[F].delay({
            new ZstdCompressorInputStream(inputStream) 
          })
        readInputStream[F](decompressed, chunkSize)
      }

  override def compress[F[_] : Async]: Pipe[F, Byte, Byte] = ???
}

object ZSTDCompression {

  lazy val defaultInstance = new ZSTDCompression

  def apply(chunkSize: Int): Compression = new ZSTDCompression(chunkSize)

}
