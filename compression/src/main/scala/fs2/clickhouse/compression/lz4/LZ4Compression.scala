package fs2.clickhouse.compression.lz4

import cats.effect.Async
import fs2.Pipe
import fs2.clickhouse.compression.Compression
import fs2.io.{readInputStream, readOutputStream, toInputStream, writeOutputStream}
import org.apache.commons.compress.compressors.lz4.{
  FramedLZ4CompressorInputStream,
  FramedLZ4CompressorOutputStream
}

import java.io.InputStream

// TODO: set better default block size
class LZ4Compression private (chunkSize: Int = 1000) extends Compression {

  override def acceptEncoding: Option[String] = Some("lz4")

  // Clickhouse uses lz4 frame format with dependent blocks
  // https://github.com/ClickHouse/ClickHouse/blob/master/src/IO/Lz4DeflatingWriteBuffer.cpp
  //
  // FramedLZ4CompressorInputStream reads/validates the frame header eagerly
  // in its constructor, throwing on empty input - but an empty body is a
  // valid, expected response (e.g. a successful insert), so an empty stream
  // has to be short-circuited before reaching it.
  override def decompress[F[_]: Async]: Pipe[F, Byte, Byte] =
    _.pull.peek1.flatMap {
      case None => fs2.Pull.done
      case Some((_, stream)) =>
        stream
          .through(toInputStream[F])
          .flatMap { inputStream =>
            val decompressed: F[InputStream] =
              Async[F].delay(
                new FramedLZ4CompressorInputStream(inputStream) // not thread safe
              )
            readInputStream[F](decompressed, chunkSize)
          }
          .pull
          .echo
    }.stream

  override def compress[F[_]: Async]: Pipe[F, Byte, Byte] =
    in =>
      readOutputStream[F](chunkSize) { os =>
        in.through(
          writeOutputStream[F](
            Async[F].delay(new FramedLZ4CompressorOutputStream(os))
          )
        ).compile.drain
      }

}

object LZ4Compression {

  lazy val defaultInstance = new LZ4Compression

  def apply(chunkSize: Int): Compression = new LZ4Compression(chunkSize)

}
