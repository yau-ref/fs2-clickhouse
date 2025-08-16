package fs2.clickhouse.internal

import cats.effect.Async
import fs2.Pipe
import fs2.io.{readInputStream, toInputStream}
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream

import java.io.InputStream

// TODO: set better default block size
class LZ4Compression(chunkSize: Int = 1000) extends Compression {

  override def acceptEncoding: Option[String] = Some("lz4")

  // Clickhouse uses lz4 frame format with dependent blocks
  // https://github.com/ClickHouse/ClickHouse/blob/master/src/IO/Lz4DeflatingWriteBuffer.cpp
  override def decompress[F[_] : Async]: Pipe[F, Byte, Byte] =
    _
      .through(toInputStream[F])
      .flatMap { inputStream =>
        val decompressed: F[InputStream] =
          Async[F].delay({
            new FramedLZ4CompressorInputStream(inputStream) // not thread safe
          })
        readInputStream[F](decompressed, chunkSize)
      }

  override def compress[F[_] : Async]: Pipe[F, Byte, Byte] = ???

}

object LZ4Compression {

  lazy val defaultInstance = new LZ4Compression

}
