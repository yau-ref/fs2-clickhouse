package fs2.clickhouse.internal

import cats.effect.Async
import fs2.Pipe

import java.io.InputStream
import net.jpountz.lz4.LZ4BlockInputStream
import fs2.io.{toInputStream, readInputStream}

// TODO: set better default block size
class LZ4Compression(chunkSize: Int = 1000) extends Compression {  

  override def acceptEncoding: Option[String] = Some("lz4")

  override def decompress[F[_] : Async]: Pipe[F, Byte, Byte] = 
    _
      .through(toInputStream[F])
      .flatMap { inputStream => 
        val lz4InputStream: F[InputStream] = 
          Async[F].delay(
            new LZ4BlockInputStream(inputStream) // not thread safe!
          ) 
        readInputStream[F](lz4InputStream, chunkSize)
      }

  override def compress[F[_] : Async]: Pipe[F, Byte, Byte] = ???

}
