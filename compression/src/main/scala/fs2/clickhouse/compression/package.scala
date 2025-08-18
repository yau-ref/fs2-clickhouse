package fs2.clickhouse

// TODO: consider moving them to compression.extra
package object compression {

  def LZ4(x: Int): Compression = lz4.LZ4Compression(x)
  lazy val LZ4: Compression = lz4.LZ4Compression.defaultInstance

  def ZSTD(x: Int): Compression = zstd.ZSTDCompression(x)
  lazy val ZSTD: Compression = zstd.ZSTDCompression.defaultInstance

}
