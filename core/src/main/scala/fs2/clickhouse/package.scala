package fs2

import fs2.Pipe
import fs2.clickhouse.internal.ClickhouseClient

package object clickhouse {

  // public api

  type ClickhouseStream[F[_]] = ClickhouseClient[F]
  val ClickhouseStream = internal.ClickhouseStream

  type ClickhouseSink[F[_], I] = Pipe[F, I, Nothing]

  type Auth = internal.Auth
  val Auth = internal.Auth

  type NoAuth = internal.NoAuth.type
  val NoAuth = internal.NoAuth

  type FromEnv = internal.FromEnv.type
  val FromEnv = internal.FromEnv

  type Credentials = internal.Credentials
  val Credentials = internal.Credentials

}
