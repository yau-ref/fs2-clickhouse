package fs2.clickhouse.internal

import cats.effect.Sync
import cats.effect.std.Env
import cats.syntax.all.{toFlatMapOps, toFunctorOps}

import scala.util.control.NoStackTrace

sealed trait Auth
case object NoAuth extends Auth
case object FromEnv extends Auth
case class Credentials(user: String, password: Option[String]) extends Auth

object Auth {

  private val ClickhouseUserEnv = "CLICKHOUSE_USER"
  private val ClickhousePasswordEnv = "CLICKHOUSE_PASSWORD"

  def fromEnv[F[_]: Sync]: F[Credentials] =
    for {
      env <- Sync[F].pure(Env.make[F])
      maybeUser <- env.get(ClickhouseUserEnv)
      maybePassword <- env.get(ClickhousePasswordEnv)
      user <-
        Sync[F].fromOption(
          maybeUser,
          new IllegalArgumentException(
            s"Clickhouse user name is not in $ClickhouseUserEnv"
          ) with NoStackTrace
        )
    } yield Credentials(user, maybePassword)

}
