ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "3.3.4"

lazy val root = (project in file("."))
  .settings(
    name := "fs2-clickhouse"
  )

val fs2Version = "3.11.0"

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % fs2Version,
  "co.fs2" %% "fs2-io"   % fs2Version,
  "io.circe" %% "circe-core" % "0.14.10"
)

