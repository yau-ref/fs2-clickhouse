import sbt.Keys.libraryDependencies

ThisBuild / version := "0.1.0"

val scala3 = "3.3.4"
val scala213 = "2.13.16"
val supportedScalaVersions = Seq(scala3, scala213)

ThisBuild / scalaVersion := scala3

val fs2Version = "3.12.0"
val catsEffectVersion = "3.6.3"

lazy val commonSettings = Seq(
  crossScalaVersions := supportedScalaVersions,
  libraryDependencies ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, _)) => Seq(compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"))
      case _            => Nil
    }
  }
)

lazy val root = (project in file("."))
  .aggregate(core, circe, examples)
  .settings(
    name := "fs2-clickhouse",
    crossScalaVersions := Nil,
    publish / skip := true
  )

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "core",
    libraryDependencies ++= Seq(
      "co.fs2" %% "fs2-core" % fs2Version,
      "co.fs2" %% "fs2-io"   % fs2Version,
      "org.typelevel" %% "cats-effect" % catsEffectVersion
    )
  )

lazy val circe = (project in file("circe"))
  .settings(commonSettings)
  .settings(
    name := "circe",
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % "0.14.14",
      "io.circe" %% "circe-generic" % "0.14.14",
      "io.circe" %% "circe-parser" % "0.14.14",
    )
  ).dependsOn(core)

lazy val examples = (project in file("examples"))
  .settings(commonSettings)
  .settings(
    name := "examples"
  ).dependsOn(core, circe)

lazy val tests = (project in file("tests"))
  .settings(commonSettings)
  .settings(
    name := "tests",
    libraryDependencies ++= Seq(
      "org.scalactic" %% "scalactic" % "3.2.19",
      "org.scalatest" %% "scalatest" % "3.2.19" % "it, test",
      "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.43.0" % "it, test",
      "com.dimafeng" %% "testcontainers-scala-clickhouse" % "0.43.0" % "it, test",
      "com.clickhouse" % "clickhouse-jdbc" % "0.9.1" % "it, test"
    ),

  ).dependsOn(core, circe)

