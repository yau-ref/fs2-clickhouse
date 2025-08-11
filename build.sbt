import sbt.Keys.libraryDependencies

ThisBuild / version := "0.1.0"

val scala3 = "3.3.4"
val scala213 = "2.13.16"
val supportedScalaVersions = Seq(scala3, scala213)

ThisBuild / scalaVersion := scala3

val fs2Version = "3.12.0"
val catsEffectVersion = "3.6.3"

lazy val commonSettings = Seq(
  crossScalaVersions := supportedScalaVersions
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
    ),
    libraryDependencies ++= { // TODO: remove duplication
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, _)) =>
          List(compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1" cross CrossVersion.binary))
        case _ =>
          Nil
      }
    }

  ).enablePlugins()


lazy val circe = (project in file("circe"))
  .settings(commonSettings)
  .settings(
    name := "circe",
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % "0.14.14",
      "io.circe" %% "circe-generic" % "0.14.14",
      "io.circe" %% "circe-parser" % "0.14.14",
    ),
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, _)) =>
          List(compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1" cross CrossVersion.binary))
        case _ =>
          Nil
      }
    }

  ).dependsOn(core)

lazy val examples = (project in file("examples"))
  .settings(commonSettings)
  .settings(
    name := "examples",
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, _)) =>
          List(compilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1" cross CrossVersion.binary))
        case _ =>
          Nil
      }
    }

  ).dependsOn(core, circe)

