ThisBuild / resolvers ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

ThisBuild / organization := "com.kevin.flinkmqtt3"
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version      := "0.1.0-SNAPSHOT"

val flinkVersion = "1.14.4"

lazy val root = (project in file("."))
  .aggregate(server)

lazy val server = project
  .settings(
    Compile / compile := ((Compile / compile)).value,
    libraryDependencies ++= Seq(
      "ch.qos.logback"    % "logback-classic"           % "1.2.3",
      "org.apache.flink" %% "flink-clients" % flinkVersion,
      "org.apache.flink" %% "flink-scala" % flinkVersion,
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
      "io.github.kevin4936" % "kevin-flink-connector-mqtt3_2.12" % "1.14.4.1"
      ////"org.apache.flink" % "flink-clients_2.12" % "1.14.5"  % "provided",
      //"de.javakaffee" % "kryo-serializers" % "0.45"
    ),
    Assets / WebKeys.packagePrefix := "public/",
    Runtime / managedClasspath += (Assets / packageBin).value,
    Compile / run / mainClass  := Some("com.kevin.flinkmqtt3.WebServer"),
    assembly / assemblyJarName := "kevin-flinkmqtt3.jar",
    assembly / mainClass := Some("com.kevin.flinkmqtt3.WebServer")
  )
  .enablePlugins(SbtWeb, JavaAppPackaging)

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
                                   Compile / run / mainClass,
                                   Compile / run / runner
                                  ).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
//assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)
