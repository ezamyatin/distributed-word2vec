lazy val root = (project in file("."))
  .settings(
    name := "distributed-word2vec",
    organization := "com.github.ezamyatin",
    version := "1.0",
    scalaVersion := "2.12.8",
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.2",
  "org.apache.spark" %% "spark-core" % "3.2.2",
  "org.apache.spark" %% "spark-mllib" % "3.2.2",
  "it.unimi.dsi" % "fastutil" % "8.5.8",
  "com.github.fommil.netlib" % "all" % "1.1.2",
  "com.github.fommil.netlib" % "core" % "1.1.2",
  "com.github.fommil.netlib" % "netlib-native_system-linux-x86_64" % "1.1",
  "com.github.scopt" %% "scopt" % "3.5.0",
)

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

