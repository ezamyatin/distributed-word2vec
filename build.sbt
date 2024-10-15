lazy val root = (project in file("."))
  .settings(
    name := "logfac",
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
)
