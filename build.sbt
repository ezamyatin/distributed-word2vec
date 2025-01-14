lazy val root = (project in file("."))
  .settings(
    name := "logfac-yt",
    organization := "ru.vk.algo",
    version := "1.1",
    scalaVersion := "2.12.15",
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.3",
  "org.apache.spark" %% "spark-core" % "3.3.3",
  "org.apache.spark" %% "spark-mllib" % "3.3.3",
  "it.unimi.dsi" % "fastutil" % "8.5.8",
  "com.github.fommil.netlib" % "all" % "1.1.2",
  "com.github.fommil.netlib" % "core" % "1.1.2",
  "com.github.fommil.netlib" % "netlib-native_system-linux-x86_64" % "1.1",
  "tech.ytsaurus" % "spark-yt-data-source-base_2.12" % "2.3.0",
)
