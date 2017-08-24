import sbt._

val mvnrepository = "MVN Repo" at "http://mvnrepository.com/artifact"

lazy val root = (project in file(".")).
  settings(
      inThisBuild(List(
          organization := "selva",
          scalaVersion := "2.11.8",
          version := "1.0.0"
      )),
      name := "kinesisProducer",
      // https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk
      libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.155",
      // https://mvnrepository.com/artifact/com.amazonaws/amazon-kinesis-client
      libraryDependencies += "com.amazonaws" % "amazon-kinesis-client" % "1.7.6",
      // https://mvnrepository.com/artifact/com.amazonaws/amazon-kinesis-producer
      libraryDependencies += "com.amazonaws" % "amazon-kinesis-producer" % "0.12.5",

      libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.8.2",
      libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.8.2",

      libraryDependencies += "org.json4s" % "json4s-native_2.11" % "3.5.2",

      resolvers ++= {
          Seq(mvnrepository)
      }
  )
