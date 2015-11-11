name := "spark-util"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.3.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.3.1"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"




