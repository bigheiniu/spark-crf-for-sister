name := "Imllib"

organization := "com.intel"

version := "0.0.1"

scalaVersion := "2.11.8"
sparkVersion := "2.1.0"
//sparkComponents ++= Seq("sql","mllib")


libraryDependencies ++= Seq(
    "org.specs2" %% "specs2-core" % "3.8.8" % "test",
    "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided",
    "org.scalanlp" %% "breeze-natives" % "0.11.2" % "provided",
    "org.slf4j" % "slf4j-api" % "1.7.16",
    "org.slf4j" % "slf4j-log4j12" % "1.7.16",
    "databricks" % "spark-corenlp_2.11" % "0.3.0-SNAPSHOT" from "file:///Users/bigheiniu/.ivy2/local/databricks/spark-corenlp_2.11/0.3.0-SNAPSHOT/jars/spark-corenlp_2.11.jar"
)
///libraryDependencies += "databricks" % "spark-corenlp_2.10" % "0.3.0-SNAPSHOT"
//spDependencies += "databricks/spark-corenlp_2.11:0.3.0-SNAPSHOT"
resolvers += Resolver.sonatypeRepo("public")

scalacOptions in Test ++= Seq("-Yrangepos")

parallelExecution := false

//unmanagedBase := baseDirectory.value / "lib"

