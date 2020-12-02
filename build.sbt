name := "movies-test"

version := "0.0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided"
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.3" % "test"

// test suite settings
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

//mainClass in (Compile, run) := Some("com.test.movies.transforms")
