import sbt.Keys.{dependencyOverrides, libraryDependencies}
name := "final_project"
version := sys.props.get("version").getOrElse("local")

scalaVersion := "2.12.12"
organization := "com.project"

lazy val sparkVersion = "3.1.0"

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies ++= Seq(
  // Config
  "com.typesafe" % "config" % "1.3.4",
  "org.rogach" %% "scallop" % "3.1.2",
  "com.github.pureconfig" %% "pureconfig" % "0.13.0",
  // spark
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.slf4j" % "slf4j-api" % "1.7.9" ,
  "com.amazon.deequ" % "deequ" % "2.0.3-spark-3.3-local",
  "org.scala-lang" % "scala-reflect" % "2.12.12",
  "org.scala-lang" % "scala-compiler" % "2.12.12"

)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*)                                       => MergeStrategy.discard
  case m if m.toLowerCase.endsWith("manifest.mf")                          => MergeStrategy.discard
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case m if m.startsWith("META-INF")                                       => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*)                               => MergeStrategy.first
  case PathList("org", "apache", xs @ _*)                                  => MergeStrategy.first
  case PathList("org", "bouncycastle", xs @ _*)                            => MergeStrategy.discard
  case _                                                                   => MergeStrategy.first
}

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)
