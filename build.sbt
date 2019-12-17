name := "FineGrainedDataProvenance"

version := "1.0"

scalaVersion := "2.11.8"


// build.sbt
libraryDependencies += "org.scalameta" %% "scalameta" % "4.2.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.3"

libraryDependencies += "org.roaringbitmap" % "RoaringBitmap" % "0.8.11"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % "2.11.8",
  "org.scala-lang" % "scala-reflect" % "2.11.8")