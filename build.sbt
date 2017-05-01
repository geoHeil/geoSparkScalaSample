name := "geoSparkStarter"
organization := "myOrg"

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-feature",
  "-Xfuture",
  "-Xlint:missing-interpolator",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-dead-code",
  "-Ywarn-unused"
)

//The default SBT testing java options are too small to support running many of the tests
// due to the need to launch Spark in local mode.
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false
fork in run:= true
lazy val spark = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark % "provided",
  "org.apache.spark" %% "spark-sql" % spark % "provided",
  "org.apache.spark" %% "spark-hive" % spark % "provided")
  .map(_.excludeAll(
    ExclusionRule(organization = "org.scalacheck"),
    ExclusionRule(organization = "org.scalactic"),
    ExclusionRule(organization = "org.scalatest")
  ))

libraryDependencies ++= Seq(
  "org.datasyslab" % "geospark" % "0.6.2",
  "org.datasyslab" % "babylon" % "0.2.0",
//  "com.typesafe" % "config" % "1.3.1",
  "com.holdenkarau" % "spark-testing-base_2.11" % s"${spark}_0.6.0" % "test"
)

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

assemblyMergeStrategy in assembly := {
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

test in assembly := {}

initialCommands in console :=
  """
    |
    |import java.io.{File, FileInputStream}
    |import java.util.Properties
    |
    |import com.vividsolutions.jts.geom.Envelope
    |import org.apache.spark.SparkConf
    |import org.apache.spark.sql.SparkSession
    |import org.apache.spark.storage.StorageLevel
    |import org.datasyslab.babylon.extension.visualizationEffect.{HeatMap, ScatterPlot}
    |import org.datasyslab.geospark.enums.FileDataSplitter
    |import org.datasyslab.geospark.spatialRDD.{PolygonRDD, RectangleRDD}
    |import java.awt.Color
    |
    |import org.datasyslab.babylon.extension.imageGenerator.BabylonImageGenerator
    |import org.datasyslab.babylon.utils.ImageType
    |
    |val conf: SparkConf = new SparkConf()
    |    .setAppName("geoSparkMemory")
    |    .setMaster("local[*]")
    |    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    |
    |  val spark: SparkSession = SparkSession
    |    .builder()
    |    .config(conf)
    |    .getOrCreate()
  """.stripMargin

mainClass := Some("myOrg.ExampleSQL")
