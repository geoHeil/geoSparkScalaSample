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

lazy val spark = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark % "provided",
  "org.apache.spark" %% "spark-sql" % spark % "provided",
  "org.apache.spark" %% "spark-hive" % spark % "provided",
  "org.datasyslab" % "geospark" % "0.5.2"
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
    |import java.awt.Color
    |import java.io.File
    |
    |import com.vividsolutions.jts.geom.Envelope
    |import org.apache.spark.SparkConf
    |import org.apache.spark.sql.SparkSession
    |import org.apache.spark.storage.StorageLevel
    |import org.datasyslab.babylon.extension.imageGenerator.NativeJavaImageGenerator
    |import org.datasyslab.babylon.extension.visualizationEffect.ScatterPlot
    |import org.datasyslab.babylon.utils.ImageType
    |import org.datasyslab.geospark.enums.{ FileDataSplitter, GridType, IndexType }
    |import org.datasyslab.geospark.spatialOperator.JoinQuery
    |import org.datasyslab.geospark.spatialRDD.PolygonRDD
    |
    |val conf: SparkConf = new SparkConf()
    |    .setAppName("geoSparkMemory")
    |    .setMaster("local[*]")
    |    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    |
    |  val spark: SparkSession = SparkSession
    |    .builder()
    |    .config(conf)
    |    //      .enableHiveSupport()
    |    .getOrCreate()
  """.stripMargin

mainClass := Some("myOrg.ExampleSQL")
