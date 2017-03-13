// Copyright (C) 2017 Georg Heiler

package myOrg

import java.awt.Color
import java.io.File

import com.vividsolutions.jts.geom.Polygon
import com.vividsolutions.jts.io.{WKTReader, WKTWriter}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.babylon.core.OverlayOperator
import org.datasyslab.babylon.extension.imageGenerator.NativeJavaImageGenerator
import org.datasyslab.babylon.extension.visualizationEffect.{ChoroplethMap, HeatMap, ScatterPlot}
import org.datasyslab.babylon.utils.ImageType
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import org.json4s.ParserUtil.ParseException

case class WKTGeometryWithPayload(lineString: String, payload: Int)

object GeoSpark extends App {

  @transient lazy val logger: Logger = Logger.getLogger(this.getClass)
  val conf: SparkConf = new SparkConf()
    .setAppName("geoSparkMemory")
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    //      .enableHiveSupport()
    .getOrCreate()

  //  loading of points RDD
  //  val objectRDD = new PointRDD(spark.sparkContext, "src" + File.separator
  //    + "main" + File.separator
  //    + "resources" + File.separator
  //    + "arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY)
  //  TODO load data from different files int oa single big RDD
  //  It takes a raw JavaRDD<Polygon> and convert it to PolygonRDD
  //  so creating a raw RDD with uion of some files should work
  val objectRDD = new PolygonRDD(spark.sparkContext, "src" + File.separator
    + "main" + File.separator
    + "resources" + File.separator
    + "zcta510-small.csv", FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY)

  val minimalPolygonCustom = new PolygonRDD(spark.sparkContext, "src"
    + File.separator + "main"
    + File.separator + "resources"
    + File.separator + "minimal01.csv", new CustomInputMapperWKT(), StorageLevel.MEMORY_ONLY)
  /*
   * Use the partition boundary of objectRDD to repartition the query window RDD, This is mandatory.
   */
  val joinResult = JoinQuery.SpatialJoinQuery(objectRDD, minimalPolygonCustom, true)
  //  joinResult.map()
  val joinResultCounted = JoinQuery.SpatialJoinQueryCountByKey(objectRDD, minimalPolygonCustom, true)

  //preservesPartitioning = true // TODO check if this is actually true, but as we do not tamper with keys should work
  // TODO AnyRef is suboptimal. Is there any way to actually know that Geometry was in there beforehand?
  // http://stackoverflow.com/questions/21185092/apache-spark-map-vs-mappartitions
  import spark.implicits._

  minimalPolygonCustom.rawSpatialRDD.rdd.mapPartitions(writeSerializableWKT).toDS.show

  objectRDD.spatialPartitioning(GridType.RTREE)
  //   TODO try out different types of indices, try out different sides (left, right) of the join and try broadcast join
  /*
   * GridType.RTREE means use R-Tree spatial partitioning technique. It will take the leaf node boundaries as parition boundary.
   * We support R-Tree partitioning and Voronoi diagram partitioning.
   */
  // TODO try out different combinations of indices
  objectRDD.buildIndex(IndexType.RTREE, true)
  /*
   * IndexType.RTREE enum means the index type is R-tree. We support R-Tree index and Quad-Tree index. But Quad-Tree doesn't support KNN.
   * True means build index on the spatial partitioned RDD. ONLY set true when doing Spatial Join Query.
   */
  minimalPolygonCustom.spatialPartitioning(objectRDD.grids)
  val fe = joinResult.rdd.first
  val feKey = fe._1
  /*
   * true means use spatial index.
   */
  println(s"count result size ${joinResult.count}")

  // TODO figure out encoder http://stackoverflow.com/questions/36648128/how-to-store-custom-objects-in-a-dataset
  // or pass plain values i.e int string and no objects?
  // or simply use RDD API but spark SQL would be nice. Especially if result is exported to other services like hive
  //  import spark.implicits._
  import scala.reflect.ClassTag

  implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  val feValue = fe._2
  //  println(joinResult.rdd.first)
  //  implicit val polygonEncoder = org.apache.spark.sql.Encoders.kryo[com.vividsolutions.jts.geom.Polygon]
  //  val df = joinResult.rdd.toDF // only works with spark.implicits, which cant be imported to make kryo work
  val df = spark.createDataset(joinResult)
  val dfCount = spark.createDataset(joinResultCounted)
  println(fe)
  println(feKey.getExteriorRing)
  println(feKey.getUserData)
  println(feValue)
  val homeDir = System.getProperty("user.home");
  var path = homeDir + File.separator
  df.show // Fails with no encoder found
  df.printSchema
  dfCount.show
  dfCount.printSchema
  //  joinResult.rdd.toDS.show // Fails with no encoder found

  def writeSerializableWKT(iterator: Iterator[AnyRef]): Iterator[WKTGeometryWithPayload] = {
    val writer = new WKTWriter()
    iterator.flatMap(cur => {
      val cPoly = cur.asInstanceOf[Polygon]
      // TODO is it efficient to create this collection? Is this a proper iterator 2 iterator transformation?
      List(WKTGeometryWithPayload(writer.write(cPoly), cPoly.getUserData.asInstanceOf[Int])).iterator
    })
  }

  // TODO fix this method to reverse create RDD
//  def createSpatialRDDFromLinestringDataSet(geoDataset: Dataset[WKTGeometryWithPayload]): PolygonRDD = {
//    geoDataset.rdd.mapPartitions(iterator => {
//      val reader = new WKTReader()
//      iterator.flatMap(cur => {
//        // TODO get rid of additional objects e.g. seq
//        val geometry = Seq(cur.lineString).flatMap(s =>
//          try {
//            Some(reader.read(s))
//          }
//          catch {
//            case e: ParseException =>
//              logger.error("Could not parse")
//              logger.error(e.getCause)
//              logger.error(e.getMessage)
//              None
//          })
//        geometry match {
//          case p: Polygon => {
//            p.setUserData(cur.payload)
//            List(p).iterator
//          }
//          case _ => throw new NotImplementedError("Multipolygon or others not supported")
//        }
//      })
//    })
//  }

  path = path.replaceFirst("^~", System.getProperty("user.home"))

  buildScatterPlot(path + "scatterObject", objectRDD)
  buildScatterPlot(path + "scatterPolygons", minimalPolygonCustom)

  //  TODO do not get an useful output / heatmap
  //  buildHeatMap(path + "heatObject", objectRDD)
  //  buildHeatMap(path + "heatPolygons", minimalPolygonCustom)

  // TODO figure out why division by zero
  //  parallelFilterRenderStitch(path + "parallelObject", objectRDD)
  //  parallelFilterRenderStitch(path + "parallelPolygons", minimalPolygonCustom)

  // TODO figure out why division by zero
  //  buildChoroplethMap(path + "joinVisualization", joinResultCounted, objectRDD)

  /**
    * Builds the scatter plot.
    *
    * @param outputPath the output path
    * @return true, if successful
    */
  // https://github.com/DataSystemsLab/GeoSpark/blob/master/src/main/java/org/datasyslab/babylon/showcase/Example.java
  def buildScatterPlot(outputPath: String, spatialRDD: PolygonRDD): Unit = {
    val envelope = spatialRDD.boundaryEnvelope
    try {
      val visualizationOperator = new ScatterPlot(1000, 600, envelope, false)
      visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
      visualizationOperator.Visualize(spark.sparkContext, spatialRDD)
      val imageGenerator = new NativeJavaImageGenerator()
      imageGenerator.SaveAsFile(visualizationOperator.pixelImage, outputPath, ImageType.PNG)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * Builds the heat map.
    *
    * @param outputPath the output path
    * @return true, if successful
    */
  def buildHeatMap(outputPath: String, spatialRDD: PolygonRDD): Boolean = {
    try {
      val visualizationOperator = new HeatMap(1000, 600, spatialRDD.boundaryEnvelope, false, 2)
      visualizationOperator.Visualize(spark.sparkContext, spatialRDD)
      val imageGenerator = new NativeJavaImageGenerator()
      imageGenerator.SaveAsFile(visualizationOperator.pixelImage, outputPath, ImageType.GIF)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    true
  }

  /**
    * Builds the choropleth map.
    *
    * @param outputPath the output path
    * @return true, if successful
    */
  def buildChoroplethMap(outputPath: String, joinResult: JavaPairRDD[Polygon, java.lang.Long], objectRDD: PolygonRDD): Boolean = {
    try {
      val visualizationOperator = new ChoroplethMap(1000, 600, objectRDD.boundaryEnvelope, false)
      visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true)
      visualizationOperator.Visualize(spark.sparkContext, joinResult)

      val frontImage = new ScatterPlot(1000, 600, objectRDD.boundaryEnvelope, false)
      frontImage.CustomizeColor(0, 0, 0, 255, Color.GREEN, true)
      frontImage.Visualize(spark.sparkContext, objectRDD) // TODO check if left vs. right boject vs query is not mixed up

      val overlayOperator = new OverlayOperator(visualizationOperator.pixelImage)
      overlayOperator.JoinImage(frontImage.pixelImage)

      val imageGenerator = new NativeJavaImageGenerator()
      imageGenerator.SaveAsFile(overlayOperator.backImage, outputPath, ImageType.PNG)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    true
  }

  /**
    * Parallel filter render stitch.
    *
    * @param outputPath the output path
    * @return true
    *         , if successful
    */
  def parallelFilterRenderStitch(outputPath: String, spatialRDD: PolygonRDD): Boolean = {
    try {
      val visualizationOperator = new HeatMap(1000, 600, spatialRDD.boundaryEnvelope, false, 2, 4, 4, true, true)
      visualizationOperator.Visualize(spark.sparkContext, spatialRDD)
      visualizationOperator.stitchImagePartitions()
      val imageGenerator = new NativeJavaImageGenerator()
      imageGenerator.SaveAsFile(visualizationOperator.pixelImage, outputPath, ImageType.GIF)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    true
  }

  spark.stop
}
