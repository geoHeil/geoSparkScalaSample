// Copyright (C) 2017 Georg Heiler

package myOrg

import java.awt.Color
import java.io.File

import com.vividsolutions.jts.geom.Polygon
import com.vividsolutions.jts.io.{ WKTReader, WKTWriter }
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.storage.StorageLevel
import org.datasyslab.babylon.core.OverlayOperator
import org.datasyslab.babylon.extension.imageGenerator.NativeJavaImageGenerator
import org.datasyslab.babylon.extension.visualizationEffect.{ ChoroplethMap, HeatMap, ScatterPlot }
import org.datasyslab.babylon.utils.ImageType
import org.datasyslab.geospark.enums.{ FileDataSplitter, GridType, IndexType }
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{ PolygonRDD, SpatialRDD }
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

  /*
   * Use the partition boundary of objectRDD to repartition the query window RDD, This is mandatory.
   */
  val joinResult = JoinQuery.SpatialJoinQuery(objectRDD, minimalPolygonCustom, true, true)
  val joinResultCounted = JoinQuery.SpatialJoinQueryCountByKey(objectRDD, minimalPolygonCustom, true, true)

  /*
   * true means use spatial index.
   */
  println(s"count result size ${joinResult.count}")

  // demonstrate Ds - RDD - Ds back and fourth to integrate Hive and Spark and geoJoins
  val geoHiveDs = minimalPolygonCustom.rawSpatialRDD.rdd.mapPartitions(writeSerializableWKT).toDS
  val geoObjects = createSpatialRDDFromLinestringDataSet(geoHiveDs)
  val polygonRDDHive = new PolygonRDD(geoObjects, StorageLevel.MEMORY_ONLY)
  polygonRDDHive.rawSpatialRDD.rdd.mapPartitions(writeSerializableWKT).toDS.show

  // TODO show how to use this for pairRDD as well
  val fe = joinResult.rdd.first
  val feKey = fe._1

  // visualization
  val homeDir = System.getProperty("user.home")
  var path = homeDir + File.separator
  path = path.replaceFirst("^~", System.getProperty("user.home"))

  //  buildScatterPlot(path + "scatterObject", objectRDD)
  //  buildScatterPlot(path + "scatterPolygons", minimalPolygonCustom)

  //  TODO do not get an useful output for the other image types -> see visualization package
  // encoder http://stackoverflow.com/questions/36648128/how-to-store-custom-objects-in-a-dataset
  // decided to manually map to Product data types
  def writeSerializableWKT(iterator: Iterator[AnyRef]): Iterator[WKTGeometryWithPayload] = {
    val writer = new WKTWriter()
    iterator.flatMap(cur => {
      val cPoly = cur.asInstanceOf[Polygon]
      // TODO is it efficient to create this collection? Is this a proper iterator 2 iterator transformation?
      List(WKTGeometryWithPayload(writer.write(cPoly), cPoly.getUserData.asInstanceOf[Int])).iterator
    })
  }

  def createSpatialRDDFromLinestringDataSet(geoDataset: Dataset[WKTGeometryWithPayload]): RDD[Polygon] = {
    geoDataset.rdd.mapPartitions(iterator => {
      val reader = new WKTReader()
      iterator.flatMap(cur => {
        try {
          reader.read(cur.lineString) match {
            case p: Polygon => {
              val polygon = p.asInstanceOf[Polygon]
              polygon.setUserData(cur.payload)
              List(polygon).iterator
            }
            case _ => throw new NotImplementedError("Multipolygon or others not supported")
          }
        } catch {
          case e: ParseException =>
            logger.error("Could not parse")
            logger.error(e.getCause)
            logger.error(e.getMessage)
            None
        }
      })
    })
  }

  @transient lazy val imageGenerator = new NativeJavaImageGenerator()
  /**
   * Builds the scatter plot.
   *
   * @param outputPath the output path
   * @return true, if successful
   */
  // https://github.com/DataSystemsLab/GeoSpark/blob/master/src/main/java/org/datasyslab/babylon/showcase/Example.java
  def buildScatterPlot(outputPath: String, spatialRDD: SpatialRDD): Boolean = {
    val envelope = spatialRDD.boundaryEnvelope
    val s = spatialRDD.getRawSpatialRDD.rdd.sparkContext
    val visualizationOperator = new ScatterPlot(1000, 600, envelope, false, -1, -1, false, true)
    visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
    visualizationOperator.Visualize(s, spatialRDD)
    import org.datasyslab.babylon.utils.ImageType
    imageGenerator.SaveAsFile(visualizationOperator.vectorImage, outputPath, ImageType.SVG)
  }

  spark.stop
}
