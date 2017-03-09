// Copyright (C) 2017 Georg Heiler

package myOrg

import java.awt.Color
import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.datasyslab.babylon.extension.imageGenerator.NativeJavaImageGenerator
import org.datasyslab.babylon.extension.visualizationEffect.ScatterPlot
import org.datasyslab.babylon.utils.ImageType
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.PolygonRDD

object GeoSpark extends App {

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
  val objectRDD = new PolygonRDD(spark.sparkContext, "src" + File.separator
    + "main" + File.separator
    + "resources" + File.separator
    + "zcta510-small.csv", FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY)

  val minimalPolygonCustom = new PolygonRDD(spark.sparkContext, "src"
    + File.separator + "main"
    + File.separator + "resources"
    + File.separator + "minimal01.csv", new CustomInputMapperWKT(), StorageLevel.MEMORY_ONLY)

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
  val joinResult = JoinQuery.SpatialJoinQuery(objectRDD, minimalPolygonCustom, true)
  /*
   * true means use spatial index.
   */
  println(s"count result size ${joinResult.count}")

  // TODO convert to dataframe
  //  joinResult.toDF().show
  //  joinResult.rawSpatialRDD.toDF().show
  //  joinResult.rawSpatialRDD.rdd.toDS().showhttps://github.com/DataSystemsLab/GeoSpark/tree/2adce0c1c13af172f9be6c3cd0cda1431c74d0b8/src/main/java/org/datasyslab/geospark/showcase

  //  TODO visualize result on a map via babylon
  val homeDir = System.getProperty("user.home");
  var path = homeDir + File.separator
  path = path.replaceFirst("^~", System.getProperty("user.home"))

  buildScatterPlot(path + "hereImage01", objectRDD)
  buildScatterPlot(path + "hereImage02", minimalPolygonCustom)
  // TODO how to visualize result?
  //  buildScatterPlot("image03.png", joinResult)

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

  spark.stop
}
