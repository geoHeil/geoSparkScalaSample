// Copyright (C) 2017 Georg Heiler
package myOrg.visualization

import java.io.{ File, FileInputStream }
import java.util.Properties

import com.vividsolutions.jts.geom.Envelope
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.datasyslab.babylon.extension.visualizationEffect.{ HeatMap, ScatterPlot }
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.spatialRDD.{ PolygonRDD, RectangleRDD }
import java.awt.Color

import org.datasyslab.babylon.extension.imageGenerator.BabylonImageGenerator
import org.datasyslab.babylon.utils.ImageType

object VisualizationGeosparkLocalRaster extends App {

  val conf = new SparkConf()
    .setAppName("babylon1")
    .setMaster("local[*]")
    .set("spark.driver.memory", "12G")
    .set("spark.default.parallelism", "12")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.speculation", "true")
  //    .set("spark.network.timeout", "100000s")

  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  // ############### basic setup #############
  val prop = new Properties()
  val resourcePath = "src/test/resources/"
  val demoOutputPath = "target/demo"
  var ConfFile = new FileInputStream(resourcePath + "babylon.point.properties")
  prop.load(ConfFile)
  val scatterPlotOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/scatterplot"
  val heatMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/heatmap"
  val choroplethMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/choroplethmap"
  val parallelFilterRenderStitchOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/parallelfilterrenderstitchheatmap"
  val earthdataScatterPlotOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/earthdatascatterplot"
  val PointInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
  val PointOffset = prop.getProperty("offset").toInt
  val PointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
  val PointNumPartitions = prop.getProperty("numPartitions").toInt
  ConfFile = new FileInputStream(resourcePath + "babylon.rectangle.properties")
  prop.load(ConfFile)
  val RectangleInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
  val RectangleOffset = prop.getProperty("offset").toInt
  val RectangleSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
  val RectangleNumPartitions = prop.getProperty("numPartitions").toInt
  ConfFile = new FileInputStream(resourcePath + "babylon.polygon.properties")
  prop.load(ConfFile)
  val PolygonInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
  val PolygonOffset = prop.getProperty("offset").toInt
  val PolygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
  val PolygonNumPartitions = prop.getProperty("numPartitions").toInt
  ConfFile = new FileInputStream(resourcePath + "babylon.linestring.properties")
  prop.load(ConfFile)
  val LineStringInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation")
  val LineStringOffset = prop.getProperty("offset").toInt
  val LineStringSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
  val LineStringNumPartitions = prop.getProperty("numPartitions").toInt
  val USMainLandBoundary = new Envelope(-126.790180, -64.630926, 24.863836, 50.000)
  // ############### basic setup complete #############
  // plot in various variants

  //  val spatialRDD = new PolygonRDD(spark.sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions, StorageLevel.MEMORY_ONLY)
  //  Vis.buildScatterPlot(scatterPlotOutputPath, spatialRDD, USMainLandBoundary)

  //  TODO build these in all 4 variants as well
  //  val rectangleRDD = new RectangleRDD(spark.sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY)
  //  val rectangleRDD = new RectangleRDD(spark.sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY)
  //  Vis.buildHeatMap(heatMapOutputPath, rectangleRDD, USMainLandBoundary) // java.lang.ArrayIndexOutOfBoundsException: 2

  val spatialRDD = new RectangleRDD(spark.sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY)
  val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2)
  visualizationOperator.Visualize(spark.sparkContext, spatialRDD)
  val imageGenerator = new BabylonImageGenerator
  imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, parallelFilterRenderStitchOutputPath, ImageType.PNG)

  //  buildChoroplethMap(choroplethMapOutputPath)
  //  parallelFilterRenderStitch(parallelFilterRenderStitchOutputPath + "-stitched")
  //  parallelFilterRenderNoStitch(parallelFilterRenderStitchOutputPath)

  spark.stop

}
