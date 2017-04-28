// Copyright (C) 2017 Georg Heiler
package myOrg.visualization

import java.io.{File, FileInputStream}
import java.util.Properties

import com.vividsolutions.jts.geom.Envelope
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.datasyslab.babylon.extension.visualizationEffect.ScatterPlot
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.spatialRDD.{PolygonRDD, RectangleRDD}
import java.awt.Color

import org.datasyslab.babylon.extension.imageGenerator.BabylonImageGenerator
import org.datasyslab.babylon.utils.ImageType

object VisualizationGeosparkLocalRaster extends App {

  val prop = new Properties()
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
  val resourcePath = "src" + File.separator + "test" + File.separator + "resources" + File.separator
  val demoOutputPath = "target" + File.separator + "demo"

  var confFile = new FileInputStream(resourcePath + "babylon.point.properties")
  val scatterPlotOutputPath = System.getProperty("user.dir") + File.separator + demoOutputPath + File.separator + "scatterplot"

  prop.load(confFile)
  val heatMapOutputPath = System.getProperty("user.dir") + File.separator + demoOutputPath + File.separator + "heatmap"
  val choroplethMapOutputPath = System.getProperty("user.dir") + demoOutputPath + File.separator + "choroplethmap"
  val parallelFilterRenderStitchOutputPath = System.getProperty("user.dir") + File.separator + demoOutputPath + File.separator + "parallelfilterrenderstitchheatmap"
  val PointInputLocation = resourcePath + prop.getProperty("inputLocation")

  val PointOffset = prop.getProperty("offset").toInt
  val PointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
  val PointNumPartitions = prop.getProperty("numPartitions").toInt
  val RectangleInputLocation = resourcePath + prop.getProperty("inputLocation")

  confFile = new FileInputStream(resourcePath + "babylon.rectangle.properties")
  prop.load(confFile)
  val RectangleOffset = prop.getProperty("offset").toInt
  val RectangleSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
  val RectangleNumPartitions = prop.getProperty("numPartitions").toInt

  confFile = new FileInputStream(resourcePath + "babylon.polygon.properties")
  prop.load(confFile)
  val PolygonOffset = prop.getProperty("offset").toInt
  val PolygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
  val PolygonNumPartitions = prop.getProperty("numPartitions").toInt
  val PolygonInputLocation = resourcePath + prop.getProperty("inputLocation")

  confFile = new FileInputStream(resourcePath + "babylon.linestring.properties")
  prop.load(confFile)
  val LineStringOffset = prop.getProperty("offset").toInt
  val LineStringSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"))
  val LineStringNumPartitions = prop.getProperty("numPartitions").toInt
  val LineStringInputLocation = resourcePath + prop.getProperty("inputLocation")
  val USMainLandBoundary = new Envelope(-126.790180, -64.630926, 24.863836, 50.000)
  // ############### basic setup complete #############
  // plot in various variants

  val spatialRDD = new PolygonRDD(spark.sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions, StorageLevel.MEMORY_ONLY)
  Vis.buildScatterPlot(scatterPlotOutputPath, spatialRDD, USMainLandBoundary)

  // this throws a null pointer of
  /**
    *java.lang.IllegalArgumentException: image == null!
  at javax.imageio.ImageTypeSpecifier.createFromRenderedImage(ImageTypeSpecifier.java:925)
  at javax.imageio.ImageIO.getWriter(ImageIO.java:1592)
  at javax.imageio.ImageIO.write(ImageIO.java:1520)
  at org.datasyslab.babylon.extension.imageGenerator.BabylonImageGenerator.SaveRasterImageAsLocalFile(BabylonImageGenerator.java:35)
  at org.datasyslab.babylon.core.AbstractImageGenerator.SaveRasterImageAsLocalFile(AbstractImageGenerator.java:59)
  ... 42 elided

    */
  val vDistributedRaster = new ScatterPlot(1000, 600, USMainLandBoundary, false, 2, 2, true, false)
  vDistributedRaster.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
  vDistributedRaster.Visualize(spark.sparkContext, spatialRDD)
  val imageGenerator = new BabylonImageGenerator()
  imageGenerator.SaveRasterImageAsLocalFile(vDistributedRaster.distributedRasterImage, scatterPlotOutputPath + "distributedRaster", ImageType.PNG)

  //  TODO build these in all 4 variants as well
  val rectangleRDD = new RectangleRDD(spark.sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY)
  Vis.buildHeatMap(heatMapOutputPath, spatialRDD, USMainLandBoundary) // java.lang.ArrayIndexOutOfBoundsException: 2

  //  buildChoroplethMap(choroplethMapOutputPath)
  //  parallelFilterRenderStitch(parallelFilterRenderStitchOutputPath + "-stitched")
  //  parallelFilterRenderNoStitch(parallelFilterRenderStitchOutputPath)

  spark.stop

}
