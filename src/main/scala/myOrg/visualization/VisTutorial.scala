// Copyright (C) 2017 Georg Heiler
package myOrg.visualization

import java.awt.Color
import java.io.FileInputStream
import java.util.Properties

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.datasyslab.babylon.core.RasterOverlayOperator
import org.datasyslab.babylon.extension.imageGenerator.BabylonImageGenerator
import org.datasyslab.babylon.extension.visualizationEffect.ChoroplethMap
import org.datasyslab.babylon.extension.visualizationEffect.HeatMap
import org.datasyslab.babylon.extension.visualizationEffect.ScatterPlot
import org.datasyslab.babylon.utils.{ ColorizeOption, EarthdataHDFPointMapper, ImageType }
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.PointRDD
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import org.datasyslab.geospark.spatialRDD.RectangleRDD
import com.vividsolutions.jts.geom.Envelope

/**
 * The Class ScalaExample.
 */
object ScalaExample extends App {
  val sparkConf = new SparkConf().setAppName("BabylonDemo").setMaster("local[4]")
  val sparkContext = new SparkContext(sparkConf)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
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
  val earthdataInputLocation = System.getProperty("user.dir") + "/src/test/resources/modis/modis.csv"
  val earthdataNumPartitions = 5
  val HDFIncrement = 5
  val HDFOffset = 2
  val HDFRootGroupName = "MOD_Swath_LST"
  val HDFDataVariableName = "LST"
  val HDFDataVariableList = Array("LST", "QC", "Error_LST", "Emis_31", "Emis_32")
  val HDFswitchXY = true
  val urlPrefix = System.getProperty("user.dir") + "/src/test/resources/modis/"

  if (buildScatterPlot(scatterPlotOutputPath) && buildHeatMap(heatMapOutputPath)
    && buildChoroplethMap(choroplethMapOutputPath) && parallelFilterRenderStitch(parallelFilterRenderStitchOutputPath + "-stitched")
    && parallelFilterRenderNoStitch(parallelFilterRenderStitchOutputPath) && parallelFilterRenderBothStitch(parallelFilterRenderStitchOutputPath + "both") && earthdataVisualization(earthdataScatterPlotOutputPath))
    System.out.println("All 5 Babylon Demos have passed.")
  else System.out.println("Babylon Demos failed.")

  /**
   * Builds the scatter plot.
   *
   * @param outputPath the output path
   * @return true, if successful
   */
  def buildScatterPlot(outputPath: String): Boolean = {
    val spatialRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions)
    var visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false)
    visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
    visualizationOperator.Visualize(sparkContext, spatialRDD)
    var imageGenerator = new BabylonImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG)
    visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false, -1, -1, false, true)
    visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
    visualizationOperator.Visualize(sparkContext, spatialRDD)
    imageGenerator = new BabylonImageGenerator
    imageGenerator.SaveVectorImageAsLocalFile(visualizationOperator.vectorImage, outputPath, ImageType.SVG)
    visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false, -1, -1, true, true)
    visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
    visualizationOperator.Visualize(sparkContext, spatialRDD)
    imageGenerator = new BabylonImageGenerator
    imageGenerator.SaveVectorImageAsSparkFile(visualizationOperator.distributedVectorImage, "file://" + outputPath + "-distributed", ImageType.SVG)
    true
  }

  /**
   * Builds the heat map.
   *
   * @param outputPath the output path
   * @return true, if successful
   */
  def buildHeatMap(outputPath: String): Boolean = {
    val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions)
    val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2)
    visualizationOperator.Visualize(sparkContext, spatialRDD)
    val imageGenerator = new BabylonImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG)
    true
  }

  /**
   * Builds the choropleth map.
   *
   * @param outputPath the output path
   * @return true, if successful
   */
  def buildChoroplethMap(outputPath: String): Boolean = {
    val spatialRDD = new PointRDD(sparkContext, PointInputLocation, PointOffset, PointSplitter, false, PointNumPartitions)
    val queryRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions)
    spatialRDD.spatialPartitioning(GridType.RTREE)
    queryRDD.spatialPartitioning(spatialRDD.grids)
    spatialRDD.buildIndex(IndexType.RTREE, true)
    val joinResult = JoinQuery.SpatialJoinQueryCountByKey(spatialRDD, queryRDD, true, false)
    val visualizationOperator = new ChoroplethMap(1000, 600, USMainLandBoundary, false)
    visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true)
    visualizationOperator.Visualize(sparkContext, joinResult)
    val frontImage = new ScatterPlot(1000, 600, USMainLandBoundary, false)
    frontImage.CustomizeColor(0, 0, 0, 255, Color.GREEN, true)
    frontImage.Visualize(sparkContext, queryRDD)
    val overlayOperator = new RasterOverlayOperator(visualizationOperator.rasterImage)
    overlayOperator.JoinImage(frontImage.rasterImage)
    val imageGenerator = new BabylonImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(overlayOperator.backRasterImage, outputPath, ImageType.PNG)
    true
  }

  /**
   * Parallel filter render stitch.
   *
   * @param outputPath the output path
   * @return true, if successful
   */
  def parallelFilterRenderNoStitch(outputPath: String): Boolean = {
    val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions)
    val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2, 4, 4, true, true)
    visualizationOperator.Visualize(sparkContext, spatialRDD)
    val imageGenerator = new BabylonImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, outputPath, ImageType.PNG)
    true
  }

  def parallelFilterRenderBothStitch(outputPath: String): Boolean = {
    val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY)
    val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2, 4, 4, true, true)
    visualizationOperator.Visualize(sparkContext, spatialRDD)
    val imageGenerator = new BabylonImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, outputPath, ImageType.PNG)

    visualizationOperator.stitchImagePartitions
    imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath + "-stitched", ImageType.PNG)
    true
  }

  /**
   * Parallel filter render stitch.
   *
   * @param outputPath the output path
   * @return true, if successful
   */
  def parallelFilterRenderStitch(outputPath: String): Boolean = {
    val spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions)
    val visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2, 4, 4, true, true)
    visualizationOperator.Visualize(sparkContext, spatialRDD)
    visualizationOperator.stitchImagePartitions
    val imageGenerator = new BabylonImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG)
    true
  }

  def earthdataVisualization(outputPath: String): Boolean = {
    val earthdataHDFPoint = new EarthdataHDFPointMapper(HDFIncrement, HDFOffset, HDFRootGroupName,
      HDFDataVariableList, HDFDataVariableName, HDFswitchXY, urlPrefix)
    val spatialRDD = new PointRDD(sparkContext, earthdataInputLocation, earthdataNumPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY)
    val visualizationOperator = new ScatterPlot(1000, 600, spatialRDD.boundaryEnvelope, ColorizeOption.ZAXIS, false, false)
    visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.BLUE, true)
    visualizationOperator.Visualize(sparkContext, spatialRDD)
    val imageGenerator = new BabylonImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG)
    true
  }

}