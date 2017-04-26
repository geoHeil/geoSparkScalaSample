// Copyright (C) 2017 Georg Heiler
package myOrg.visualization

import java.awt.Color

import com.vividsolutions.jts.geom.{ Envelope, Polygon }
import org.apache.spark.api.java.JavaPairRDD
import org.datasyslab.babylon.core.OverlayOperator
import org.datasyslab.babylon.extension.imageGenerator.{ NativeJavaImageGenerator, SparkImageGenerator }
import org.datasyslab.babylon.extension.visualizationEffect.{ ChoroplethMap, HeatMap, ScatterPlot }
import org.datasyslab.babylon.utils.ImageType
import org.datasyslab.geospark.spatialRDD.{ PolygonRDD, SpatialRDD }

/**
 * Visualization utility functions
 */
object Vis {
  @transient private lazy val localImageGenerator = new NativeJavaImageGenerator()
  @transient private lazy val sparkImageGenerator = new SparkImageGenerator()

  /**
   * Builds the scatter plot of a geometry. Based on
   * https://github.com/DataSystemsLab/GeoSpark/blob/master/src/main/java/org/datasyslab/babylon/showcase/Example.java
   *
   * @param outputPath the output path
   * @return true, if successful
   */
  def buildScatterPlot(outputPath: String, spatialRDD: SpatialRDD, envelope: Envelope): Boolean = {
    //    val envelope = spatialRDD.boundaryEnvelope
    val s = spatialRDD.getRawSpatialRDD.rdd.sparkContext
    import org.datasyslab.babylon.utils.ImageType
    val vLocalVector = new ScatterPlot(1000, 600, envelope, false, -1, -1, false, true)
    vLocalVector.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
    vLocalVector.Visualize(s, spatialRDD)
    localImageGenerator.SaveAsFile(vLocalVector.vectorImage, outputPath + "localVector", ImageType.SVG)

    val vLocalRaster = new ScatterPlot(1000, 600, envelope, false, -1, -1, false, false)
    vLocalRaster.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
    vLocalRaster.Visualize(s, spatialRDD)
    localImageGenerator.SaveAsFile(vLocalRaster.rasterImage, outputPath + "localRaster", ImageType.PNG)

    // TODO try using native 
    // TODO fix compile error of:
    /**
     * overloaded method value SaveAsFile with alternatives:
     * [error]   (x$1: java.util.List[String],x$2: String,x$3: org.datasyslab.babylon.utils.ImageType)Boolean <and>
     * [error]   (x$1: java.awt.image.BufferedImage,x$2: String,x$3: org.datasyslab.babylon.utils.ImageType)Boolean <and>
     * [error]   (x$1: org.apache.spark.api.java.JavaPairRDD,x$2: String,x$3: org.datasyslab.babylon.utils.ImageType)Boolean
     * [error]  cannot be applied to (org.apache.spark.api.java.JavaPairRDD[Integer,String], String, org.datasyslab.babylon.utils.ImageType)
     * [error]     sparkImageGenerator.SaveAsFile(vDistributedVector.distributedVectorImage, outputPath + "distributedVector", ImageType.SVG)
     *
     */
    //    val vDistributedVector = new ScatterPlot(1000, 600, envelope, false, 2, 2, true, true)
    //    vDistributedVector.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
    //    vDistributedVector.Visualize(s, spatialRDD)
    //    sparkImageGenerator.SaveAsFile(vDistributedVector.distributedVectorImage, outputPath + "distributedVector", ImageType.SVG)
    //
    //    val vDistributedRaster = new ScatterPlot(1000, 600, envelope, false, 2, 2, true, false)
    //    vDistributedRaster.CustomizeColor(255, 255, 255, 255, Color.GREEN, true)
    //    vDistributedRaster.Visualize(s, spatialRDD)
    //    sparkImageGenerator.SaveAsFile(vDistributedRaster.distributedRasterImage, outputPath + "distributedRaster", ImageType.PNG)
  }

  /**
   * Builds the heat map.
   *
   * @param outputPath the output path
   * @return true, if successful
   */
  def buildHeatMap(outputPath: String, spatialRDD: SpatialRDD): Boolean = {
    val s = spatialRDD.getRawSpatialRDD.rdd.sparkContext
    val visualizationOperator = new HeatMap(7000, 4900, spatialRDD.boundaryEnvelope, false, 2, -1, -1, false, false)
    visualizationOperator.Visualize(s, spatialRDD)
    import org.datasyslab.babylon.utils.ImageType
    localImageGenerator.SaveAsFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG)
  }

  /**
   * Builds the choropleth map.
   *
   * @param outputPath the output path
   * @return true, if successful
   */
  def buildChoroplethMap(outputPath: String, joinResult: JavaPairRDD[Polygon, java.lang.Long], objectRDD: PolygonRDD): Boolean = {
    val s = joinResult.rdd.sparkContext
    val visualizationOperator = new ChoroplethMap(1000, 600, objectRDD.boundaryEnvelope, false, -1, -1, false, true)
    visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true)
    visualizationOperator.Visualize(s, joinResult)

    val frontImage = new ScatterPlot(1000, 600, objectRDD.boundaryEnvelope, false, -1, -1, false, true)
    frontImage.CustomizeColor(0, 0, 0, 255, Color.GREEN, true)
    frontImage.Visualize(s, objectRDD) // TODO check if left vs. right object vs query is not mixed up

    val overlayOperator = new OverlayOperator(visualizationOperator.vectorImage, true)
    overlayOperator.JoinImage(frontImage.vectorImage)
    import org.datasyslab.babylon.utils.ImageType
    localImageGenerator.SaveAsFile(overlayOperator.backVectorImage, outputPath, ImageType.SVG)
  }

  /**
   * Parallel filter render no stitch.
   *
   * @param outputPath the output path
   * @return true, if successful
   */
  def parallelFilterRenderNoStitch(outputPath: String, spatialRDD: SpatialRDD): Boolean = {
    val s = spatialRDD.getRawSpatialRDD.rdd.sparkContext
    val visualizationOperator = new HeatMap(7000, 4900, spatialRDD.boundaryEnvelope, false, 2, -1, -1, false, false)
    visualizationOperator.Visualize(s, spatialRDD)
    localImageGenerator.SaveAsFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG)
  }

  /**
   * Parallel filter render stitch.
   *
   * @param outputPath the output path
   * @return true, if successful
   */
  def parallelFilterRenderStitch(outputPath: String, spatialRDD: SpatialRDD): Boolean = {
    val s = spatialRDD.getRawSpatialRDD.rdd.sparkContext
    val visualizationOperator = new HeatMap(7000, 4900, spatialRDD.boundaryEnvelope, false, 2, 4, 4, false, false)
    visualizationOperator.Visualize(s, spatialRDD)
    visualizationOperator.stitchImagePartitions
    localImageGenerator.SaveAsFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG)
  }
}
