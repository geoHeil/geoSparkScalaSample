// Copyright (C) 2017 Georg Heiler

package myOrg

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.{ FileDataSplitter, GridType, IndexType }
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{ PolygonRDD, RectangleRDD }

object GeoSpark extends App {

  val conf: SparkConf = new SparkConf()
    .setAppName("geomesaSparkInMemory")
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

  val objectRDD = new PolygonRDD(spark.sparkContext, "src" + File.separator
    + "main" + File.separator
    + "resources" + File.separator
    + "zcta510-polygon.csv", FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY)

  val minimalPolygonCustom = new PolygonRDD(spark.sparkContext, "src"
    + File.separator + "main"
    + File.separator + "resources"
    + File.separator + "minimal01.csv", new CustomInputMapperWKT(), StorageLevel.MEMORY_ONLY)

  objectRDD.spatialPartitioning(GridType.RTREE);
  //   TODO try out different types of indices, try out different sides (left, right) of the join and try broadcast join
  /*
   * GridType.RTREE means use R-Tree spatial partitioning technique. It will take the leaf node boundaries as parition boundary.
   * We support R-Tree partitioning and Voronoi diagram partitioning.
   */
  // TODO try out different combinations of indices
  objectRDD.buildIndex(IndexType.RTREE, true);
  /*
   * IndexType.RTREE enum means the index type is R-tree. We support R-Tree index and Quad-Tree index. But Quad-Tree doesn't support KNN.
   * True means build index on the spatial partitioned RDD. ONLY set true when doing Spatial Join Query.
   */
  minimalPolygonCustom.spatialPartitioning(objectRDD.grids);
  /*
   * Use the partition boundary of objectRDD to repartition the query window RDD, This is mandatory.
   */
  // TODO make the join work here. I want to join my pologons with some sample points or boxes
  val joinResult = JoinQuery.SpatialJoinQuery(objectRDD, minimalPolygonCustom, true)
  /*
   * true means use spatial index.
   */
  //  println(s"count result size ${joinResult.count}")

  // TODO convert to dataframe
  //  joinResult.rawSpatialRDD.toDF().show
  //  joinResult.rawSpatialRDD.rdd.toDS().showhttps://github.com/DataSystemsLab/GeoSpark/tree/2adce0c1c13af172f9be6c3cd0cda1431c74d0b8/src/main/java/org/datasyslab/geospark/showcase

  //  TODO visualize result on a map via babylon

  spark.stop
}
