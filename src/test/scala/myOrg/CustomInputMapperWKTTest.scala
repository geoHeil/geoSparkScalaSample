// Copyright (C) 2017 Georg Heiler

package myOrg

import java.io.File

import com.holdenkarau.spark.testing.{DatasetSuiteBase, SharedSparkContext}
import com.vividsolutions.jts.geom.Polygon
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import org.scalatest.{FunSuite, Matchers}

// TODO test spark unrelated parsing functionality maybe in a simple unit test without spark context.
// anyhow seing this full integration work is nice too.
class CustomInputMapperWKTTest extends FunSuite with SharedSparkContext with DatasetSuiteBase with Matchers {
  test("Check custom WKT format is read correctly") {
    val poly = new PolygonRDD(spark.sparkContext, "src"
      + File.separator + "main"
      + File.separator + "resources"
      + File.separator + "minimal01.csv", new CustomInputMapperWKT(), StorageLevel.MEMORY_ONLY)

    assert(poly.rawSpatialRDD.count == 3)
    val userData: Int = poly.rawSpatialRDD.first.asInstanceOf[Polygon].getUserData.asInstanceOf[Int]
    assert(userData == -120)
  }
}
