// Copyright (C) 2017 Georg Heiler

package myOrg

import java.io.File

import com.holdenkarau.spark.testing.{ DatasetSuiteBase, SharedSparkContext }
import com.vividsolutions.jts.geom.Polygon
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import org.scalatest.{ FunSuite, Matchers }

class CustomInputMapperWKTTest extends FunSuite with SharedSparkContext with DatasetSuiteBase with Matchers {

  test("Check custom WKT format is read correctly") {

    val poly = new PolygonRDD(spark.sparkContext, "src"
      + File.separator + "main"
      + File.separator + "resources"
      + File.separator + "minimal01.csv", new CustomInputMapperWKT(), StorageLevel.MEMORY_ONLY)

    assert(poly.rawSpatialRDD.count == 3)
    println(poly.rawSpatialRDD.first)
    println("####################################### user data")
    println(poly.rawSpatialRDD.first.asInstanceOf[Polygon].getUserData)
    println("###################################### user data end")
    assert(poly.rawSpatialRDD.first.asInstanceOf[Polygon].getUserData.toString.toInt == -120)
  }

}
