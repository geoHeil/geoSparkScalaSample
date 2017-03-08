// Copyright (C) 2017 Georg Heiler
package myOrg

import java.util.Collections

import com.vividsolutions.jts.geom.{ MultiPolygon, Polygon }
import com.vividsolutions.jts.io.WKTReader
import org.apache.log4j.Logger
import org.apache.spark.api.java.function.FlatMapFunction
import org.json4s.ParserUtil.ParseException

import scala.collection.generic.Growable
import scala.collection.mutable

class CustomInputMapperWKT extends FlatMapFunction[String, Polygon] {

  @transient lazy val logger: Logger = Logger.getLogger(this.getClass)

  // based on https://github.com/DataSystemsLab/GeoSpark/blob/master/src/main/java/org/datasyslab/geospark/showcase/UserSuppliedPolygonMapper.java#L34
  override def call(line: String): java.util.Iterator[Polygon] = {
    val lines = line.split(";")
    if (lines.head.startsWith("WKT")) {
      // skipping the header and returning empty iterator
      logger.warn(s"skipping lines ${lines.head}")
      Collections.emptyIterator()
    } else {
      logger.warn(s"full lines ${line}")
      val lineString = lines.head
      logger.warn(lineString)
      try {
        val geoObject = new WKTReader().read(lineString)
        geoObject match {
          case m: MultiPolygon => {
            // TODO find a more scala native way for tis loop maybe a fold? or for ... yield?
            val result: collection.Seq[Polygon] with Growable[Polygon] = mutable.Buffer[Polygon]()
            var i = 0
            while (i < m.getNumGeometries) {
              val intermediateGeoObjectPolygon = m.getGeometryN(i).asInstanceOf[Polygon]
              intermediateGeoObjectPolygon.setUserData(lineString.tail) // TODO figure out if this is still correct for MULTIPLOYGONs
              result += intermediateGeoObjectPolygon
              i += 1
            }
            //TODO get implicit scala to java conversion to work
            scala.collection.JavaConversions.seqAsJavaList(result).iterator()
          }
          case p: Polygon => {
            // with single element
            val result: collection.Seq[Polygon] with Growable[Polygon] = mutable.Buffer[Polygon]()
            p.setUserData(lineString.tail)
            result += p
            // TODO replace lines above with smaller code as only single element is used. However, I tried the following
            // which did not work due to not inferred types
            //            scala.collection.JavaConversions.seqAsJavaList(p).iterator()
            // TODO get automatic implicit Seq(p).iterator.asJava to work - import of scala collection implict java conversion did not work
            scala.collection.JavaConversions.seqAsJavaList(result).iterator()
          }
          case _ => {
            logger.error(s"No geometry") //TODO proper logging better error handling
            Collections.emptyIterator()
          }
        }
      } catch {
        case e: ParseException => {
          logger.error("Could not parse")
          logger.error(e.getCause)
          logger.error(e.getMessage)
          // todo find better means than silently ignoring parse failure
          Collections.emptyIterator()
        }
      }
    }
  }
}
