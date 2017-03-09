// Copyright (C) 2017 Georg Heiler
package myOrg

import java.util
import java.util.Collections

import com.vividsolutions.jts.geom.{ MultiPolygon, Polygon }
import com.vividsolutions.jts.io.WKTReader
import org.apache.log4j.Logger
import org.apache.spark.api.java.function.FlatMapFunction
import org.json4s.ParserUtil.ParseException

class CustomInputMapperWKT extends FlatMapFunction[String, Polygon] {

  @transient lazy val logger: Logger = Logger.getLogger(this.getClass)

  // based on https://github.com/DataSystemsLab/GeoSpark/blob/master/src/main/java/org/datasyslab/geospark/showcase/UserSuppliedPolygonMapper.java#L34
  override def call(line: String): java.util.Iterator[Polygon] = {
    val lines = line.split(";")
    if (lines.head.startsWith("WKT")) {
      // skipping the header and returning empty iterator
      logger.debug(s"skipping lines ${lines.head}")
      Collections.emptyIterator()
    } else {
      logger.debug(s"full lines ${line}")
      val lineString = lines.head
      logger.debug(lineString)
      try {
        new WKTReader().read(lineString) match {
          case m: MultiPolygon => {
            // as a java iterator must be returned it will be best to start using java collections from the start
            val result = new util.ArrayList[Polygon](m.getNumGeometries)
            var i = 0
            while (i < m.getNumGeometries) {
              val intermediateGeoObjectPolygon = m.getGeometryN(i).asInstanceOf[Polygon]
              intermediateGeoObjectPolygon.setUserData(lineString.tail)
              // TODO figure out if this is still correct for MULTIPOLYGONs user data
              // will probably be required to be loaded separately for each polygon
              logger.warn(s"using multy polygon, probably with wrong user data section of ${lineString.tail}")
              result.add(intermediateGeoObjectPolygon)
              i += 1
            }
            result.iterator
          }
          case p: Polygon => {
            logger.warn(lineString.head)
            logger.warn(lineString.tail)
            logger.warn(s"using user data of ${lineString.tail}")
            p.setUserData(lineString.tail)
            Collections.singleton(p).iterator
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
