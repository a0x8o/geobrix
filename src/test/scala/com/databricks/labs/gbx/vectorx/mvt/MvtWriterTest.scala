package com.databricks.labs.gbx.vectorx.mvt

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite

/** Direct unit tests for [[MvtWriter]] — happy path + bad-input resilience. */
class MvtWriterTest extends AnyFunSuite {

    private val gf = new GeometryFactory()

    test("encode should return empty Array[Byte] for an empty feature list") {
        val out = MvtWriter.encode("empty", 4096, Seq.empty)
        assert(out != null && out.isEmpty)
    }

    test("encode should skip null / empty / invalid WKB rows and still emit good ones") {
        val good = JTS.toWKB(gf.createPoint(new Coordinate(0.5, 0.5)))
        val features = Seq(
            (null.asInstanceOf[Array[Byte]], Map[String, Any]("name" -> "skip-null")),
            (Array.emptyByteArray, Map[String, Any]("name" -> "skip-empty")),
            (Array[Byte](0, 1, 2, 3), Map[String, Any]("name" -> "skip-invalid")),
            (good, Map[String, Any]("name" -> "ok"))
        )
        val out = MvtWriter.encode("layer1", 4096, features)
        assert(out != null && out.nonEmpty)
        assert(new String(out, "UTF-8").contains("layer1"))
    }

}
