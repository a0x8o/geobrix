package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.gridx.grid.{BNG, H3, Quadbin}
import com.databricks.labs.gbx.rasterx.expressions.grid.{
    RasterToGridGeneric,
    RST_BNG_RasterToGrid,
    RST_H3_RasterToGridAvg, RST_H3_RasterToGridCount, RST_H3_RasterToGridMax,
    RST_H3_RasterToGridMedian, RST_H3_RasterToGridMin, RST_H3_RasterToGridStddev,
    RST_H3_RasterToGridSum, RST_H3_RasterToGridVariance,
    RST_Quadbin_RasterToGrid
}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.collection.mutable

class RST_GridExecuteTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString.replace("file:/", "/")
        ds = gdal.Open(tifPath)
    }

    override def afterAll(): Unit = {
        ds.delete()
    }

    test("RST_H3_RasterToGridAvg should produce average cells") {
        val result = RST_H3_RasterToGridAvg.execute(ds, 2)
        result.length shouldBe 1
        result(0).length should be > 0
        val sample = result(0).take(5)
        sample.foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0.0
        }
    }

    test("RST_H3_RasterToGridCount should produce count cells") {
        val result = RST_H3_RasterToGridCount.execute(ds, 2)
        result.length shouldBe 1
        result(0).length should be > 0
        val sample = result(0).take(5)
        sample.foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0
        }
    }

    test("RST_H3_RasterToGridMax should produce max cells") {
        val result = RST_H3_RasterToGridMax.execute(ds, 2)
        result.length shouldBe 1
        result(0).length should be > 0
        val sample = result(0).take(5)
        sample.foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0.0
        }
    }

    test("RST_H3_RasterToGridMin should produce min cells") {
        val result = RST_H3_RasterToGridMin.execute(ds, 2)
        result.length shouldBe 1
        result(0).length should be > 0
        val sample = result(0).take(5)
        sample.foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0.0
        }
    }

    test("RST_H3_RasterToGridMedian should produce median cells") {
        val result = RST_H3_RasterToGridMedian.execute(ds, 2)
        result.length shouldBe 1
        result(0).length should be > 0
        val sample = result(0).take(5)
        sample.foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0.0
        }
    }

    test("RST_H3_RasterToGridSum should produce sum cells consistent with avg*count") {
        val sumRes = RST_H3_RasterToGridSum.execute(ds, 2)
        sumRes.length shouldBe 1
        sumRes(0).length should be > 0
        sumRes(0).take(5).foreach { case (cellID, measure) =>
            cellID should be > 0L
            measure should be >= 0.0
        }
        // sum == avg * count per cell (same summation-order class as avg).
        val avgByCell = RST_H3_RasterToGridAvg.execute(ds, 2)(0).toMap
        val cntByCell = RST_H3_RasterToGridCount.execute(ds, 2)(0).toMap
        sumRes(0).foreach { case (cellID, sumVal) =>
            val expected = avgByCell(cellID) * cntByCell(cellID)
            sumVal shouldBe (expected +- 1e-6)
        }
    }

    test("generic execute matches per-grid avg for all three grids") {
        val fAvg: mutable.ArrayBuffer[Double] => Double = b => b.sum / b.size

        // H3: generic should match the per-grid path end-to-end (reprojection + aggregation)
        val h3Gen = RasterToGridGeneric.execute(H3, ds, 2, fAvg)
        val h3Cur = RST_H3_RasterToGridAvg.execute(ds, 2)
        h3Gen.map(_.map { case (c, v) => (c.toString, v) }).map(_.toMap) shouldBe
            h3Cur.map(_.map { case (c, v) => (c.toString, v) }).map(_.toMap)

        // Quadbin: same check at resolution 10
        val qbGen = RasterToGridGeneric.execute(Quadbin, ds, 10, fAvg)
        val qbCur = RST_Quadbin_RasterToGrid.execute(ds, 10, fAvg)
        qbGen.map(_.map { case (c, v) => (c.toString, v) }).map(_.toMap) shouldBe
            qbCur.map(_.map { case (c, v) => (c.toString, v) }).map(_.toMap)

        // BNG: generic called with BNG.isValid filter must match the per-grid path
        val bngGen = RasterToGridGeneric.execute(BNG, ds, 3, fAvg, BNG.isValid)
        val bngCur = RST_BNG_RasterToGrid.execute(ds, 3, fAvg)
        bngGen.map(_.map { case (c, v) => (c.toString, v) }).map(_.toMap) shouldBe
            bngCur.map(_.map { case (c, v) => (c.toString, v) }).map(_.toMap)
    }

    test("RST_H3_RasterToGridVariance is population variance (>=0) and stddev == its sqrt") {
        val varRes = RST_H3_RasterToGridVariance.execute(ds, 2)
        val stdRes = RST_H3_RasterToGridStddev.execute(ds, 2)
        varRes.length shouldBe 1
        varRes(0).length should be > 0
        // Population variance is always >= 0; at least one multi-pixel cell in a
        // real MODIS tile carries nonzero spread (guard against a vacuous pass).
        val varByCell = varRes(0).toMap
        varByCell.values.foreach { v => v should be >= 0.0 }
        varByCell.values.exists(_ > 0.0) shouldBe true
        // stddev == sqrt(variance) per cell, exact cell-set match.
        val stdByCell = stdRes(0).toMap
        stdByCell.keySet shouldBe varByCell.keySet
        stdByCell.foreach { case (cellID, stdVal) =>
            stdVal shouldBe (math.sqrt(varByCell(cellID)) +- 1e-9)
        }
        // No NaN leaks from the two-pass math.
        varByCell.values.foreach { v => v.isNaN shouldBe false }
    }

}
