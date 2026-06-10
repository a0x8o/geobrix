package com.databricks.labs.gbx.bench

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}

/**
 * Regression coverage for the concurrent `ogr.RegisterAll()` race category.
 *
 * BACKGROUND: `GDALManager.init` registers GDAL (raster) drivers once per JVM under a lock,
 * but historically OGR (vector) registration was done ad-hoc — every `execute()` body that
 * touched OGR called `org.gdal.ogr.ogr.RegisterAll()` itself, UNGUARDED. `RegisterAll()` is a
 * process-global, non-thread-safe mutation of the OGR driver registry. Under concurrent Spark
 * tasks in one executor JVM these race: a `GetDriverByName(...)` racing a concurrent
 * `RegisterAll()` can return null (NPE) or the native layer can sigabrt/sigsegv (observed
 * on-cluster for rst_gridfrompoints_agg / rst_dtmfromgeoms_agg).
 *
 * STEP 1 (RED) outcome — DOCUMENTED, NOT KEPT AS A TEST:
 * A first "RED demonstration" test was written that hammered the OLD unguarded pattern
 * (8 threads x 50 iterations of raw `ogr.RegisterAll()` then `ogr.GetDriverByName("Memory")`).
 * On this GDAL build (libgdal 3.x) it did NOT surface as collectible nulls — it HARD-CRASHED
 * the JVM with a fatal SIGSEGV inside native GDAL:
 *
 *   SIGSEGV ... Problematic frame:
 *   C  [libgdal.so.37+...]  std::vector<WMSMiniDriverFactory*,...>::_M_realloc_insert<...>(...)
 *   "The crash happened outside the Java Virtual Machine in native code."
 *
 * i.e. concurrent `RegisterAll()` threads racing the driver-factory vectors corrupt native
 * memory and abort the whole JVM (taking the test runner with it). Per the task's Step-1
 * guidance, a JVM-aborting sigsegv is NOT expressible as a ScalaTest assertion (it kills the
 * suite, not the test case), so that demonstration test is intentionally NOT kept here. The
 * crash itself is the definitive proof the race is real and severe; the KEPT regression
 * guarantee below proves the race is CLOSED by routing every caller through the guard.
 *
 * STEP 3 (kept guard invariant): this test routes every concurrent caller through
 * `GDALManager.initOgr()` (register-once-under-lock) and asserts that on every iteration of
 * every thread `GetDriverByName("Memory")` is non-null and no exception escapes. With the
 * guard there is never a concurrent `RegisterAll()` (it runs exactly once, under the lock),
 * so the registry is never mutated while another thread reads it — the race that crashed the
 * JVM above cannot occur, and this passes deterministically.
 */
class OgrThreadSafetyTest extends AnyFunSuite {

  private val Threads = 8
  private val Iterations = 50

  test("guard invariant: concurrent GDALManager.initOgr() never races GetDriverByName") {
    GDALManager.loadSharedObjects(Iterable.empty[String])

    val nulls = new AtomicInteger(0)
    val errors = new ConcurrentLinkedQueue[Throwable]()
    val start = new CountDownLatch(1)
    val done = new CountDownLatch(Threads)

    val workers = (0 until Threads).map { _ =>
      val t = new Thread(() => {
        start.await()
        var i = 0
        while (i < Iterations) {
          try {
            // The GUARDED pattern: register-once-under-lock, then resolve a driver.
            GDALManager.initOgr()
            val driver = org.gdal.ogr.ogr.GetDriverByName("Memory")
            if (driver == null) nulls.incrementAndGet()
          } catch {
            case t: Throwable => errors.add(t)
          }
          i += 1
        }
        done.countDown()
      })
      t.setDaemon(true)
      t
    }
    workers.foreach(_.start())
    start.countDown()
    done.await()

    assert(errors.isEmpty, s"initOgr/GetDriverByName threw under concurrency: ${errors.toArray.mkString(", ")}")
    assert(nulls.get() == 0,
      s"GetDriverByName(\"Memory\") returned null ${nulls.get()} times under concurrent initOgr() — race not closed")
  }
}
