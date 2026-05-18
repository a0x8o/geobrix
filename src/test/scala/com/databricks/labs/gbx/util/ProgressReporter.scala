package com.databricks.labs.gbx.util

import org.scalatest.Reporter
import org.scalatest.events._

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

/**
 * Custom ScalaTest reporter that prints a one-line progress marker after every
 * `SuiteCompleted` event so that long mvn test runs surface "how far in are we"
 * without needing to count test names in the log.
 *
 * Wired in pom.xml via scalatest-maven-plugin's `<reporters>` config. Loaded by
 * reflection; must have a public no-arg constructor and live on the test
 * classpath.
 *
 * Sample lines:
 *
 *   [progress] suite #12/64 done · SpatialRefOpsTest · 215 ms · tests=6 (0 failed)
 *                                · totals: 312 tests, 0 failed · elapsed 3m 24s
 *   [progress] RUN COMPLETE · 64 suites · 1,247 tests · 0 failed · elapsed 18m 03s
 *
 * The "/M" denominator is computed once on the first event by walking
 * `target/test-classes/` and counting `*Test.class` files (geobrix's
 * convention; verified by `find src/test/scala -name '*Test.scala' | wc -l`).
 * For filtered runs (`-Dsuites=...`) M still reflects the full discoverable
 * count, so `#3/64` reads as "3 of 64 available" rather than "3 of 3 selected".
 * The discovery path can be overridden with `-DgbxTestClassesDir=…`; if the
 * directory is missing entirely, M is suppressed and only `#N` is printed.
 *
 * Counters are AtomicInteger because ScalaTest may fire events from multiple
 * threads when suites run in parallel.
 */
class ProgressReporter extends Reporter {
  private val suitesCompleted = new AtomicInteger(0)
  private val testsTotal = new AtomicInteger(0)
  private val testsFailedTotal = new AtomicInteger(0)
  private val testsInCurrentSuite = new ThreadLocal[Int] {
    override def initialValue(): Int = 0
  }
  private val failedInCurrentSuite = new ThreadLocal[Int] {
    override def initialValue(): Int = 0
  }
  private val startTimeMs = System.currentTimeMillis()

  // 0 means "not discovered" — the formatter falls back to just `#N`.
  private lazy val totalSuites: Int = discoverTotalSuites()

  override def apply(event: Event): Unit = event match {
    case _: SuiteStarting =>
      testsInCurrentSuite.set(0)
      failedInCurrentSuite.set(0)

    case _: TestSucceeded =>
      testsTotal.incrementAndGet()
      testsInCurrentSuite.set(testsInCurrentSuite.get() + 1)

    case _: TestFailed =>
      testsTotal.incrementAndGet()
      testsFailedTotal.incrementAndGet()
      testsInCurrentSuite.set(testsInCurrentSuite.get() + 1)
      failedInCurrentSuite.set(failedInCurrentSuite.get() + 1)

    case e: SuiteCompleted =>
      val n = suitesCompleted.incrementAndGet()
      val suiteMs = e.duration.getOrElse(0L)
      val suiteTests = testsInCurrentSuite.get()
      val suiteFailed = failedInCurrentSuite.get()
      val totalTests = testsTotal.get()
      val totalFailed = testsFailedTotal.get()
      Console.out.println(
        f"[progress] suite ${suiteIndex(n)} done · ${e.suiteName} · $suiteMs%,d ms · " +
          f"tests=$suiteTests ($suiteFailed failed) · " +
          f"totals: $totalTests%,d tests, $totalFailed failed · elapsed ${elapsedHuman()}"
      )
      Console.out.flush()
      testsInCurrentSuite.remove()
      failedInCurrentSuite.remove()

    case _: RunCompleted =>
      Console.out.println(
        f"[progress] RUN COMPLETE · ${suitesCompleted.get}%,d suites · " +
          f"${testsTotal.get}%,d tests · ${testsFailedTotal.get}%,d failed · " +
          f"elapsed ${elapsedHuman()}"
      )
      Console.out.flush()

    case _ => // ignore other events
  }

  private def suiteIndex(n: Int): String =
    if (totalSuites > 0) f"#$n/$totalSuites" else f"#$n"

  private def elapsedHuman(): String = {
    val ms = System.currentTimeMillis() - startTimeMs
    val s = ms / 1000
    val m = s / 60
    val rem = s % 60
    if (m > 0) f"${m}m ${rem}%02ds" else f"${s}s"
  }

  /**
   * Walks `target/test-classes/` (or `-DgbxTestClassesDir=…`) and counts compiled
   * `*Test.class` files, excluding inner classes (filenames containing `$`).
   * Returns 0 on any error or if the directory doesn't exist — caller treats 0
   * as "no denominator, print just #N".
   */
  private def discoverTotalSuites(): Int = {
    val path = sys.props.getOrElse("gbxTestClassesDir", "target/test-classes")
    val dir = new File(path)
    if (!dir.isDirectory) 0
    else try countTestClasses(dir) catch { case _: Throwable => 0 }
  }

  private def countTestClasses(dir: File): Int = {
    val entries = Option(dir.listFiles()).getOrElse(Array.empty[File])
    entries.foldLeft(0) { (acc, f) =>
      if (f.isDirectory) acc + countTestClasses(f)
      else if (f.getName.endsWith("Test.class") && !f.getName.contains("$")) acc + 1
      else acc
    }
  }
}
