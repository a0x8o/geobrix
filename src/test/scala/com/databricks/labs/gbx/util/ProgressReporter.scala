package com.databricks.labs.gbx.util

import org.scalatest.Reporter
import org.scalatest.events._

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
 *   [progress] suite #12 done · SpatialRefOpsTest · 215 ms · tests=6 (0 failed)
 *                              · totals: 312 tests, 0 failed · elapsed 3m 24s
 *   [progress] RUN COMPLETE · 42 suites · 1,247 tests · 0 failed · elapsed 18m 03s
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
        f"[progress] suite #$n done · ${e.suiteName} · $suiteMs%,d ms · " +
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

  private def elapsedHuman(): String = {
    val ms = System.currentTimeMillis() - startTimeMs
    val s = ms / 1000
    val m = s / 60
    val rem = s % 60
    if (m > 0) f"${m}m ${rem}%02ds" else f"${s}s"
  }
}
