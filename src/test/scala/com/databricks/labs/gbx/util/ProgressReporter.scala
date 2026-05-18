package com.databricks.labs.gbx.util

import org.scalatest.Reporter
import org.scalatest.events._

import java.io.File
import java.lang.reflect.Modifier
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
 * The "/M" denominator starts as the reflection-filtered upper bound
 * (concrete + public + extends `org.scalatest.Suite` + has a public
 * no-arg constructor — same structural shape ScalaTest's own discovery
 * uses) and then decrements whenever a SuiteCompleted fires with 0 tests.
 * Those empty suites pass discovery structurally but run nothing — the
 * only way to detect them without running constructors at discovery time
 * (which would have side effects) is to watch them go by. M converges to
 * the true runnable count as empty suites are encountered; empty suites
 * themselves are silently dropped (no progress line emitted, so #N keeps
 * pace with what the user perceives as "actual work happened"). For
 * filtered runs (`-Dsuites=...`) M still reflects the full runnable
 * count, so `#3/63` reads as "3 of 63 available" rather than "3 of 3
 * selected". The discovery path can be overridden with
 * `-DgbxTestClassesDir=…`; if the directory is missing entirely, M is
 * suppressed and only `#N` is printed.
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

  // Starts at -1 ("not yet discovered"); first SuiteCompleted triggers the
  // class scan. 0 thereafter means "discovery returned nothing usable" — the
  // formatter falls back to just `#N` in that case. Otherwise this value is
  // an upper bound on the runnable suite count, decremented in-place when an
  // empty SuiteCompleted (0 tests) is observed.
  private val totalSuites = new AtomicInteger(-1)

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
      // Lazy-init M on the first SuiteCompleted so the cost is paid inside the
      // test JVM (where the test classpath is fully assembled), not in the
      // constructor of the reporter.
      if (totalSuites.get() == -1) totalSuites.set(discoverTotalSuites())
      val suiteTests = testsInCurrentSuite.get()
      if (suiteTests == 0) {
        // Empty suite (Suite-extending class with no `test(...)` blocks
        // registered): discovery counted it structurally; the run produced
        // zero work. Adjust M down and skip the progress line so #N stays
        // aligned with real work.
        if (totalSuites.get() > 0) totalSuites.decrementAndGet()
      } else {
        val n = suitesCompleted.incrementAndGet()
        val suiteMs = e.duration.getOrElse(0L)
        val suiteFailed = failedInCurrentSuite.get()
        val totalTests = testsTotal.get()
        val totalFailed = testsFailedTotal.get()
        Console.out.println(
          f"[progress] suite ${suiteIndex(n)} done · ${e.suiteName} · $suiteMs%,d ms · " +
            f"tests=$suiteTests ($suiteFailed failed) · " +
            f"totals: $totalTests%,d tests, $totalFailed failed · elapsed ${elapsedHuman()}"
        )
        Console.out.flush()
      }
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

  private def suiteIndex(n: Int): String = {
    val m = totalSuites.get()
    if (m > 0) f"#$n/$m" else f"#$n"
  }

  private def elapsedHuman(): String = {
    val ms = System.currentTimeMillis() - startTimeMs
    val s = ms / 1000
    val m = s / 60
    val rem = s % 60
    if (m > 0) f"${m}m ${rem}%02ds" else f"${s}s"
  }

  /**
   * Walks `target/test-classes/` (or `-DgbxTestClassesDir=…`) and counts the
   * `*Test.class` files that ScalaTest will actually run: concrete + public +
   * extends `org.scalatest.Suite` + has a public no-arg constructor. Returns 0
   * on any error or if the directory doesn't exist — caller treats 0 as
   * "no denominator, print just #N".
   */
  private def discoverTotalSuites(): Int = {
    val path = sys.props.getOrElse("gbxTestClassesDir", "target/test-classes")
    val dir = new File(path)
    if (!dir.isDirectory) return 0
    val suiteCls =
      try Class.forName("org.scalatest.Suite", false, Thread.currentThread().getContextClassLoader)
      catch { case _: Throwable => return 0 }
    try countRunnableSuites(dir, dir, suiteCls)
    catch { case _: Throwable => 0 }
  }

  private def countRunnableSuites(root: File, dir: File, suiteCls: Class[_]): Int = {
    val entries = Option(dir.listFiles()).getOrElse(Array.empty[File])
    entries.foldLeft(0) { (acc, f) =>
      if (f.isDirectory) acc + countRunnableSuites(root, f, suiteCls)
      else if (f.getName.endsWith("Test.class") && !f.getName.contains("$"))
        acc + (if (isRunnableSuite(root, f, suiteCls)) 1 else 0)
      else acc
    }
  }

  /**
   * Reflective check matching ScalaTest's discovery filter. Uses
   * `Class.forName(name, initialize=false, ...)` so static initializers don't
   * run during counting — only the class metadata is loaded. Any failure
   * (NoClassDefFoundError, missing transitive dep, locked classloader)
   * conservatively counts the class as "not runnable" so a transient
   * reflection issue can't inflate the denominator.
   */
  private def isRunnableSuite(root: File, classFile: File, suiteCls: Class[_]): Boolean = {
    try {
      val rel = root.toURI.relativize(classFile.toURI).getPath
      val className = rel.stripSuffix(".class").replace('/', '.')
      val cls = Class.forName(className, false, Thread.currentThread().getContextClassLoader)
      val mods = cls.getModifiers
      if (Modifier.isAbstract(mods) || Modifier.isInterface(mods) || !Modifier.isPublic(mods)) false
      else if (!suiteCls.isAssignableFrom(cls)) false
      else {
        try { cls.getConstructor(); true }
        catch { case _: NoSuchMethodException => false }
      }
    } catch {
      case _: Throwable => false
    }
  }
}
