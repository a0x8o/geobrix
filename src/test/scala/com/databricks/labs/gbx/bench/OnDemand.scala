package com.databricks.labs.gbx.bench

import org.scalatest.Tag

/**
 * ScalaTest tag for on-demand benchmark suites that must NOT run in the normal
 * (remote CI / default `mvn test`) suite. These suites only do useful work when
 * the `gbx.bench.*` system properties are set by the `gbx:bench:*` commands; in a
 * plain CI run they would otherwise be collected and cancel themselves, polluting
 * the test output with spurious CANCELED lines.
 *
 * The default scalatest-maven-plugin config excludes this tag via the overridable
 * `${tagsToExclude}` Maven property (see pom.xml). The `gbx:bench:*` commands clear
 * that property (`-DtagsToExclude=`) so the tagged suite runs on demand.
 *
 * Tag the `test(...)` block, e.g. `test("...", OnDemand) { ... }`.
 */
object OnDemand extends Tag("com.databricks.labs.gbx.bench.OnDemand")
