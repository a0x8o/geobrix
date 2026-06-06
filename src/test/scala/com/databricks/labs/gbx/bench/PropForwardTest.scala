package com.databricks.labs.gbx.bench

import org.scalatest.funsuite.AnyFunSuite

class PropForwardTest extends AnyFunSuite {
  test("bench system properties are forwarded to the test JVM") {
    // Default from pom.xml <properties> is "both"; proves -Dgbx.bench.modes is forwarded.
    assert(sys.props.getOrElse("gbx.bench.modes", "MISSING") != "MISSING")
  }
}
