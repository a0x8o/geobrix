package com.databricks.labs.gbx.util

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Tests for AtomicDistributedCopy (copy-if-needed and wait-until-exists behavior on local FS). */
class AtomicDistributedCopyTest extends AnyFunSuite with BeforeAndAfterEach {

    private var tempDir: java.nio.file.Path = _
    private var srcPath: Path = _
    private var dstPath: Path = _
    private var localFs: org.apache.hadoop.fs.FileSystem = _

    override def beforeEach(): Unit = {
        tempDir = Files.createTempDirectory("AtomicDistributedCopyTest")
        localFs = org.apache.hadoop.fs.FileSystem.getLocal(new org.apache.hadoop.conf.Configuration())
        srcPath = new Path(tempDir.toUri.toString, "source.txt")
        dstPath = new Path(tempDir.toUri.toString, "dest.txt")
    }

    override def afterEach(): Unit = {
        if (localFs != null) localFs.close()
        if (tempDir != null) {
            try {
                Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
            } catch { case _: Exception => }
        }
    }

    test("copyIfNeeded should copy file when destination does not exist") {
        val out = localFs.create(srcPath)
        out.write("hello".getBytes)
        out.close()

        AtomicDistributedCopy.copyIfNeeded(localFs, localFs, srcPath, dstPath)

        localFs.exists(dstPath) shouldBe true
        val in = localFs.open(dstPath)
        val buf = Array.ofDim[Byte](32)
        val n = in.read(buf)
        in.close()
        new String(buf, 0, n) shouldBe "hello"
    }

    /** Helper: read dst back as a String. */
    private def readDst(): String = {
        val in = localFs.open(dstPath)
        val buf = Array.ofDim[Byte](32)
        val n = in.read(buf)
        in.close()
        new String(buf, 0, n)
    }

    test("copyIfNeeded skips re-copy when destination exists with the same length") {
        // Same length => treated as already-present (the cache key is the path; equal length is
        // the cheap content-match signal). copyIfNeeded must NOT throw and must leave dst intact.
        val out1 = localFs.create(srcPath)
        out1.write("src".getBytes) // 3 bytes
        out1.close()
        val out2 = localFs.create(dstPath)
        out2.write("dst".getBytes) // 3 bytes — same length as src
        out2.close()

        AtomicDistributedCopy.copyIfNeeded(localFs, localFs, srcPath, dstPath)

        localFs.exists(dstPath) shouldBe true
        readDst() shouldBe "dst" // unchanged: same length => no re-copy
    }

    test("copyIfNeeded re-copies when destination exists with a different length (stale cache)") {
        // Different length => the local copy is stale (the remote changed under the same path);
        // copyIfNeeded must overwrite it with the source content.
        val out1 = localFs.create(srcPath)
        out1.write("src".getBytes) // 3 bytes
        out1.close()
        val out2 = localFs.create(dstPath)
        out2.write("existing".getBytes) // 8 bytes — different length
        out2.close()

        AtomicDistributedCopy.copyIfNeeded(localFs, localFs, srcPath, dstPath)

        localFs.exists(dstPath) shouldBe true
        readDst() shouldBe "src" // re-copied: length mismatch => overwrite stale dst
    }

    test("copyIfNeeded with same path (src == dst) should not throw when file exists") {
        val out = localFs.create(srcPath)
        out.write("same".getBytes)
        out.close()

        AtomicDistributedCopy.copyIfNeeded(localFs, localFs, srcPath, srcPath)

        localFs.exists(srcPath) shouldBe true
    }
}
