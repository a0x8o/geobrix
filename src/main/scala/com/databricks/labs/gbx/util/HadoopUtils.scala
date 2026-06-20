package com.databricks.labs.gbx.util

import com.google.common.io.{ByteStreams, Closeables}
import org.apache.hadoop.fs._
import org.apache.orc.util.Murmur3
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import scala.collection.mutable

/** Path normalization (Volumes, DBFS, file:), listing, first-file, and copy for Hadoop filesystems. */
//noinspection ScalaWeakerAccess
object HadoopUtils {

    var hadoopConf: SerializableConfiguration = _

    /** Sets the default Hadoop config used by listHadoopFiles when no config is passed. */
    def setHadoopConf(hconf: SerializableConfiguration): Unit = {
        hadoopConf = hconf
    }

    /** Normalizes a path to a Hadoop-resolvable URI on the local (FUSE-backed) `file:` connector.
      *
      * The supported storage fabric on current DBRs is FUSE-mounted under `/Volumes/...` (Unity
      * Catalog Volumes) and `/Workspace/...` (Workspace files); their URI forms `dbfs:/Volumes/...`
      * and `file:/...` are accepted too. A scheme-less `/Volumes/...` would otherwise resolve
      * against `fs.defaultFS` (`dbfs:` on classic UC compute), which does NOT reach the FUSE mount
      * and silently returns a null tile — so everything is routed to `file:`.
      *
      * Legacy pre-Volumes DBFS (`/dbfs/...`, `dbfs:/...`) is NOT a supported target: the retired
      * `/dbfs/` FUSE mount and the `dbfs:` connector are never emitted. A `/dbfs/Volumes/` or
      * `dbfs:/Volumes/` alias is coerced to the supported `file:/Volumes/...`; any other legacy DBFS
      * path is coerced off the `/dbfs` prefix (best-effort `file:` — such reads are expected to fail
      * rather than silently use the retired mount).
      *
      *  - `/Volumes/...`, `dbfs:/Volumes/...`, `/dbfs/Volumes/...` -> `file:/Volumes/...`
      *  - `/Workspace/...` -> `file:/Workspace/...`; `file:/...` kept as-is
      *  - `/tmp/...` and other OS-absolute paths -> `file:...`
      */
    def cleanPath(inPath: String): String = {
        inPath match {
            // Unity Catalog Volumes (FUSE OS path, the dbfs:/Volumes URI, or the legacy /dbfs/Volumes
            // alias) -> the supported local connector at file:/Volumes/...
            case _ if inPath.startsWith("/Volumes/")       => s"file:$inPath"
            case _ if inPath.startsWith("dbfs:/Volumes/")  => s"file:${inPath.stripPrefix("dbfs:")}"
            case _ if inPath.startsWith("/dbfs/Volumes/")  => s"file:${inPath.stripPrefix("/dbfs")}"
            // Workspace files FUSE -> file:/Workspace/...
            case _ if inPath.startsWith("/Workspace/")     => s"file:$inPath"
            // Already a local/FUSE connector URI (file:/Volumes/, file:/Workspace/, file:/tmp/, ...).
            case _ if inPath.startsWith("file:/")          => inPath
            // Legacy pre-Volumes DBFS is unsupported: coerce AWAY from /dbfs and the dbfs: connector
            // (neither is used on supported DBRs). No automatic mapping to /Volumes or /Workspace.
            case _ if inPath.startsWith("/dbfs/")          => s"file:${inPath.stripPrefix("/dbfs")}"
            case _ if inPath.startsWith("dbfs:/")          => s"file:/${inPath.stripPrefix("dbfs:/")}"
            // OS-absolute local paths (incl. /tmp/).
            case _ if inPath.startsWith("/")               => s"file:$inPath"
            case _                                         => s"file:/$inPath"
        }
    }

    /** Lists non-directory files under inPath using hadoopConf. */
    def listHadoopFiles(inPath: String): Seq[String] = {
        listHadoopFiles(inPath, hadoopConf)
    }

    /** Spark/Hadoop convention: a name starting with '_' or '.' is a marker or hidden file
      * (e.g. `_SUCCESS`, `_committed_*`, `.crc`), NOT data. Skip these when enumerating a
      * directory so a reader pointed at a writer's output dir doesn't try to open `_SUCCESS`
      * as a dataset (mirrors Spark's default PathFilter). */
    private def isDataFile(path: Path): Boolean = {
        val n = path.getName
        !n.startsWith("_") && !n.startsWith(".")
    }

    /** Lists non-directory data files under inPath using the given Hadoop config
      * (marker/hidden files like `_SUCCESS` are skipped). */
    def listHadoopFiles(inPath: String, hconf: SerializableConfiguration): Seq[String] = {
        val path = new Path(new URI(cleanPath(inPath)))
        val fs = path.getFileSystem(hconf.value)
        fs.listStatus(path)
            .filter(st => !st.isDirectory && isDataFile(st.getPath))
            .map(_.getPath.toString)
    }

    /** Returns the first data file (by listing order) under inPath; used for schema inference
      * from a single file. Marker/hidden files (`_SUCCESS`, `.crc`, ...) are skipped so a
      * directory of writer output infers schema from a real shard, not the `_SUCCESS` marker. */
    def getFirstFile(inPath: String, hconf: SerializableConfiguration): String = {
        val path = new Path(new URI(cleanPath(inPath)))
        val fs = path.getFileSystem(hconf.value)
        val status = fs.getFileStatus(path)
        if (status.isDirectory) {
            val it = fs.listFiles(path, false)
            var first: String = null
            while (first == null && it.hasNext) {
                val st = it.next()
                if (isDataFile(st.getPath)) first = st.getPath.toString
            }
            if (first == null) {
                throw new IllegalArgumentException(s"No data files found under directory: $inPath")
            }
            first
        } else {
            path.toString
        }
    }

    /** Lists immediate subdirectories under inPath (non-recursive). */
    def listHadoopDirs(inPath: String, hconf: SerializableConfiguration): Seq[String] = {
        val path = new Path(new URI(cleanPath(inPath)))
        val fs = path.getFileSystem(hconf.value)
        if (!fs.exists(path)) Seq.empty[String]
        else fs
            .listStatus(path)
            .filter(_.isDirectory)
            .map(_.getPath.toString)
    }

    /** Recursively lists files under inPath, optionally filtered by regex and excluding empty files. */
    def listAllHadoopFiles(
        inPath: String,
        hconf: SerializableConfiguration,
        regexFilter: String,
        dropEmpty: Boolean = false
    ): mutable.Seq[String] = {
        val filter = if (regexFilter == "") ".*" else s".*$regexFilter.*"
        val path = new Path(new URI(cleanPath(inPath)))
        val fs = path.getFileSystem(hconf.value)
        val it = fs.listFiles(path, true) // recursive
        val files = scala.collection.mutable.ArrayBuffer[String]()
        while (it.hasNext) {
            val fileStatus = it.next()
            if (!dropEmpty || fileStatus.getLen > 0) {
                if (regexFilter == "" || filter == ".*") {
                    files += fileStatus.getPath.toString
                } else if (fileStatus.getPath.toString.matches(filter)) {
                    files += fileStatus.getPath.toString
                }
            }
        }
        files
    }

    /** Copies a file or directory from inPath to outPath; returns path to copied item in outDir. */
    def copyToPath(
        inPath: String,
        outPath: String,
        hconf: SerializableConfiguration
    ): String = {
        val copyFromPath = new Path(cleanPath(inPath))
        val srcFS = copyFromPath.getFileSystem(hconf.value)
        val srcStatus = srcFS.getFileStatus(copyFromPath)
        val outputDir =
            if (srcStatus.isDirectory) {
                new Path(cleanPath(outPath)).toString
            } else {
                new Path(cleanPath(outPath)).getParent.toString
            }
        copyToLocalDir(copyFromPath.toString, outputDir, hconf)
    }

    /** Copies files from srcFs whose names start with baseSrcPath prefix into dstDirPath on dstFs. */
    def copyRelativeFiles(
        srcFs: FileSystem,
        dstFs: FileSystem,
        baseSrcPath: Path,
        dstDirPath: Path
    ): Unit = {
        val extension = baseSrcPath.getName.split("\\.").lastOption.getOrElse("")
        val baseName = baseSrcPath.getName.stripSuffix(s".$extension")
        val prefix = baseName + "."

        val filter = new PathFilter {
            override def accept(path: Path): Boolean = path.getName.startsWith(prefix)
        }

        val parentDir = baseSrcPath.getParent
        val matchingFiles = srcFs.listStatus(parentDir, filter)

        matchingFiles.foreach { fileStatus =>
            val srcFile = fileStatus.getPath
            val dstFile = new Path(dstDirPath, srcFile.getName)
            AtomicDistributedCopy.copyIfNeeded(srcFs, dstFs, srcFile, dstFile)
        }
    }

    /** Copies inPath (file or dir) into outDir; for multi-file sources (e.g. .shp) copies all related files. Returns path to result. */
    def copyToLocalDir(inPath: String, outDir: String, hConf: SerializableConfiguration): String = {
        val copyFromPath = new Path(cleanPath(inPath))
        val outDirPath = new Path(cleanPath(outDir))
        val srcFS = copyFromPath.getFileSystem(hConf.value)
        val dstFS = outDirPath.getFileSystem(hConf.value)

        if (!dstFS.exists(outDirPath)) dstFS.mkdirs(outDirPath)

        if (srcFS.getFileStatus(copyFromPath).isDirectory) {
            val dst = new Path(outDirPath, copyFromPath.getName)
            AtomicDistributedCopy.copyIfNeeded(srcFS, dstFS, copyFromPath, dst)
            dst.toString
        } else {
            if (!dstFS.exists(outDirPath)) dstFS.mkdirs(outDirPath)
            copyRelativeFiles(srcFS, dstFS, copyFromPath, outDirPath)
            val fileName = copyFromPath.getName
            s"$outDirPath/$fileName"
        }
    }

    /** Reads file at status.getPath into a byte array; caller closes stream via try/finally. */
    def readContent(fs: FileSystem, status: FileStatus): Array[Byte] = {
        val stream = fs.open(status.getPath)
        try { // noinspection UnstableApiUsage
            ByteStreams.toByteArray(stream)
        } finally { // noinspection UnstableApiUsage
            Closeables.close(stream, true)
        }
    }

    /** Reads the bytes at `rawPath` through the Hadoop FileSystem.
      *
      * `cleanPath` routes UC Volumes / Workspace / local paths to the `file:` scheme, which the
      * executor's `fs.file.impl` resolves to Databricks' `WorkspaceLocalFileSystem` — the
      * UC-credentialed FUSE connector. We deliberately do NOT read via raw NIO
      * (`java.nio.file.Files`): in an expression's execution context (UC ephemeral-credential
      * scope) a raw POSIX read of a `/Volumes/...` FUSE path is denied ("Operation not
      * permitted"), whereas `WorkspaceLocalFileSystem` mediates the credential and succeeds
      * (issue #34). A fresh executor-side `Configuration` is used because the executor classpath's
      * core-site.xml registers `fs.file.impl`, while the driver-serialized `hConf` may not carry it.
      */
    def readBytes(rawPath: String, hConf: SerializableConfiguration): Array[Byte] = {
        val p = new Path(cleanPath(rawPath))
        val fs = p.getFileSystem(new org.apache.hadoop.conf.Configuration())
        readContent(fs, fs.getFileStatus(p))
    }

    /** Deletes the path recursively if it exists. */
    def deleteIfExists(tmpPath: String, hconf: SerializableConfiguration): Unit = {
        val cleanPath = HadoopUtils.cleanPath(tmpPath)
        val path = new Path(cleanPath)
        val fs = path.getFileSystem(hconf.value)
        if (fs.exists(path)) {
            fs.delete(path, true)
        }
    }

    /** Returns total size in bytes (file length or directory content summary). */
    def getSize(path: String, hConf: SerializableConfiguration): Long = {
        val cleanPath = new Path(HadoopUtils.cleanPath(path))
        val fs = cleanPath.getFileSystem(hConf.value)
        val status = fs.getFileStatus(cleanPath)
        if (status.isDirectory) {
            fs.getContentSummary(cleanPath).getLength
        } else {
            status.getLen
        }
    }

    /** Murmur3 hash of path + length + modification time; used as stable file id. */
    def getUUID(status: FileStatus): Long = {
        val uuid = Murmur3.hash64(
          status.getPath.toString.getBytes("UTF-8") ++
              status.getLen.toString.getBytes("UTF-8") ++
              status.getModificationTime.toString.getBytes("UTF-8")
        )
        uuid
    }

}
