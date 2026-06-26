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
        withFileSystem(path, hconf) { fs =>
            fs.listStatus(path)
                .filter(st => !st.isDirectory && isDataFile(st.getPath))
                .map(_.getPath.toString)
                .toSeq
        }
    }

    /** Returns the first data file (by listing order) under inPath; used for schema inference
      * from a single file. Marker/hidden files (`_SUCCESS`, `.crc`, ...) are skipped so a
      * directory of writer output infers schema from a real shard, not the `_SUCCESS` marker. */
    def getFirstFile(inPath: String, hconf: SerializableConfiguration): String = {
        val path = new Path(new URI(cleanPath(inPath)))
        withFileSystem(path, hconf) { fs =>
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
    }

    /** Lists data files under inPath via Spark's file index, which forwards the UC Volume / WSFS
      * credential. A raw driver-thread Hadoop FS `getFileStatus`/`listStatus` does NOT carry that
      * credential during query analysis (the OGR reader's `inferSchema` runs on the Spark analyzer
      * thread), so it throws `FileNotFoundException` on `/Volumes`; Spark's listing does carry it.
      * A single file or a `.gdb` / `.gdb.zip` / `.zip` dataset is returned as-is (not a dir of
      * shards). Falls back to the raw Hadoop FS listing if Spark's binaryFile can't enumerate it. */
    def listDataFilesSpark(spark: org.apache.spark.sql.SparkSession, inPath: String): Seq[String] = {
        val cp = cleanPath(inPath)
        val lower = inPath.toLowerCase(java.util.Locale.ROOT).stripSuffix("/")
        if (lower.endsWith(".gdb") || lower.endsWith(".gdb.zip") || lower.endsWith(".zip")) {
            Seq(cp)
        } else {
            try {
                val files = spark.read.format("binaryFile").load(cp).inputFiles.toSeq.sorted
                if (files.nonEmpty) files else Seq(cp)
            } catch {
                case _: Throwable =>
                    val hc = new SerializableConfiguration(spark.sessionState.newHadoopConf)
                    try listHadoopFiles(cp, hc) catch { case _: Throwable => Seq(cp) }
            }
        }
    }

    /** Stages a dataset's head file (plus any same-stem sidecars, e.g. shapefile `.shx`/`.dbf`/
      * `.prj`) to a local temp dir for OGR schema inference, returning the local head path. Reads
      * bytes via POSIX `java.io.File` on the FUSE path (proven to read `/Volumes` on driver and
      * executor; `binaryFile` content reads and raw Hadoop FS stats do NOT). Tries a direct read
      * first; if the calling (analyzer) thread lacks the UC Volume credential, falls back to a
      * one-task Spark job whose executor carries it. `candidates` is the `listDataFilesSpark`
      * listing, used to find the sidecars. */
    def stageHeadForSchemaSpark(
        spark: org.apache.spark.sql.SparkSession,
        headPath: String,
        candidates: Seq[String]
    ): String = {
        def baseName(p: String): String = p.replace("\\", "/").reverse.takeWhile(_ != '/').reverse
        def toPosix(p: String): String = p.stripPrefix("file:").stripPrefix("dbfs:")
        val headName = baseName(headPath)
        val dot = headName.lastIndexOf('.')
        val stem = if (dot > 0) headName.substring(0, dot) else headName
        val siblings = candidates.filter { p =>
            val n = baseName(p)
            n == headName || n.startsWith(stem + ".")
        }.distinct
        val toStage = if (siblings.isEmpty) Seq(headPath) else siblings
        val tmpDir = java.nio.file.Files.createTempDirectory("gbx_ogr_schema_").toFile
        for (p <- toStage) {
            val local = toPosix(p)
            val bytes: Array[Byte] =
                try java.nio.file.Files.readAllBytes(new java.io.File(local).toPath)
                catch {
                    case _: Throwable =>
                        // analyzer thread lacked the FUSE credential -> read on an executor task,
                        // which carries it (proven: executor java.io.File reads /Volumes).
                        spark.sparkContext.parallelize(Seq(local), 1)
                            .map(lp => java.nio.file.Files.readAllBytes(new java.io.File(lp).toPath))
                            .collect().head
                }
            java.nio.file.Files.write(new java.io.File(tmpDir, baseName(p)).toPath, bytes)
        }
        new java.io.File(tmpDir, headName).getAbsolutePath
    }

    /** Lists immediate subdirectories under inPath (non-recursive). */
    def listHadoopDirs(inPath: String, hconf: SerializableConfiguration): Seq[String] = {
        val path = new Path(new URI(cleanPath(inPath)))
        withFileSystem(path, hconf) { fs =>
            if (!fs.exists(path)) Seq.empty[String]
            else fs
                .listStatus(path)
                .filter(_.isDirectory)
                .map(_.getPath.toString)
                .toSeq
        }
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
        withFileSystem(path, hconf) { fs =>
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
    }

    /** Copies a file or directory from inPath to outPath; returns path to copied item in outDir. */
    def copyToPath(
        inPath: String,
        outPath: String,
        hconf: SerializableConfiguration
    ): String = {
        val copyFromPath = new Path(cleanPath(inPath))
        val outputDir = withFileSystem(copyFromPath, hconf) { srcFS =>
            if (srcFS.getFileStatus(copyFromPath).isDirectory) {
                new Path(cleanPath(outPath)).toString
            } else {
                new Path(cleanPath(outPath)).getParent.toString
            }
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
        withFileSystem(copyFromPath, hConf) { srcFS =>
            withFileSystem(outDirPath, hConf) { dstFS =>
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

    /** Runs `body` with the Hadoop FileSystem for `path` (resolved from the caller's config).
      *
      * NOTE (issue #34): this reads local / DBFS / Workspace paths. It does NOT read UC Volume
      * (`/Volumes/...`) paths from the JVM in the Spark execution context — the UC FUSE credential
      * is held only by Spark's managed Python worker, so `/Volumes` reads go through the Python
      * implementation (`gbx_rst_fromfile` is registered as the `pyrx` UDF; or use `binaryFile` +
      * `gbx_rst_fromcontent`). The Scala readers/`rst_fromfile` here serve non-Volume paths. */
    private def withFileSystem[T](path: Path, hconf: SerializableConfiguration)(body: FileSystem => T): T = {
        body(path.getFileSystem(hconf.value))
    }

    /** Reads the bytes at `rawPath` through the Hadoop FileSystem (local / DBFS / Workspace).
      * Not for UC Volume (`/Volumes/...`) paths from the JVM — see `withFileSystem` (issue #34). */
    def readBytes(rawPath: String, hConf: SerializableConfiguration): Array[Byte] = {
        val p = new Path(cleanPath(rawPath))
        withFileSystem(p, hConf) { fs => readContent(fs, fs.getFileStatus(p)) }
    }

    /** Deletes the path recursively if it exists. */
    def deleteIfExists(tmpPath: String, hconf: SerializableConfiguration): Unit = {
        val cleanPath = HadoopUtils.cleanPath(tmpPath)
        val path = new Path(cleanPath)
        withFileSystem(path, hconf) { fs =>
            if (fs.exists(path)) {
                fs.delete(path, true)
            }
        }
    }

    /** Returns total size in bytes (file length or directory content summary). */
    def getSize(path: String, hConf: SerializableConfiguration): Long = {
        val cleanPath = new Path(HadoopUtils.cleanPath(path))
        withFileSystem(cleanPath, hConf) { fs =>
            val status = fs.getFileStatus(cleanPath)
            if (status.isDirectory) {
                fs.getContentSummary(cleanPath).getLength
            } else {
                status.getLen
            }
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
