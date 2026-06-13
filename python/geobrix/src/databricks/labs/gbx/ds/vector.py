"""Light vector readers (*_gbx) — pyogrio-backed PySpark DataSource V2, emitting
the same schema as the heavyweight Scala OGR readers (geom_j WKB + srid + proj4 +
typed attributes). Pure-Python / Serverless-safe (no JVM)."""

from __future__ import annotations

import contextlib
import os
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceWriter,
    InputPartition,
    WriterCommitMessage,
)
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# OGR field type (+ subtype) -> Spark type, matching heavy OGR_SchemaInference.getType.
_OGR_TO_SPARK = {
    "OFTInteger": IntegerType,
    "OFTInteger64": LongType,
    "OFTReal": DoubleType,
    "OFTString": StringType,
    "OFTWideString": StringType,
    "OFTDate": DateType,
    "OFTTime": TimestampType,
    "OFTDateTime": TimestampType,
    "OFTBinary": BinaryType,
}
_OGR_LIST_TO_SPARK = {
    "OFTIntegerList": IntegerType,
    "OFTRealList": DoubleType,
    "OFTStringList": StringType,
    "OFTWideStringList": StringType,
}


def _arrow_to_spark(at):
    """Map a pyarrow attribute-column type to the Spark type the reader declares.
    Mirrors _ogr_to_spark's targets so the per-partition output schema (derived from
    the pyogrio Arrow table) matches schema()/_vector_schema for the same source."""
    import pyarrow as pa

    if pa.types.is_boolean(at):
        return BooleanType()
    if pa.types.is_int32(at) or (pa.types.is_integer(at) and at.bit_width <= 32):
        return IntegerType()
    if pa.types.is_integer(at):  # int64 / uint
        return LongType()
    if pa.types.is_floating(at):
        return DoubleType()
    if pa.types.is_date(at):
        return DateType()
    if pa.types.is_timestamp(at):
        return TimestampType()
    if pa.types.is_binary(at) or pa.types.is_large_binary(at):
        return BinaryType()
    if pa.types.is_list(at) or pa.types.is_large_list(at):
        return ArrayType(_arrow_to_spark(at.value_type))
    return StringType()  # string / large_string / anything else


def _ogr_to_spark(ogr_type: str, subtype: str):
    if subtype == "OFSTBoolean":
        return BooleanType()
    if ogr_type in _OGR_LIST_TO_SPARK:
        return ArrayType(_OGR_LIST_TO_SPARK[ogr_type]())
    return _OGR_TO_SPARK.get(ogr_type, StringType)()


def _geom_name(info: Dict) -> str:
    # Heavy uses the OGR geom field name if present, else geom_0 (single-geom v1).
    return info.get("geometry_name") or "geom_0"


def _vector_schema(info: Dict, as_wkb: bool) -> StructType:
    fields: List[StructField] = []
    names = list(info.get("fields", []))
    ogr_types = list(info.get("ogr_types", []))
    subtypes = list(info.get("ogr_subtypes", []))
    for j, name in enumerate(names):
        col = name if name else f"field_{j}"
        ot = ogr_types[j] if j < len(ogr_types) else "OFTString"
        st = subtypes[j] if j < len(subtypes) else "OFSTNone"
        fields.append(StructField(col, _ogr_to_spark(ot, st), True))
    gname = _geom_name(info)
    geom_type = BinaryType() if as_wkb else StringType()
    fields.append(StructField(gname, geom_type, True))
    fields.append(StructField(gname + "_srid", StringType(), True))
    fields.append(StructField(gname + "_srid_proj", StringType(), True))
    return StructType(fields)


def _crs_to_srid_proj(crs) -> Tuple[str, str]:
    """(authority code string e.g. '4326' or '0', PROJ4 string or '')."""
    if not crs:
        return "0", ""
    try:
        from pyproj import CRS

        c = CRS.from_user_input(crs)
        auth = c.to_authority()
        srid = auth[1] if auth else "0"
        try:
            proj4 = c.to_proj4() or ""
        except Exception:
            proj4 = ""
        return srid, proj4
    except Exception:
        return "0", ""


def _zip_vsi(path: str) -> str:
    """Map a zipped vector source to a GDAL /vsizip/ path."""
    if path.lower().endswith(".zip"):
        return "/vsizip/" + path
    return path


def _geometry_type_of(wkb: bytes) -> str:
    """OGR geometry-type name (e.g. 'Point', 'MultiPolygon') from a WKB blob."""
    from shapely import from_wkb

    return from_wkb(bytes(wkb)).geom_type


def _srid_to_crs(srid: str, proj4: str):
    """Inverse of the reader's CRS encoding: authority code -> 'EPSG:<code>',
    else the PROJ4 string, else None (CRS-less)."""
    if srid and srid != "0":
        return f"EPSG:{srid}"
    if proj4:
        return proj4
    return None


def _writer_col_roles(schema):
    """(geom_col, srid_col, proj_col, attr_cols) derived from the reader schema:
    the column X paired with X_srid is the geometry; X_srid_proj is its proj4;
    everything else is an attribute. Mirrors how the parity test finds geom."""
    names = [f.name for f in schema.fields]
    srid_cols = [n for n in names if n.endswith("_srid")]
    if not srid_cols:
        raise ValueError(
            "vector writer input needs a geometry/'*_srid' column pair "
            f"(from a *_gbx reader); got columns {names}"
        )
    srid_col = srid_cols[0]
    geom_col = srid_col[: -len("_srid")]
    proj_col = geom_col + "_srid_proj"
    if geom_col not in names:
        raise ValueError(f"no geometry column '{geom_col}' for srid '{srid_col}'")
    attr_cols = [n for n in names if n not in (geom_col, srid_col, proj_col)]
    return geom_col, srid_col, proj_col, attr_cols


class _ChunkPartition(InputPartition):
    """One contiguous feature slice of one layer (picklable)."""

    def __init__(self, path, driver, layer, as_wkb, skip, count):
        self.path = path
        self.driver = driver
        self.layer = layer
        self.as_wkb = as_wkb
        self.skip = skip
        self.count = count


class VectorGbxReader(DataSourceReader):
    _DRIVER = ""  # named subclasses override

    # Extensions (lower-case) recognised per OGR driver name.  A .gdb directory
    # is always treated as a single FileGDB dataset regardless of the driver.
    # OpenFileGDB includes .gdb.zip and .zip so a directory of copy_*.gdb.zip files
    # is enumerated by _members(); _zip_vsi() prefixes /vsizip/ for .zip paths so
    # each archive is opened correctly by pyogrio / GDAL.
    _EXT_FOR_DRIVER: Dict[str, Tuple[str, ...]] = {
        "GeoJSON": (".geojson", ".json"),
        "GeoJSONSeq": (".geojsonl", ".geojsons"),
        "ESRI Shapefile": (".shp", ".shz", ".zip"),
        "GPKG": (".gpkg",),
        "OpenFileGDB": (".gdb", ".gdb.zip", ".zip"),
    }

    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("vector_gbx requires a 'path' (e.g. .load(path)).")
        self.driver = options.get("driverName", "") or self._DRIVER
        # `multi=true` reads a DIRECTORY of newline-delimited GeoJSONL shards (the
        # output of geojsonl_gbx): switch a GeoJSON reader to the GeoJSONSeq driver so
        # _members() enumerates .geojsonl/.geojsons and each shard is parsed as a
        # one-Feature-per-line sequence rather than a FeatureCollection.
        if options.get("multi", "false").lower() == "true" and self.driver == "GeoJSON":
            self.driver = "GeoJSONSeq"
        self.as_wkb = options.get("asWKB", "true").lower() != "false"
        self.chunk_size = max(1, int(options.get("chunkSize", "10000")))
        self.layer_number = int(options.get("layerNumber", "0"))
        self.layer_name = options.get("layerName", "")

    def _layer(self):
        return self.layer_name if self.layer_name else self.layer_number

    @staticmethod
    def _gdal_readonly_safe() -> None:
        # Reading a GeoPackage (SQLite) from read-only object storage (a Volume) must
        # not attempt a journal/checkpoint write. DELETE journal mode avoids that.
        import pyogrio

        pyogrio.set_gdal_config_options({"OGR_SQLITE_JOURNAL": "DELETE"})

    def _members(self) -> List[str]:
        """Member paths to read. For a plain directory, enumerate matching vector
        files (by driver extension). A .gdb directory is a single FileGDB dataset
        and is returned as-is. A regular file path returns [self.path]."""
        if not os.path.isdir(self.path) or self.path.lower().rstrip("/").endswith(
            ".gdb"
        ):
            return [self.path]
        exts: Tuple[str, ...] = self._EXT_FOR_DRIVER.get(self.driver) or ()
        names = sorted(os.listdir(self.path))
        members = [
            os.path.join(self.path, n)
            for n in names
            if (exts and n.lower().endswith(exts))
            or n.lower().rstrip("/").endswith(".gdb")
        ]
        return members or [self.path]

    def _needs_stage(self) -> bool:
        """Random-access formats (GeoPackage = SQLite; FileGDB = seeked multi-file).
        Reading these directly from object storage (a UC Volume) does poorly -- FUSE
        does not serve random/seeked I/O well -- or triggers a read-only write attempt.
        Stage the source to worker-local temp first and read it there."""
        return self.driver in ("GPKG", "OpenFileGDB")

    @contextlib.contextmanager
    def _staged(self, path: str):
        """Yield a locally-readable path. For random-access drivers, copy the source
        (a file, or a `.gdb` directory) to worker-local temp with a sequential copy
        (FUSE-safe), so GDAL does its seeked I/O on local disk, then clean up -- all
        transparent to the caller. Sequential drivers (GeoJSON/Shapefile) read in place.
        """
        if not self._needs_stage():
            yield path
            return
        tmpd = tempfile.mkdtemp(prefix="gbx_vecstage_")
        try:
            local = os.path.join(tmpd, os.path.basename(path.rstrip("/")))
            if os.path.isdir(path):
                shutil.copytree(path, local)
            else:
                shutil.copy(
                    path, local
                )  # sequential read of object storage -> FUSE-safe
            yield local
        finally:
            shutil.rmtree(tmpd, ignore_errors=True)

    def _info_for(self, path: str):
        """Read pyogrio metadata for the given path. Random-access formats are staged
        to local temp first (see _staged) so the open does not seek over a Volume."""
        self._gdal_readonly_safe()
        import pyogrio

        kw: Dict = {"layer": self._layer()}
        if self.driver:
            kw["driver"] = self.driver
        with self._staged(path) as _p:
            return pyogrio.read_info(_zip_vsi(_p), **kw)

    def _info(self):
        return self._info_for(self._members()[0])

    def schema(self) -> StructType:
        first = self._members()[0]
        return _vector_schema(self._info_for(first), self.as_wkb)

    def partitions(self) -> Sequence[InputPartition]:
        # ONE partition per member file, read whole (count=0 = all features). Splitting a
        # single file into feature-offset chunks is counterproductive for these formats:
        # OGR's GeoJSON driver re-parses the ENTIRE FeatureCollection into memory on every
        # open, so N chunks = N full parses (~O(features * chunks) -- this was the dominant
        # cost). Parallelism comes from reading many files concurrently (one task per file);
        # within a single read, chunk_size only bounds the Arrow batch size on the yield, not
        # the parse. Random-access formats (GPKG/FileGDB) are staged to local temp + read whole.
        return [
            _ChunkPartition(member, self.driver, self._layer(), self.as_wkb, 0, 0)
            for member in self._members()
        ]

    def read(self, partition: "_ChunkPartition"):
        """Arrow-native read: transform the pyogrio Arrow table in Arrow (rename the
        geometry column, vectorized WKB/WKT, constant srid/proj columns, cast to the
        declared StructType) and yield pyarrow.RecordBatch objects. No per-row Python
        tuple construction -- that per-row loop made large reads ~10x slower than the
        JVM reader."""
        import numpy as np
        import pyarrow as pa
        import shapely
        from pyspark.sql.pandas.types import to_arrow_schema

        self._gdal_readonly_safe()
        import pyogrio

        kw: Dict = {
            "layer": partition.layer,
            "skip_features": partition.skip,
            "read_geometry": True,
            "datetime_as_string": False,
        }
        # One partition per file reads the whole member (count==0); max_features is left
        # unset so OGR parses the file once. (A non-zero count would cap features, but the
        # planner no longer splits a file into offset chunks -- see partitions().)
        if partition.count:
            kw["max_features"] = partition.count
        if partition.driver:
            kw["driver"] = partition.driver
        with self._staged(partition.path) as _p:
            meta, tbl = pyogrio.read_arrow(_zip_vsi(_p), **kw)

        # Arrow table uses 'wkb_geometry' when geometry_name is empty; the declared
        # schema names the geometry column geometry_name or 'geom_0' (see _geom_name).
        gcol = meta.get("geometry_name") or "wkb_geometry"
        out_gname = meta.get("geometry_name") or "geom_0"
        srid, proj4 = _crs_to_srid_proj(meta.get("crs"))
        n = tbl.num_rows

        # Build the declared output Spark schema from THIS table's field types (so we do
        # not re-open the file just to compute it): attrs preserved, geom typed by asWKB,
        # srid/proj string columns -- column NAMES/ORDER match _vector_schema/schema().
        attr_cols = [c for c in tbl.column_names if c != gcol]
        fields: List[StructField] = [
            StructField(c, _arrow_to_spark(tbl.schema.field(c).type), True)
            for c in attr_cols
        ]
        geom_spark = BinaryType() if partition.as_wkb else StringType()
        fields.append(StructField(out_gname, geom_spark, True))
        fields.append(StructField(out_gname + "_srid", StringType(), True))
        fields.append(StructField(out_gname + "_srid_proj", StringType(), True))
        target = to_arrow_schema(StructType(fields))

        if n == 0:
            for batch in target.empty_table().to_batches():
                yield batch
            return

        # Geometry column: keep WKB binary, or vectorized WKB->WKT (shapely 2.x).
        wkb_arr = tbl.column(gcol).combine_chunks()
        if partition.as_wkb:
            geom_out = wkb_arr.cast(pa.binary())
        else:
            geoms = shapely.from_wkb(wkb_arr.to_numpy(zero_copy_only=False))
            wkt = shapely.to_wkt(geoms)  # None -> None preserved
            geom_out = pa.array(np.asarray(wkt, dtype=object), type=pa.string())

        srid_out = pa.array([srid] * n, type=pa.string())
        proj_out = pa.array([proj4] * n, type=pa.string())

        out_cols = [tbl.column(c) for c in attr_cols] + [geom_out, srid_out, proj_out]
        out_names = attr_cols + [
            out_gname,
            out_gname + "_srid",
            out_gname + "_srid_proj",
        ]
        out_table = pa.Table.from_arrays(
            [
                c.combine_chunks() if isinstance(c, pa.ChunkedArray) else c
                for c in out_cols
            ],
            names=out_names,
        )
        # Cast to the declared schema's Arrow types for guaranteed PySpark alignment.
        out = out_table.cast(target, safe=False)
        # Yield in chunk_size-bounded batches: the file is parsed once (above); chunk_size
        # only bounds the per-batch row count handed to Spark (memory), not the parse.
        for batch in out.to_batches(max_chunksize=self.chunk_size):
            yield batch


class VectorGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "vector_gbx"

    _READER = VectorGbxReader

    def schema(self) -> StructType:
        return self._READER(self.options).schema()

    def reader(self, schema: StructType) -> DataSourceReader:
        return self._READER(self.options)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        path = self.options.get("path")
        if not path:
            raise ValueError("vector_gbx writer requires an output path (.save(path)).")
        return VectorGbxWriter(
            path, schema, self._READER._DRIVER, dict(self.options), overwrite
        )


class _ShapefileReader(VectorGbxReader):
    _DRIVER = "ESRI Shapefile"


class _GeoJSONReader(VectorGbxReader):
    _DRIVER = "GeoJSON"


class _GpkgReader(VectorGbxReader):
    _DRIVER = "GPKG"


class _FileGdbReader(VectorGbxReader):
    _DRIVER = "OpenFileGDB"


class ShapefileGbxDataSource(VectorGbxDataSource):
    _READER = _ShapefileReader

    @classmethod
    def name(cls) -> str:
        return "shapefile_gbx"


class GeoJSONGbxDataSource(VectorGbxDataSource):
    _READER = _GeoJSONReader

    @classmethod
    def name(cls) -> str:
        return "geojson_gbx"


class GpkgGbxDataSource(VectorGbxDataSource):
    _READER = _GpkgReader

    @classmethod
    def name(cls) -> str:
        return "gpkg_gbx"


class FileGdbGbxDataSource(VectorGbxDataSource):
    _READER = _FileGdbReader

    @classmethod
    def name(cls) -> str:
        return "file_gdb_gbx"


@dataclass
class _VectorCommitMessage(WriterCommitMessage):
    frag_path: str


class VectorGbxWriter(DataSourceWriter):
    """Two-phase vector writer: each partition -> one Arrow-IPC fragment in a
    shared-FS scratch dir; the driver merges fragments into one output file via
    pyogrio.write_arrow (first plain, rest append=True). Mirrors the PMTiles
    writer's executor-scratch / driver-merge shape."""

    def __init__(self, path, schema, driver, options, overwrite):
        opts = {k.lower(): v for k, v in options.items()}
        self.path = path
        self.driver = opts.get("drivername", "") or driver
        if not self.driver:
            raise ValueError(
                "vector_gbx writer requires a 'driverName' option (e.g. 'GeoJSON')."
            )
        self.overwrite = overwrite
        self.geometry_type_override = opts.get("geometrytype")
        self.layer_name = opts.get("layername")
        self.geom_col, self.srid_col, self.proj_col, self.attr_cols = _writer_col_roles(
            schema
        )
        self._col_order = [f.name for f in schema.fields]
        self._geom_is_wkb = any(
            f.name == self.geom_col and isinstance(f.dataType, BinaryType)
            for f in schema.fields
        )
        parent = os.path.dirname(self.path) or "."
        self.scratch_dir = os.path.join(parent, "_vec_scratch")
        if not self.overwrite and self._target_exists():
            raise ValueError(
                "vector_gbx does not support append; use .mode('overwrite')."
            )

    def _target_exists(self) -> bool:
        return os.path.exists(self.path) and (
            os.path.isfile(self.path) or bool(os.listdir(self.path))
        )

    # ---- executor: partition rows -> one Arrow-IPC fragment ----
    def write(self, iterator: Iterator) -> WriterCommitMessage:
        import pyarrow as pa
        import pyarrow.feather as feather
        from shapely import from_wkt, to_wkb

        idx = {n: i for i, n in enumerate(self._col_order)}
        cols: Dict[str, list] = {n: [] for n in self._col_order}
        for row in iterator:
            for n in self._col_order:
                v = row[idx[n]]
                if n == self.geom_col and v is not None and not self._geom_is_wkb:
                    v = to_wkb(from_wkt(v))  # WKT input -> WKB
                elif n == self.geom_col and v is not None:
                    v = bytes(v)
                cols[n].append(v)
        if not cols[self.geom_col]:
            return _VectorCommitMessage(frag_path="")  # empty partition
        os.makedirs(self.scratch_dir, exist_ok=True)
        tbl = pa.table({n: cols[n] for n in self._col_order})
        frag = os.path.join(self.scratch_dir, f"frag-{uuid.uuid4().hex}.arrow")
        feather.write_feather(tbl, frag)
        return _VectorCommitMessage(frag_path=frag)

    # ---- driver: merge fragments into one output file ----
    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        import pyarrow.feather as feather

        frags = [
            m.frag_path
            for m in messages
            if isinstance(m, _VectorCommitMessage) and m.frag_path
        ]
        local_dir = None
        try:
            if not frags:
                return
            import pyarrow as pa

            tables = [feather.read_table(f) for f in frags]
            # Merge all partition fragments into ONE Arrow table so the local write is a
            # single pass. pyogrio's append path (append=True per fragment) re-encodes the
            # growing file for some drivers -- GeoJSON especially -- which is ~quadratic in
            # fragment count. One concatenated write keeps single-file export fast (and is
            # why no coalesce is needed: the writer always emits a single merged file).
            if len(tables) > 1:
                tables = [pa.concat_tables(tables)]
            geom_type, crs = self._infer_geom_crs(tables)
            # Write to driver-local disk first (supports random I/O for SQLite/
            # FileGDB/Shapefile sidecars), then copy to the Volume target with
            # sequential byte copies (FUSE-safe). Mirrors the PMTiles writer.
            local_dir = tempfile.mkdtemp(prefix="gbx_vecout_")
            local_out = os.path.join(local_dir, os.path.basename(self.path.rstrip("/")))
            self._write_local(tables, local_out, geom_type, crs)
            # Clear any Spark-created stub at self.path, then copy everything
            # pyogrio produced in local_dir (file, sidecar set, or .gdb dir).
            self._prepare_target()
            parent = os.path.dirname(self.path) or "."
            os.makedirs(parent, exist_ok=True)
            for name in os.listdir(local_dir):
                if name.endswith(("-wal", "-shm", "-journal")):
                    continue  # transient SQLite sidecars -- never publish
                src = os.path.join(local_dir, name)
                dst = os.path.join(parent, name)
                if os.path.isdir(src):
                    shutil.copytree(src, dst, dirs_exist_ok=True)
                else:
                    shutil.copy(src, dst)  # sequential -> FUSE-safe
        finally:
            if local_dir is not None:
                shutil.rmtree(local_dir, ignore_errors=True)
            shutil.rmtree(self.scratch_dir, ignore_errors=True)

    def _drop_meta_cols(self, tbl):
        return tbl.drop_columns(
            [c for c in (self.srid_col, self.proj_col) if c in tbl.column_names]
        )

    def _write_local(self, tables, local_out, geom_type, crs) -> None:
        """Write the merged tables to a local path. Use the fast Arrow path; if the
        driver lacks Arrow-write support (e.g. OpenFileGDB), fall back to the classic
        feature-based path, which has broader OGR driver support."""
        if self.driver == "OpenFileGDB":
            self._write_local_osgeo_gdb(tables, local_out, geom_type, crs)
            return
        import pyogrio

        # Write SQLite-backed formats (GPKG) with a DELETE journal -- no WAL/-shm
        # sidecars -- so the output reads back from read-only object storage (a
        # Volume) without the reader attempting a checkpoint (write). Harmless for
        # non-SQLite drivers. (GDAL config is process-global; set per write.)
        pyogrio.set_gdal_config_options({"OGR_SQLITE_JOURNAL": "DELETE"})
        kw = dict(
            driver=self.driver,
            geometry_name=self.geom_col,
            geometry_type=geom_type,
            crs=crs,
        )
        if self.layer_name:
            kw["layer"] = self.layer_name
        try:
            for n, tbl in enumerate(tables):
                pyogrio.write_arrow(
                    self._drop_meta_cols(tbl), local_out, append=(n > 0), **kw
                )
        except Exception as e:  # noqa: BLE001
            if "does not support write functionality" not in str(e):
                raise
            # Arrow-write path unsupported for this driver -> classic path. Start
            # from a clean local_out so a partial Arrow attempt doesn't corrupt it.
            if os.path.isdir(local_out):
                shutil.rmtree(local_out, ignore_errors=True)
            elif os.path.isfile(local_out):
                os.remove(local_out)
            self._write_local_classic(tables, local_out, geom_type, crs)

    def _write_local_osgeo_gdb(self, tables, local_out, geom_type, crs) -> None:
        """Hybrid FileGDB path. pyogrio's bundled GDAL has a read-only OpenFileGDB
        driver; the native GDAL from the heavyweight GDAL init script has write. Use
        osgeo.ogr (native) to encode the .gdb. Requires those natives -- raises a clear
        error otherwise (FileGDB write is unavailable in a lightweight-only env)."""
        try:
            from osgeo import ogr, osr
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                "file_gdb_gbx writing requires the native GDAL Python bindings (osgeo) "
                "from the heavyweight GDAL init script; pyogrio's bundled GDAL ships a "
                "read-only OpenFileGDB driver. Install the GeoBrix GDAL natives, or write "
                "gpkg_gbx / geojson_gbx instead."
            ) from e
        import pyarrow as pa

        drv = ogr.GetDriverByName("OpenFileGDB")
        if drv is None or not drv.TestCapability(ogr.ODrCCreateDataSource):
            raise RuntimeError(
                "native GDAL OpenFileGDB driver lacks create capability; FileGDB write "
                "needs GDAL >= 3.6 with OpenFileGDB write (the heavyweight GDAL natives)."
            )
        _WKB = {
            "Point": ogr.wkbPoint,
            "LineString": ogr.wkbLineString,
            "Polygon": ogr.wkbPolygon,
            "MultiPoint": ogr.wkbMultiPoint,
            "MultiLineString": ogr.wkbMultiLineString,
            "MultiPolygon": ogr.wkbMultiPolygon,
            "GeometryCollection": ogr.wkbGeometryCollection,
        }
        srs = None
        if crs:
            srs = osr.SpatialReference()
            if str(crs).upper().startswith("EPSG:"):
                srs.ImportFromEPSG(int(str(crs).split(":")[1]))
            else:
                srs.ImportFromProj4(str(crs))

        def _ogr_type(t):
            if pa.types.is_floating(t):
                return ogr.OFTReal
            if pa.types.is_boolean(t):
                return ogr.OFTInteger
            if pa.types.is_integer(t):
                return ogr.OFTInteger64
            if pa.types.is_binary(t) or pa.types.is_large_binary(t):
                return ogr.OFTBinary
            return ogr.OFTString  # strings + anything else

        first = tables[0]
        meta = {self.geom_col, self.srid_col, self.proj_col}
        attr_cols = [c for c in first.column_names if c not in meta]
        types = {f.name: f.type for f in first.schema}

        ds = drv.CreateDataSource(local_out)
        if ds is None:
            raise RuntimeError(
                f"OpenFileGDB CreateDataSource returned None for {local_out!r}; "
                "the FileGDB output path must end in '.gdb'."
            )
        try:
            lyr = ds.CreateLayer(
                self.layer_name or "layer", srs, _WKB.get(geom_type, ogr.wkbUnknown)
            )
            for c in attr_cols:
                lyr.CreateField(ogr.FieldDefn(c, _ogr_type(types[c])))
            defn = lyr.GetLayerDefn()
            for tbl in tables:
                cols = {c: tbl.column(c).to_pylist() for c in tbl.column_names}
                geom = cols.get(self.geom_col, [None] * tbl.num_rows)
                for i in range(tbl.num_rows):
                    feat = ogr.Feature(defn)
                    for c in attr_cols:
                        v = cols[c][i]
                        if v is not None:
                            feat.SetField(c, v)
                    g = geom[i]
                    if g is not None:
                        feat.SetGeometry(ogr.CreateGeometryFromWkb(bytes(g)))
                    lyr.CreateFeature(feat)
                    feat = None
        finally:
            ds = None  # flush + close

    def _write_local_classic(self, tables, local_out, geom_type, crs) -> None:
        import numpy as np
        import pyogrio.raw

        kw = dict(driver=self.driver, geometry_type=geom_type, crs=crs)
        if self.layer_name:
            kw["layer"] = self.layer_name
        meta = {self.geom_col, self.srid_col, self.proj_col}
        for n, tbl in enumerate(tables):
            attr_cols = [c for c in tbl.column_names if c not in meta]
            geometry = np.array(tbl.column(self.geom_col).to_pylist(), dtype=object)
            field_data = [np.array(tbl.column(c).to_pylist()) for c in attr_cols]
            pyogrio.raw.write(
                local_out,
                geometry=geometry,
                field_data=field_data,
                fields=np.array(attr_cols, dtype=object),
                append=(n > 0),
                **kw,
            )

    def _infer_geom_crs(self, tables) -> Tuple[str, Optional[str]]:
        geom_type, crs = self.geometry_type_override, None
        for tbl in tables:
            g = tbl.column(self.geom_col).to_pylist()
            s = (
                tbl.column(self.srid_col).to_pylist()
                if self.srid_col in tbl.column_names
                else []
            )
            p = (
                tbl.column(self.proj_col).to_pylist()
                if self.proj_col in tbl.column_names
                else []
            )
            for i, gv in enumerate(g):
                if gv is None:
                    continue
                if geom_type is None:
                    geom_type = _geometry_type_of(gv)
                if crs is None:
                    crs = _srid_to_crs(
                        s[i] if i < len(s) else "", p[i] if i < len(p) else ""
                    )
                break
            if geom_type is not None and crs is not None:
                break
        return geom_type or "Unknown", crs

    def _prepare_target(self) -> None:
        # PySpark may pre-create self.path as a directory; vector output is a
        # single file (or driver-managed dir). Clear it and write directly --
        # no os.rename (FUSE-unsafe on DBFS/Volumes); write_arrow writes
        # sequentially so a direct write to a FUSE path is safe.
        parent = os.path.dirname(self.path) or "."
        os.makedirs(parent, exist_ok=True)
        if os.path.isdir(self.path):
            shutil.rmtree(self.path)
        elif os.path.isfile(self.path):
            os.remove(self.path)

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        shutil.rmtree(self.scratch_dir, ignore_errors=True)
        if os.path.isfile(self.path):
            os.remove(self.path)
        elif os.path.isdir(self.path):
            shutil.rmtree(self.path, ignore_errors=True)


@dataclass
class _GeoJSONLCommitMessage(WriterCommitMessage):
    shard_paths: Tuple[str, ...]


class GeoJSONLGbxWriter(DataSourceWriter):
    """Multi-file GeoJSONL writer: each partition writes one (or, with
    maxRecordsPerFile, several) newline-delimited GeoJSONL shard(s) directly into
    the output DIRECTORY -- NO driver merge. The natural shape for JSONL: shards
    are splittable + concatenable, so write throughput scales with partitions
    instead of bottlenecking on a single-node assembly. Contrast geojson_gbx,
    which merges all partitions into one FeatureCollection file on the driver.

    Each shard is encoded to worker-local temp via pyogrio (driver GeoJSONSeq),
    then sequentially copied to OUTDIR/part-<uuid>.geojsonl (FUSE-safe on Volumes).
    overwrite clears the target directory once before the executors write; append
    is rejected (matching the other vector writers for v0.4.0)."""

    _DRIVER = "GeoJSONSeq"
    _EXT = ".geojsonl"

    def __init__(self, path, schema, options, overwrite):
        opts = {k.lower(): v for k, v in options.items()}
        self.path = path
        self.overwrite = overwrite
        self.geometry_type_override = opts.get("geometrytype")
        self.layer_name = opts.get("layername")
        mrpf = opts.get("maxrecordsperfile")
        self.max_records_per_file = int(mrpf) if mrpf else 0
        if self.max_records_per_file < 0:
            raise ValueError("maxRecordsPerFile must be a non-negative integer.")
        self.geom_col, self.srid_col, self.proj_col, self.attr_cols = _writer_col_roles(
            schema
        )
        self._col_order = [f.name for f in schema.fields]
        self._geom_is_wkb = any(
            f.name == self.geom_col and isinstance(f.dataType, BinaryType)
            for f in schema.fields
        )
        if not self.overwrite and self._target_exists():
            raise ValueError(
                "geojsonl_gbx does not support append; use .mode('overwrite')."
            )
        # Clear the target directory ONCE, on the driver, before executors write
        # (the writer is constructed once on the driver). Sequential rmtree -- no
        # rename -- so it is FUSE-safe on DBFS/Volumes.
        if os.path.isfile(self.path):
            os.remove(self.path)
        elif os.path.isdir(self.path):
            shutil.rmtree(self.path, ignore_errors=True)
        os.makedirs(self.path, exist_ok=True)

    def _target_exists(self) -> bool:
        return os.path.exists(self.path) and (
            os.path.isfile(self.path) or bool(os.listdir(self.path))
        )

    def _drop_meta_cols(self, tbl):
        return tbl.drop_columns(
            [c for c in (self.srid_col, self.proj_col) if c in tbl.column_names]
        )

    def _infer_geom_crs(self, tbl) -> Tuple[str, Optional[str]]:
        geom_type, crs = self.geometry_type_override, None
        g = tbl.column(self.geom_col).to_pylist()
        s = (
            tbl.column(self.srid_col).to_pylist()
            if self.srid_col in tbl.column_names
            else []
        )
        p = (
            tbl.column(self.proj_col).to_pylist()
            if self.proj_col in tbl.column_names
            else []
        )
        for i, gv in enumerate(g):
            if gv is None:
                continue
            if geom_type is None:
                geom_type = _geometry_type_of(gv)
            if crs is None:
                crs = _srid_to_crs(
                    s[i] if i < len(s) else "", p[i] if i < len(p) else ""
                )
            break
        return geom_type or "Unknown", crs

    # ---- executor: partition rows -> one or more GeoJSONL shards in OUTDIR ----
    def write(self, iterator: Iterator) -> WriterCommitMessage:
        import pyarrow as pa
        import pyogrio
        from shapely import from_wkt, to_wkb

        idx = {n: i for i, n in enumerate(self._col_order)}
        cols: Dict[str, list] = {n: [] for n in self._col_order}
        for row in iterator:
            for n in self._col_order:
                v = row[idx[n]]
                if n == self.geom_col and v is not None and not self._geom_is_wkb:
                    v = to_wkb(from_wkt(v))  # WKT input -> WKB
                elif n == self.geom_col and v is not None:
                    v = bytes(v)
                cols[n].append(v)
        nrows = len(cols[self.geom_col])
        if nrows == 0:
            return _GeoJSONLCommitMessage(shard_paths=())  # empty partition -> nothing

        tbl = pa.table({n: cols[n] for n in self._col_order})
        geom_type, crs = self._infer_geom_crs(tbl)
        chunk = self.max_records_per_file or nrows  # split into N-row shards if set
        bounds = list(range(0, nrows, chunk))

        os.makedirs(self.path, exist_ok=True)
        local_dir = tempfile.mkdtemp(prefix="gbx_geojsonl_")
        written: List[str] = []
        try:
            for start in bounds:
                slice_tbl = self._drop_meta_cols(tbl.slice(start, chunk))
                # Per-shard uuid -> unique across tasks AND across chunks of one task.
                name = f"part-{uuid.uuid4().hex}{self._EXT}"
                local_path = os.path.join(local_dir, name)
                kw = dict(
                    driver=self._DRIVER,
                    geometry_name=self.geom_col,
                    geometry_type=geom_type,
                    crs=crs,
                )
                if self.layer_name:
                    kw["layer"] = self.layer_name
                pyogrio.write_arrow(slice_tbl, local_path, **kw)
                dst = os.path.join(self.path, name)
                shutil.copy(local_path, dst)  # sequential -> FUSE-safe
                written.append(dst)
        finally:
            shutil.rmtree(local_dir, ignore_errors=True)
        return _GeoJSONLCommitMessage(shard_paths=tuple(written))

    # ---- driver: NO merge; just finalize (optional _SUCCESS marker) ----
    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        os.makedirs(self.path, exist_ok=True)
        try:
            with open(os.path.join(self.path, "_SUCCESS"), "w") as fh:
                fh.write("")
        except OSError:
            pass  # marker is advisory; never fail the commit on it

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        # Best-effort cleanup of any shards that did land.
        for m in messages:
            if isinstance(m, _GeoJSONLCommitMessage):
                for shard in m.shard_paths:
                    with contextlib.suppress(OSError):
                        if os.path.isfile(shard):
                            os.remove(shard)


class GeoJSONLGbxDataSource(DataSource):
    """`geojsonl_gbx` -- multi-file GeoJSONL writer (directory of shards). It also
    reads back via the geojson_gbx reader with option('multi','true')."""

    @classmethod
    def name(cls) -> str:
        return "geojsonl_gbx"

    def schema(self) -> StructType:
        # Reading is done via geojson_gbx(multi=true); expose the GeoJSONSeq reader
        # for symmetry so .load() on a shard directory works through this name too.
        opts = dict(self.options)
        opts["driverName"] = opts.get("driverName", "") or "GeoJSONSeq"
        return VectorGbxReader(opts).schema()

    def reader(self, schema: StructType) -> DataSourceReader:
        opts = dict(self.options)
        opts["driverName"] = opts.get("driverName", "") or "GeoJSONSeq"
        return VectorGbxReader(opts)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        path = self.options.get("path")
        if not path:
            raise ValueError(
                "geojsonl_gbx writer requires an output path (.save(path))."
            )
        return GeoJSONLGbxWriter(path, schema, dict(self.options), overwrite)
