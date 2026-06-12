"""Light vector readers (*_gbx) — pyogrio-backed PySpark DataSource V2, emitting
the same schema as the heavyweight Scala OGR readers (geom_j WKB + srid + proj4 +
typed attributes). Pure-Python / Serverless-safe (no JVM)."""

from __future__ import annotations

import os
import shutil
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


class OgrGbxReader(DataSourceReader):
    _DRIVER = ""  # named subclasses override

    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError("ogr_gbx requires a 'path' (e.g. .load(path)).")
        self.driver = options.get("driverName", "") or self._DRIVER
        self.as_wkb = options.get("asWKB", "true").lower() != "false"
        self.chunk_size = max(1, int(options.get("chunkSize", "10000")))
        self.layer_number = int(options.get("layerNumber", "0"))
        self.layer_name = options.get("layerName", "")

    def _layer(self):
        return self.layer_name if self.layer_name else self.layer_number

    def _info(self):
        import pyogrio

        kw: Dict = {"layer": self._layer()}
        if self.driver:
            kw["driver"] = self.driver
        return pyogrio.read_info(_zip_vsi(self.path), **kw)

    def schema(self) -> StructType:
        return _vector_schema(self._info(), self.as_wkb)

    def partitions(self) -> Sequence[InputPartition]:
        n = int(self._info().get("features", 0) or 0)
        parts: List[_ChunkPartition] = []
        skip = 0
        while skip < n or (n == 0 and skip == 0):
            parts.append(
                _ChunkPartition(
                    self.path,
                    self.driver,
                    self._layer(),
                    self.as_wkb,
                    skip,
                    self.chunk_size,
                )
            )
            skip += self.chunk_size
            if n == 0:
                break
        return parts

    def read(self, partition: "_ChunkPartition"):
        import pyogrio

        kw: Dict = {
            "layer": partition.layer,
            "skip_features": partition.skip,
            "max_features": partition.count,
            "read_geometry": True,
            "datetime_as_string": False,
        }
        if partition.driver:
            kw["driver"] = partition.driver
        meta, tbl = pyogrio.read_arrow(_zip_vsi(partition.path), **kw)
        # Arrow table uses 'wkb_geometry' when geometry_name is empty.
        gcol = meta.get("geometry_name") or "wkb_geometry"
        srid, proj4 = _crs_to_srid_proj(meta.get("crs"))
        attr_cols = [c for c in tbl.column_names if c != gcol]
        cols = {c: tbl.column(c).to_pylist() for c in tbl.column_names}
        geom = cols.get(gcol, [None] * tbl.num_rows)
        for i in range(tbl.num_rows):
            g = geom[i]
            if g is not None and not partition.as_wkb:
                from shapely import from_wkb as _from_wkb

                g = _from_wkb(bytes(g)).wkt
            elif g is not None:
                g = bytes(g)
            yield tuple(cols[c][i] for c in attr_cols) + (g, srid, proj4)


class OgrGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "ogr_gbx"

    _READER = OgrGbxReader

    def schema(self) -> StructType:
        return self._READER(self.options).schema()

    def reader(self, schema: StructType) -> DataSourceReader:
        return self._READER(self.options)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        path = self.options.get("path")
        if not path:
            raise ValueError("ogr_gbx writer requires an output path (.save(path)).")
        return OgrGbxWriter(
            path, schema, self._READER._DRIVER, dict(self.options), overwrite
        )


class _ShapefileReader(OgrGbxReader):
    _DRIVER = "ESRI Shapefile"


class _GeoJSONReader(OgrGbxReader):
    _DRIVER = "GeoJSON"


class _GpkgReader(OgrGbxReader):
    _DRIVER = "GPKG"


class _FileGdbReader(OgrGbxReader):
    _DRIVER = "OpenFileGDB"


class ShapefileGbxDataSource(OgrGbxDataSource):
    _READER = _ShapefileReader

    @classmethod
    def name(cls) -> str:
        return "shapefile_gbx"


class GeoJSONGbxDataSource(OgrGbxDataSource):
    _READER = _GeoJSONReader

    @classmethod
    def name(cls) -> str:
        return "geojson_gbx"


class GpkgGbxDataSource(OgrGbxDataSource):
    _READER = _GpkgReader

    @classmethod
    def name(cls) -> str:
        return "gpkg_gbx"


class FileGdbGbxDataSource(OgrGbxDataSource):
    _READER = _FileGdbReader

    @classmethod
    def name(cls) -> str:
        return "file_gdb_gbx"


@dataclass
class _VectorCommitMessage(WriterCommitMessage):
    frag_path: str


class OgrGbxWriter(DataSourceWriter):
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
                "ogr_gbx writer requires a 'driverName' option (e.g. 'GeoJSON')."
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
            raise ValueError("ogr_gbx does not support append; use .mode('overwrite').")

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
        import pyogrio

        frags = [
            m.frag_path
            for m in messages
            if isinstance(m, _VectorCommitMessage) and m.frag_path
        ]
        try:
            if not frags:
                return
            self._prepare_target()
            tables = [feather.read_table(f) for f in frags]
            geom_type, crs = self._infer_geom_crs(tables)
            kw = dict(
                driver=self.driver,
                geometry_name=self.geom_col,
                geometry_type=geom_type,
                crs=crs,
            )
            if self.layer_name:
                kw["layer"] = self.layer_name
            for n, tbl in enumerate(tables):
                out_tbl = tbl.drop_columns(
                    [c for c in (self.srid_col, self.proj_col) if c in tbl.column_names]
                )
                pyogrio.write_arrow(out_tbl, self.path, append=(n > 0), **kw)
        finally:
            shutil.rmtree(self.scratch_dir, ignore_errors=True)

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
