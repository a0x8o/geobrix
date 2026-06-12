"""Light vector readers (*_gbx) — pyogrio-backed PySpark DataSource V2, emitting
the same schema as the heavyweight Scala OGR readers (geom_j WKB + srid + proj4 +
typed attributes). Pure-Python / Serverless-safe (no JVM)."""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
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
