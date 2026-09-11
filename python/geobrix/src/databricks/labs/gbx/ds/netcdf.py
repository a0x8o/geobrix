"""netcdf_gbx — lightweight NetCDF reader.

One DataSource, two modes (the `mode` option, default "raster"):
  * raster — CF regular/projected grids -> the shared (source, tile) GeoTIFF struct.
  * vector — DSG points, or any 2-D field (incl. curvilinear swath) coerced to
    per-cell points -> the light vector schema (attrs + geom_0 WKB + srid cols).

Class 4 (raw sensor geometry + GLT) is rejected in both modes.
Serverless-safe: registers a DataSource and builds Column output only (no runtime
Spark-config mutation or JVM-bridge access).
"""

from __future__ import annotations

from typing import Dict, Iterator, List, Tuple

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType

from databricks.labs.gbx.ds import _encode, _listing, _netcdf
from databricks.labs.gbx.ds.raster import RasterGbxReader, _FilePartition, reader_schema


def _parse_dim_index(raw: str) -> Dict[str, int]:
    """Parse the ``dimIndex`` option string (e.g. ``"time=2,level=1"``) into a
    mapping of dim name → integer index.

    Raises:
        ValueError: if any entry is malformed or the index is not a valid integer.
    """
    result: Dict[str, int] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            raise ValueError(
                f"netcdf_gbx: dimIndex entry {part!r} is invalid; expected 'dim=index'."
            )
        k, _, v = part.partition("=")
        k, v = k.strip(), v.strip()
        try:
            result[k] = int(v)
        except ValueError:
            raise ValueError(
                f"netcdf_gbx: dimIndex dim {k!r}: {v!r} is not a valid integer index."
            )
    return result


def _parse_fanout(raw: str) -> List[str]:
    """Parse the ``fanout`` option string (e.g. ``"time,level"``) into a list of
    dim names.
    """
    return [s.strip() for s in raw.split(",") if s.strip()]


class NetcdfRasterReader(RasterGbxReader):
    """Raster mode: transcode each CF grid variable to a GeoTIFF tile (one row per variable)."""

    def __init__(self, options: Dict[str, str]):
        super().__init__(options)  # path/sizeInMB/filterRegex/bbox/bboxCrs
        self.options = dict(options)
        self.group = options.get("group")

    def partitions(self):
        # NetCDF raster reader emits one row PER VARIABLE per file, not per tile
        # window. The tile-window planning in RasterGbxReader.partitions() does not
        # apply here — return one legacy _FilePartition per file so read() receives
        # a file-scoped partition and can iterate over variables itself.
        files = _listing.list_files(self.path, self.filter_regex)
        return [_FilePartition(f, self.size_mib) for f in files]

    def read(self, partition: "_FilePartition") -> Iterator[Tuple]:
        from itertools import product as _product

        from rasterio.io import MemoryFile

        # Parse dimIndex and fanout options (empty string → empty result).
        dim_index = _parse_dim_index(self.options.get("dimIndex", ""))
        fanout_dims = _parse_fanout(self.options.get("fanout", ""))

        # Overlap guard: a dim cannot be both pinned and expanded.
        overlap = set(dim_index) & set(fanout_dims)
        if overlap:
            raise ValueError(
                f"netcdf_gbx: dims {sorted(overlap)!r} appear in both dimIndex and "
                "fanout; a dimension cannot be both pinned (dimIndex) and expanded "
                "(fanout)."
            )

        with _netcdf.open_dataset(partition.file_path, self.group) as ds:
            variables = _netcdf.select_variables(ds, self.options, "raster")
            for var in variables:
                transform, crs = _netcdf.grid_transform_crs(ds, var)
                nodata = _netcdf.nodata_of(ds, var)

                # Determine which fanout dims this variable actually has.
                var_ldims = dict(_netcdf.leading_dims(ds, var))
                active_fanout = [
                    (d, var_ldims[d]) for d in fanout_dims if d in var_ldims
                ]
                fanout_dim_names = [d for d, _ in active_fanout]

                # Build the Cartesian product of fanout dim ranges.
                # An empty active_fanout yields one empty tuple → one row (no expansion).
                combos: List[Tuple] = (
                    list(_product(*[range(size) for _, size in active_fanout]))
                    if active_fanout
                    else [()]
                )

                for combo in combos:
                    # Merge dimIndex + current fanout combination into sel.
                    sel: Dict[str, int] = dict(dim_index)
                    for dim_name, idx in zip(fanout_dim_names, combo):
                        sel[dim_name] = idx

                    arr = _netcdf.array_2d(ds, var, sel=sel if sel else None)

                    # Build the source string. When sel is non-empty (any option
                    # was set), append a sorted bracketed suffix to make each row
                    # unique and parseable.  With no options, source is byte-
                    # identical to the original format.
                    if sel:
                        suffix = ",".join(f"{k}={v}" for k, v in sorted(sel.items()))
                        source = f'NETCDF:"{partition.file_path}":{var}[{suffix}]'
                    else:
                        source = f'NETCDF:"{partition.file_path}":{var}'

                    h, w = arr.shape[-2], arr.shape[-1]
                    profile = dict(
                        driver="GTiff",
                        width=w,
                        height=h,
                        count=1,
                        dtype=str(arr.dtype),
                        crs=crs,
                        transform=transform,
                    )
                    if nodata is not None:
                        profile["nodata"] = nodata
                    with MemoryFile() as mf:
                        with mf.open(**profile) as out:
                            out.write(arr.astype(profile["dtype"]), 1)
                        with mf.open() as rds:
                            cellid, raster_bytes, meta = _encode.encode_tile(
                                rds,
                                window=(0, 0, w, h),
                                source_path=partition.file_path,
                                all_parents="",
                                tile_format="gtiff",
                            )

                    # Record slice selection in tile metadata so downstream can
                    # recover which time/level a tile represents.
                    if sel:
                        meta["sliceDims"] = ",".join(
                            f"{k}={v}" for k, v in sorted(sel.items())
                        )
                        for dim_name, idx in sorted(sel.items()):
                            if dim_name in ds.coords:
                                try:
                                    coord_val = float(ds.coords[dim_name].values[idx])
                                    meta[f"sliceCoord_{dim_name}"] = str(coord_val)
                                except (IndexError, TypeError, ValueError):
                                    pass

                    yield (source, (cellid, raster_bytes, meta))


class NetcdfGbxDataSource(DataSource):
    @classmethod
    def name(cls) -> str:
        return "netcdf_gbx"

    def _mode(self) -> str:
        return self.options.get("mode", "raster").lower()

    def schema(self) -> StructType:
        mode = self._mode()
        if mode == "raster":
            return reader_schema()
        if mode == "vector":
            from databricks.labs.gbx.ds._netcdf_vector import NetcdfVectorReader

            return NetcdfVectorReader(self.options).schema()
        raise ValueError(
            f"netcdf_gbx: unknown mode={mode!r} (use 'raster' or 'vector')."
        )

    def reader(self, schema: StructType) -> DataSourceReader:
        mode = self._mode()
        if mode == "raster":
            return NetcdfRasterReader(self.options)
        if mode == "vector":
            from databricks.labs.gbx.ds._netcdf_vector import NetcdfVectorReader

            return NetcdfVectorReader(self.options)
        raise ValueError(
            f"netcdf_gbx: unknown mode={mode!r} (use 'raster' or 'vector')."
        )

    def writer(self, schema: StructType, overwrite: bool):
        mode = self._mode()
        if mode == "raster":
            from databricks.labs.gbx.ds._write_netcdf import NetcdfRasterGbxWriter

            if not self.options.get("path"):
                raise ValueError(
                    "netcdf_gbx writer requires an output path (.save(path))."
                )
            return NetcdfRasterGbxWriter(self.options, schema, overwrite)
        if mode == "vector":
            from databricks.labs.gbx.ds._write_netcdf import NetcdfVectorGbxWriter

            if not self.options.get("path"):
                raise ValueError(
                    "netcdf_gbx writer requires an output path (.save(path))."
                )
            return NetcdfVectorGbxWriter(self.options, schema, overwrite)
        raise ValueError(
            f"netcdf_gbx: unknown mode={mode!r} (use 'raster' or 'vector')."
        )
