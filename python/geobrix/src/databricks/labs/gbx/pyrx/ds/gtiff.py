"""gtiff_gbx — named GeoTIFF reader. Light analogue of Scala GTiff_DataSource:
extends the catch-all and presets driver="GTiff" (the dsExtraMap mirror).
"""
from __future__ import annotations

from typing import Dict

from pyspark.sql.datasource import DataSourceReader
from pyspark.sql.types import StructType

from databricks.labs.gbx.pyrx.ds.raster import RasterGbxDataSource, RasterGbxReader


class GTiffGbxReader(RasterGbxReader):
    def __init__(self, options: Dict[str, str]):
        super().__init__(options)
        self.driver = "GTiff"


class GTiffGbxDataSource(RasterGbxDataSource):
    @classmethod
    def name(cls) -> str:
        return "gtiff_gbx"

    def reader(self, schema: StructType) -> DataSourceReader:
        return GTiffGbxReader(self.options)
