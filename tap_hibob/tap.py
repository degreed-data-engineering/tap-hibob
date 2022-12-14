"""hibob tap class."""

from pathlib import Path
from typing import List
import logging
import click
from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_hibob.streams import (
    Employees,
)

PLUGIN_NAME = "tap-hibob"

STREAM_TYPES = [
    Employees,
]

class TapHibob(Tap):
    """hibob tap class."""

    name = "tap-hibob"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "authorization",
            th.StringType,
            required=True,
            description="Authorization token for Auth2.0",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=False,
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_url",
            th.StringType,
            required=False,
            default="https://api.hibob.com",
            description="The url for the API service",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams =  [stream_class(tap=self) for stream_class in STREAM_TYPES]

        return streams


# CLI Execution:
cli = TapHibob.cli
