"""Stream class for tap-hibob."""

import base64
import json
from typing import Dict, Optional, Any, Iterable
from pathlib import Path
from singer_sdk import typing
from functools import cached_property
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import SimpleAuthenticator
import requests


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class TapHibobStream(RESTStream):
    """Hibob stream class."""

    _LOG_REQUEST_METRIC_URLS: bool = True

    @property
    def url_base(self) -> str:
        """Base URL of source"""
        return self.config["api_url"]

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Content-Type"] = "application/json"
        headers["Accept"] = "application/json"
        return headers

    @property
    def authenticator(self):
        authorization_key = self.config.get("authorization")
        http_headers = {"Authorization": f"Basic {authorization_key}"}
        return SimpleAuthenticator(stream=self, auth_headers=http_headers)


class Employees(TapHibobStream):
    name = "employees"  # Stream name
    path = "/v1/people"  # API endpoint after base_url
    primary_keys = ["id"]
    records_jsonpath = "$.employees[*]"  # https://jsonpath.com Use requests response json to identify the json path
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("creationDateTime", th.DateTimeType),
        th.Property("firstName", th.StringType),
        th.Property("surname", th.StringType),
        th.Property("fullName", th.StringType),
        th.Property("displayName", th.StringType),
        th.Property("companyId", th.IntegerType),
        th.Property("email", th.StringType),
        th.Property(
            "work",
            th.ObjectType(
                th.Property("startDate", th.StringType),
                th.Property("department", th.StringType),
                th.Property("isManager", th.BooleanType),
                th.Property("site", th.StringType),
                th.Property(
                    "reportsTo",
                    th.ObjectType(
                        th.Property("id", th.StringType),
                    ),
                ),
            ),
        ),
        th.Property(
            "internal",
            th.ObjectType(
                th.Property("terminationDate", th.StringType),
                th.Property("lifecycleStatus", th.StringType),
            ),
        ),
        th.Property(
            "address",
            th.ObjectType(
                th.Property("siteCountry", th.StringType),
                th.Property("usaState", th.StringType),
                th.Property("city", th.StringType),
                th.Property("siteCity", th.StringType),
                th.Property("country", th.StringType),
            ),
        ),
        th.Property(
            "payroll",
            th.ObjectType(
                th.Property(
                    "employment",
                    th.ObjectType(
                        th.Property("contract", th.StringType),
                    ),
                ),
            ),
        ),
        th.Property(
            "humanReadable",
            th.ObjectType(
                th.Property(
                    "work",
                    th.ObjectType(
                        th.Property(
                            "custom",
                            th.ObjectType(
                                # CompanyName
                                th.Property("field_1667499206086", th.StringType),
                                # AssociateID
                                th.Property("field_1667499039796", th.StringType),
                            ),
                        ),
                        th.Property(
                            "customColumns",
                            th.ObjectType(
                                # Subdepartment
                                th.Property("column_1667499229415", th.StringType),
                            ),
                        ),
                        th.Property("reportsTo", th.StringType),
                        th.Property("department", th.StringType),
                        th.Property("title", th.StringType),
                    ),
                ),
                th.Property(
                    "custom",
                    th.ObjectType(
                        th.Property(
                            "category_1673451690985",
                            th.ObjectType(
                                # DevelopPermissionRole
                                th.Property("field_1704464569961", th.StringType),
                                # DevelopDisableLogin
                                th.Property("field_1704464284132", th.StringType),
                                # DevelopDelete
                                th.Property("field_1704464333828", th.StringType),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["showInactive"] = "true"
        params["includeHumanReadable"] = "true"
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        # TODO: Improve the logic below and simplify it.
        employees_keys = set(
            [
                "id",
                "creationDateTime",
                "firstName",
                "surname",
                "fullName",
                "displayName",
                "companyId",
                "email",
                "work",
                "internal",
                "address",
                "payroll",
                "humanReadable",
            ]
        )
        for k in list(row.keys()):
            if k not in employees_keys:
                row.pop(k, None)

        employees_work_keys = set(
            [
                "startDate",
                "reportsTo",
                "department",
                "isManager",
                "site",
                "custom",
            ]
        )
        for k in list(row.get("work", {}).keys()):
            if k not in employees_work_keys:
                row.get("work", {}).pop(k, None)
        if row.get("work", {}).get("reportsTo"):
            for k in list(row.get("work", {}).get("reportsTo", {}).keys()):
                if k not in set(["id"]):
                    row.get("work", {}).get("reportsTo", {}).pop(k, None)
        if row.get("work", {}).get("custom"):
            for k in list(row.get("work", {}).get("custom", {}).keys()):
                if k not in set(["field_1667499206086"]):
                    row.get("work", {}).get("custom", {}).pop(k, None)

        employees_internal_keys = set(
            [
                "terminationDate",
                "lifecycleStatus",
            ]
        )
        for k in list(row.get("internal", {}).keys()):
            if k not in employees_internal_keys:
                row.get("internal", {}).pop(k, None)

        employees_address_keys = set(
            [
                "siteCountry",
                "usaState",
                "city",
                "siteCity",
                "country",
            ]
        )
        for k in list(row.get("address", {}).keys()):
            if k not in employees_address_keys:
                row.get("address", {}).pop(k, None)

        for k in list(row.get("payroll", {}).keys()):
            if k not in set(["employment"]):
                row.get("payroll", {}).pop(k, None)

        if row.get("payroll", {}).get("employment"):
            for k in list(row.get("payroll", {}).get("employment", {}).keys()):
                if k not in set(["contract"]):
                    row.get("payroll", {}).get("employment", {}).pop(k, None)

        humanreadable_work_keys = set(
            [
                "custom",
                "customColumns",
                "reportsTo",
                "department",
                "title",
            ]
        )
        for k in list(row.get("humanReadable", {}).keys()):
            if k not in set(["work"]):
                row.get("humanReadable", {}).pop(k, None)

        for k in list(row.get("humanReadable", {}).get("work", {}).keys()):
            if k not in humanreadable_work_keys:
                row.get("humanReadable", {}).get("work", {}).pop(k, None)

        for k in list(
            row.get("humanReadable", {}).get("work", {}).get("customColumns", {}).keys()
        ):
            if k not in set(["column_1667499229415"]):
                row.get("humanReadable", {}).get("work", {}).get(
                    "customColumns", {}
                ).pop(k, None)

        for k in list(
            row.get("humanReadable", {}).get("work", {}).get("custom", {}).keys()
        ):
            if k not in set(["field_1667499039796"]):
                row.get("humanReadable", {}).get("work", {}).get("custom", {}).pop(
                    k, None
                )

        humanreadable_custom_keys = set(
            [
                "category_1673451690985",
            ]
        )
        for k in list(row.get("humanReadable", {}).keys()):
            if k not in set(["custom"]):
                row.get("humanReadable", {}).pop(k, None)

        for k in list(row.get("humanReadable", {}).get("custom", {}).keys()):
            if k not in humanreadable_custom_keys:
                row.get("humanReadable", {}).get("custom", {}).pop(k, None)

        for k in list(
            row.get("humanReadable", {})
            .get("custom", {})
            .get("category_1673451690985", {})
            .keys()
        ):
            if k not in set(
                ["field_1704464569961", "field_1704464284132", "field_1704464333828"]
            ):
                row.get("humanReadable", {}).get("custom", {}).get(
                    "category_1673451690985", {}
                ).pop(k, None)

        return row
