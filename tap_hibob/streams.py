"""Stream class for tap-hibob."""

import requests
from pathlib import Path
from singer_sdk import typing as th
from typing import Dict, Optional, Any
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import SimpleAuthenticator


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
    path = "v1/people/search"  # API endpoint after base_url
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
                            "category_1726078147147",
                            th.ObjectType(
                                # DD_JobFamilyLevel
                                th.Property("field_1730210998067", th.StringType),
                            ),
                        ),
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

    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> requests.PreparedRequest:
        """Prepare a request object.

        If partitioning is supported, the `context` object will contain the partition
        definitions. Pagination information can be parsed from `next_page_token` if
        `next_page_token` is not None.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Build a request with the stream's URL, path, query parameters,
            HTTP headers and authenticator.
        """
        # TODO:
        # By default, the Meltano SDK sends requests using the GET method.
        # However, the new HiBob Employee API requires POST requests.
        # Since the current SDK version does not support overriding the `http_method` property directly,
        # we use a workaround by overriding the `prepare_request()` method.
        # Once we upgrade to Meltano SDK version 0.44.x or later, we can simplify the implementation
        # by overriding the `http_method` property instead of overriding the `prepare_request()` method.
        # References:
        # - SDK docs: https://sdk.meltano.com/en/latest/classes/singer_sdk.RESTStream.html#singer_sdk.RESTStream.http_method
        # - HiBob API docs: https://apidocs.hibob.com/reference/post_people-search

        http_method = "POST"
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        authenticator = self.authenticator
        if authenticator:
            headers.update(authenticator.auth_headers or {})
            params.update(authenticator.auth_params or {})

        request = self.requests_session.prepare_request(
            requests.Request(
                method=http_method,
                url=url,
                params=params,
                headers=headers,
                json=request_data,
            ),
        )
        return request

    def prepare_request_payload(self, context, next_page_token):
        # Return your JSON payload as a Python dict
        # See: https://apidocs.hibob.com/reference/post_people-search
        return {
            "showInactive": True,
            "humanReadable": "append",
            "fields": [
                "root.id",
                "root.creationDateTime",
                "root.firstName",
                "root.surname",
                "root.fullName",
                "root.displayName",
                "root.companyId",
                "root.email",
                "work.startDate",
                "work.department",
                "work.isManager",
                "work.site",
                "work.reportsTo",
                "internal.lifecycleStatus",
                "internal.terminationDate",
                "address.siteCountry",
                "address.usaState",
                "address.city",
                "address.siteCity",
                "address.country",
                "payroll.employment.contract",
                "work.custom.field_1667499206086",
                "work.custom.field_1667499039796",
                "work.customColumns.column_1667499229415",
                "work.reportsTo",
                "work.department",
                "work.title",
                "custom.category_1726078147147.field_1730210998067",
                "custom.category_1673451690985.field_1704464569961",
                "custom.category_1673451690985.field_1704464284132",
                "custom.category_1673451690985.field_1704464333828",
            ],
        }

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        # Define the keys to keep at each level
        employees_keys = {
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
        }
        employees_work_keys = {
            "startDate",
            "reportsTo",
            "department",
            "isManager",
            "site",
            "custom",
        }
        employees_internal_keys = {
            "terminationDate",
            "lifecycleStatus",
        }
        employees_address_keys = {
            "siteCountry",
            "usaState",
            "city",
            "siteCity",
            "country",
        }
        payroll_keys = {"employment"}
        employment_keys = {"contract"}
        human_readable_keys = {"work", "custom"}
        hr_work_keys = {
            "custom",
            "customColumns",
            "reportsTo",
            "department",
            "title",
        }
        hr_work_custom_keys = {
            "field_1667499206086",
            "field_1667499039796",
        }
        hr_work_custom_columns_keys = {
            "column_1667499229415",
        }
        hr_custom_keys = {
            "category_1726078147147",
            "category_1673451690985",
        }
        category_1726078147147_keys = {
            "field_1730210998067",
        }
        category_1673451690985_keys = {
            "field_1704464569961",
            "field_1704464284132",
            "field_1704464333828",
        }

        # Helper function to clean up dictionaries
        def cleanup_dict(d: Optional[dict], keys_to_keep: set):
            if not isinstance(d, dict):
                return
            keys_to_delete = [key for key in d.keys() if key not in keys_to_keep]
            for key in keys_to_delete:
                d.pop(key, None)

        # Clean up the top-level row
        cleanup_dict(row, employees_keys)

        # Clean up 'work' section
        work = row.get("work")
        cleanup_dict(work, employees_work_keys)
        if isinstance(work, dict):
            reports_to = work.get("reportsTo")
            cleanup_dict(reports_to, {"id"})
            custom = work.get("custom")
            cleanup_dict(custom, {"field_1667499206086"})

        # Clean up 'internal' section
        internal = row.get("internal")
        cleanup_dict(internal, employees_internal_keys)

        # Clean up 'address' section
        address = row.get("address")
        cleanup_dict(address, employees_address_keys)

        # Clean up 'payroll' section
        payroll = row.get("payroll")
        cleanup_dict(payroll, payroll_keys)
        if isinstance(payroll, dict):
            employment = payroll.get("employment")
            cleanup_dict(employment, employment_keys)

        # Clean up 'humanReadable' section
        human_readable = row.get("humanReadable")
        cleanup_dict(human_readable, human_readable_keys)
        if isinstance(human_readable, dict):
            # Clean up 'humanReadable' -> 'work' section
            hr_work = human_readable.get("work")
            cleanup_dict(hr_work, hr_work_keys)
            if isinstance(hr_work, dict):
                # Clean up 'humanReadable' -> 'work' -> 'custom' section
                hr_work_custom = hr_work.get("custom")
                cleanup_dict(hr_work_custom, hr_work_custom_keys)

                # Clean up 'humanReadable' -> 'work' -> 'customColumns' section
                hr_work_custom_columns = hr_work.get("customColumns")
                cleanup_dict(hr_work_custom_columns, hr_work_custom_columns_keys)

            # Clean up 'humanReadable' -> 'custom' section
            hr_custom = human_readable.get("custom")
            cleanup_dict(hr_custom, hr_custom_keys)
            if isinstance(hr_custom, dict):
                # Clean up 'humanReadable' -> 'custom' -> 'category_1726078147147' section
                category_1726078147147 = hr_custom.get("category_1726078147147")
                cleanup_dict(category_1726078147147, category_1726078147147_keys)

                # Clean up 'humanReadable' -> 'custom' -> 'category_1673451690985' section
                category_1673451690985 = hr_custom.get("category_1673451690985")
                cleanup_dict(category_1673451690985, category_1673451690985_keys)

        return row
