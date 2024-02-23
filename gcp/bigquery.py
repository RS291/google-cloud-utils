"""
Barklion's BigQuery Client for handling pandas and json based data.

NOTE: google.cloud.bigquery requires pyarrow to serialize a pandas DataFrame to a Parquet file.
        may need in requirements.txt?

"""

import datetime
import json
import re
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from logging import Logger
from math import inf, nan
from typing import Any, Literal, Optional, Union
from unicodedata import decimal

import flat_table
import google.auth
import numpy as np
import pandas as pd
import pytz
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField
from google.cloud.exceptions import NotFound
from pandas.core.frame import DataFrame


class BigQueryClient(object):
    pass


class BQC(object):
    def __init__(self, parent: BigQueryClient):
        self._parent = parent
        self._project_id = parent._project_id
        self._credentials = parent._credentials
        self._region = parent._region
        self._client = parent._client
        self._logger = parent._logger


class Schema(BQC):
    """Methods with API calls that pertain to BQ Schema."""

    def get(self, table_id: str) -> list:
        """
        - get_bq_schema
        Returns table schema for existing BQ table in BQ format.
        """
        table = self._client.get_table(table_id)
        bq_schema = table.schema
        self._logger.info("BQ Schema retrieved.")
        return bq_schema

    def update(self, table_id: str, new_schema: list) -> Literal[True]:
        """
        - update_bq_schema
        Update the BQ table schema in BQ.

        Args:
            table_id: ifn format <project>.<dataset>.<table>
            new_schema: in BigQuery format.

        NOTE: It is not possible to change the datatype of already existing fields,
            you can only ADD new fields and define their datatype!
        """
        table = self._client.get_table(table_id)
        table.schema = new_schema
        table = self._client.update_table(table, ["schema"])
        self._logger.info("Updated BQ Schema.")
        return True


class SchemaData(Schema):
    """Methods that manipulate BQ Schemas. No API calls."""

    def rec_to_py(self, bq_schema: list[SchemaField]) -> dict:
        """
        - recursive_bq_to_json_schema
        Converts a BQ schema using SchemaField types into our internal python schema.

        Args:
            bq_schema : BQ formatted Schema, eg. result of schema.get()

        Returns:
            py_schema : internal python schema format (e.g. {'first_name': str, 'dob': date})

        NOTE: current type matching found at Metadata Dictionary in Airtable:
            https://airtable.com/appadtNUd0rPav12f/tblHxw7SNz69nlBbQ/viw7OSYnV7ZxIddqs?blocks=hide
            Hardcoding used to avoid changes of type matching in existing ETLs, unless actually planned.
        """
        types = {
            "ARRAY": list,
            "BIGNUMERIC": Decimal,
            "BOOL": bool,
            "BYTES": bytes,
            "DATE": date,
            "TIMESTAMP": "datetime|UTC",  # datetime,
            "DATETIME": "datetime|None",  # datetime,
            "INTERVAL": timedelta,
            "TIME": time,
            "NUMERIC": Decimal,
            "FLOAT": float,  # np.double in Metadata Dictionary TODO: discuss type matching
            "INT": int,  # not INT64 when using table.schema
            "STRUCT": dict,
            "STRING": str,
        }

        py_schema = {}
        for field in bq_schema:
            info = field.to_api_repr()
            data_type = info["type"]
            if data_type == "RECORD":
                fields = info.get("fields")
                nested = []
                for f in fields:
                    bq_f = bigquery.SchemaField.from_api_repr(f)
                    nested.append(bq_f)
                mapped = self.rec_to_py(nested)
                mapped["BQSCHEMA-repeated"] = (
                    True if info["mode"] == "REPEATED" else False
                )
            else:
                mapped = types.get(data_type)
            py_schema[info.get("name")] = mapped
        self._logger.info("Created python schema from BQ schema.")
        return py_schema

    def rec_to_json(self, bq_schema: list[SchemaField]) -> list[dict]:
        """
        Converts a BQ schema into a BQ formatted json schema
        https://cloud.google.com/bigquery/docs/schemas

        Example:
        [
            {
                "description": "quarter",
                "mode": "REQUIRED",
                "name": "qtr",
                "type": "STRING"
            },
            {
                "description": "sales representative",
                "mode": "NULLABLE",
                "name": "rep",
                "type": "STRING"
            },
        ]
        """
        json_schema = [i.to_api_repr() for i in bq_schema]
        self._logger.info("Created json schema from BQ schema.")
        return json_schema

    def append_field(
        self,
        bq_schema,
        name: str,
        data_type: str,
        mode: str = "NULLABLE",
        description: Optional[str] = None,
    ) -> Literal[True]:
        """
        Appends a new schema field to the BQ schema.
        NOTE: nothing gets written to BQ at this stage!!!

        Args:
            bq_schema : original BQ schema in BQ format.
            name :  Name of new field (be aware of GCP/BQ restrictions on sign usage)
            type :  Data type, see https://cloud.google.com/bigquery/docs/schemas#standard_sql_data_types
            mode :  NULLABLE or REPEATED (cannot use REQUIRED for new fields).
            description : Field description as it will be shown in BQ.

        """
        try:
            new_py = {
                "description": description,
                "name": name,
                "type": data_type,
                "mode": mode,
            }
            new = bigquery.SchemaField.from_api_repr(new_py)
            bq_schema.append(new)
            self._logger.info(
                f"Appended new field '{name}' with type '{data_type}' to BQ schema."
            )
            return True
        except:
            raise RuntimeError(
                f"Field {name} could not be added to existing BQ schema."
            )


class Pandas(BQC):
    """Methods with API calls that use pandas DFs."""

    def _shared_col(self, df1: DataFrame, df2: DataFrame) -> list[str]:
        """
        - _get_shared_df_col
        Returns a list of column names shared between 2 DataFrames.
        """
        col1 = set(df1.columns.tolist())
        col2 = set(df2.columns.tolist())
        col_shared = col1.intersection(col2)
        self._logger.info(f"Shared df cols identified: {list(col_shared)}.")
        return list(col_shared)

    def _diff_col(self, df1: DataFrame, df2: DataFrame) -> tuple[list[str], list[str]]:
        """
        - _get_diff_df_col
        Find column names that are unique to the DataFrames compared.

        Returns:
            diff1: column names found in df1 only
            diff2: column names found in df2 only
        """
        col1 = set(df1.columns.tolist())
        col2 = set(df2.columns.tolist())
        diff1 = list(col1.difference(col2))
        diff2 = list(col2.difference(col1))
        self._logger.info(
            f"Unique df columns identified: df1 ({diff1}), df2, ({diff2})."
        )
        return diff1, diff2

    def _build_schema(
        self,
        df: DataFrame,
    ) -> list[SchemaField]:
        """
        - generate_bq_schema
        Generates a BQ schema based on assumed datatypes in a given DataFrame.
        NOTE: Replace all Null fields before running, and have all column names formatted to their BQ name
            Default type for cols that cannot be determined is a nullable string.

        NOTE: CURRENTLY DOES NOT HANDLE ALL ENCOUNTERED DATATYPES (DATETIME/DATE/TIME VALUES)
        BUG fix pending
        """
        bq_schema = []
        for c, field_type in df.dtypes.iteritems():
            not_nulls = df[~df[f"{c}"].isnull()]
            if len(not_nulls) == 0:  # if the column is all nulls, pass for now..
                self._logger.debug(f"There is no data to determine type of: {c}.")
            else:
                # first, handle the generic object case
                ## count the different types present
                type_counts = [
                    x
                    for x in df[c].apply(type).value_counts().iteritems()
                    if x[0] != type(None)
                ]
                # overwrite field types if it turns out this is a homogeneous object
                if field_type is np.dtype("O") and len(type_counts) == 1:
                    field_type = type_counts[0][0]
                # if this is a homogeneous list, just change the mode
                if field_type is list:
                    mode = "REPEATED"
                    list_types = [
                        x
                        for x in df[c]
                        .apply(pd.Series)
                        .stack()
                        .reset_index(drop=True)
                        .apply(type)
                        .value_counts()
                        .iteritems()
                        if x[0] != type(None)
                    ]
                    if len(list_types) == 1:
                        field_type = list_types[0][0]
                    else:
                        field_type = np.dtype("O")
                        type_counts = list_types
                else:
                    # keep the mode standard
                    mode = "NULLABLE"
                # in the case of mixed types, consolidate to a standard
                if field_type is np.dtype("O"):
                    # This case only happens if object is a mixed type, see ln 53-54 above
                    # in this case, it's a mixed case, so we need to identify how to represent
                    if any([x[0] == str for x in type_counts]):
                        field_type = str
                    elif any(
                        [x[0] in [np.float64, Decimal, float] for x in type_counts]
                    ):
                        field_type = np.float64
                    elif any(
                        [
                            x([0]) == np.dtype("datetime64[ns]") for x in type_counts
                        ]  # TODO: doesn't seem to handle datetime
                    ):
                        field_type = np.dtype("datetime64[ns]")
                    else:
                        # default to string
                        field_type = str
                # now do standard type matching
                # TODO: EG. NO HANDLING OF DATETIME.DATETIME TO TIMESTAMP!
                if field_type == str:
                    bq_schema.append(bigquery.SchemaField(c, "STRING", mode))
                elif field_type == np.float64 or field_type == float:
                    bq_schema.append(bigquery.SchemaField(c, "FLOAT64", mode))
                elif field_type == np.int64 or field_type == int:
                    bq_schema.append(bigquery.SchemaField(c, "INT64", mode))
                elif field_type == bool:
                    bq_schema.append(bigquery.SchemaField(c, "BOOLEAN", mode))
                elif (
                    field_type == np.dtype("datetime64[ns]")
                    or field_type == pd.Timestamp
                ):
                    bq_schema.append(bigquery.SchemaField(c, "DATETIME", mode))
                elif (
                    field_type == datetime.datetime and df[c][0].tzinfo is not None
                ):  # datetime obj with timezone info present - ^ is this the best way to access the first field to test tzinfo? would be happy to chat about in digidemo
                    bq_schema.append(bigquery.SchemaField(c, "DATETIME", mode))
                elif field_type == datetime.datetime:  # datetime obj w/o timezone info
                    bq_schema.append(bigquery.SchemaField(c, "TIMESTAMP", mode))
                elif field_type == Decimal:
                    bq_schema.append(bigquery.SchemaField(c, "NUMERIC", mode))
                elif field_type == datetime.date:
                    bq_schema.append(bigquery.SchemaField(c, "DATE", mode))
                elif field_type == datetime.time:
                    bq_schema.append(bigquery.SchemaField(c, "TIME", mode))
                else:
                    bq_schema.append(
                        bigquery.SchemaField(c, "STRING", mode)
                    )  # if cannot be identified & parsed, make str
                    self._logger.error(
                        f"No matching BQ field type for {field_type} - assigned STRING in schema."
                    )
        self._logger.info(f"BQ schema has been created from DataFrame.")
        return bq_schema

    def _format_keys(self, df: DataFrame) -> DataFrame:
        """
        Format column names to be BQ compatible.
        NOTE: does not overwrite original DF, but creates new one.
        """
        new_df = df.copy()
        colnames = []
        list_to_sub = "[^_0-9a-zA-Z]+"
        for c in new_df.columns:
            if re.search(list_to_sub, c) is not None:
                c = re.sub(list_to_sub, "_", c)
            c = c.lower()
            colnames.append(c)
        new_df.columns = colnames
        self._logger.info("Column names have been formatted for BigQuery.")
        return new_df

    def write(
        self,
        df: DataFrame,
        table_id: str,
        mode: Literal["append", "replace", "save_write"] = "append",
        bq_schema: Optional[list[SchemaField]] = None,
        bq_detect: bool = False,
    ) -> Literal[True]:
        """
        - write_df_to_gbq
        Write a Pandas DataFrame to BigQuery.
        This function requires some data to be pre-processed due to Parquet-related constraints within the BQ API.
        Any data coming from AirTable needs to be processed to remove Nulls and SpecialValues.

        Args:
            df :    DataFrame to upload.
            table_id :  BigQuery table ID. in the format "project.dataset.table" (e.g. f"{project}.events_from_airtable.events")
            mode : ["append", "replace", "fail"], to replace or append existing records. same as if_exists param in pandas_gbq.
            schema :    BigQuery table schema in list format. Documentation for formatting can be found here: https://cloud.google.com/bigquery/docs/schemas

        TODO:
            - SCHEMA AUTODETECT DOES NOT HANDLE SPECIAL DATA TYPES WELL YET (E.G. DATATIME, DATE, TIME, NUMERIC...)
        """

        def df_to_json_parser(df):
            """
            Converts Decimal and datetime to BQ compatible formats for write.
            NOTE: in lieu of using something like simplejson and creating another dependancy,
                 this JSON encoder exists to handle decimal and datetimes

            """

            try:
                jsonized_df = df.to_json(
                    orient="records", default_handler=PandasData._default_handler
                )  # str representing the jsonified df
                json_object = json.loads(
                    jsonized_df
                )  # turn the str representation into json object
                return json_object
            except Exception as e:
                self._logger.error(f"Error! Could not convert DataFrame to JSON: {e}")
                return False

        try:
            # convert keys to BQ compatible
            df_cl = self._format_keys(df)

            # create schema if needed
            bq_detect = (
                False if bq_schema else bq_detect
            )  # providing schema disables BQ autodetect
            if bq_detect:
                bq_schema = None
            else:
                bq_schema = (
                    self._build_schema(df_cl) if not bq_schema else bq_schema
                )  # BUG!

            # set mode
            if mode == "replace":
                job_config = bigquery.LoadJobConfig(
                    schema=bq_schema,
                    write_disposition="WRITE_TRUNCATE",
                    ignore_unknown_values=True,
                )
            elif (
                mode == "save_write"
            ):  # does NOT overwrite, only writes data if table does not exist
                job_config = bigquery.LoadJobConfig(
                    schema=bq_schema,
                    write_disposition="WRITE_EMPTY",
                    ignore_unknown_values=True,
                )
            else:  # TODO: need explicit value error failure if not given 'append'
                # default is "WRITE_APPEND", if mode is not specified
                job_config = bigquery.LoadJobConfig(
                    schema=bq_schema,
                    write_disposition="WRITE_APPEND",
                    ignore_unknown_values=True,
                )
            self._logger.info(
                f"Load Job Config has been set with mode: {mode}. Making API request..."
            )

            # Serialize DF to JSON
            jsonified_df = df_to_json_parser(df)

            # Make the API request
            job = self._client.load_table_from_json(
                jsonified_df, table_id, job_config=job_config
            )
            job.result()

            table = self._client.get_table(table_id)
            self._logger.info(
                f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}."
            )
            return True

        except Exception as e:
            self._logger.error(f"Error! Could not write DataFrame to BigQuery: {e}")
            return False

    def get(self, query: str) -> DataFrame:
        """
        - get_df_to_gbq
        Reads and loads data from BigQuery into a Pandas DataFrame.

        Args:
            query: SQL query to return data.
        """
        try:
            query_job = self._client.query(query)
            df = query_job.result().to_dataframe()
            self._logger.info("SQL query posted and date converted to DF.")
            return df
        except Exception as e:
            self._logger.error(
                f"Error! Could not retrieve DataFrame from BigQuery: {e}"
            )
            return None


class PandasData(Pandas):
    """Methods that are based on DF data or use pandas DF for data manipulation."""

    def flatten(
        self, data: Union[list[dict], DataFrame], output: Literal["df", "json"]
    ) -> Union[list[dict], DataFrame]:
        """
        Flattens data via use of pandas DF and flat_table.
        Can take either a json (list of dictionaries) or a df, and will return
        df, or json (list[dict]) as requested.

        NOTE:
            - if field values are lists, this function creates new rows, thus
                limiting the number of new fields created, but increasing the number
                of rows in the table.
            - BUG: DOES NOT handle values that are pure lists (eg. 'key': [1, 2, 3]) unless
                entries are strings or of mixed type.
                Reason: 'key' has <object> as value type, unnested this column will have
                take the type of the list elements.
            - unnested names will contain "." (example "by_identity.name"), which
                need to be removed before converting DF to BQ Schema, and/or writing
                to BQ.

        Example:
            data_json = {'a': 1, 'b': {'z': 1, 'y': 2}, 'd': [{'aa': 11, 'bb': 22}, {'aa': 33, 'bb': 44}]}
            returns_json = [{'a': 1, 'b.z': 1, 'b.y': 2, 'd.bb': 22, 'd.aa': 11}, {'a': 1, 'b.z': 1, 'b.y': 2, 'd.bb': 44, 'd.aa': 33}]
            returns_df =       a  b.z  b.y d.bb d.aa
                            0  1    1    2   22   11
                            1  1    1    2   44   33

        """
        # (1) convert json to df, no unnest
        if isinstance(data, DataFrame):
            df1 = data
        else:
            df1 = pd.json_normalize(data)

        if df1.empty:
            self._logger.warning("No data.")
            return df1

        # (2) unnest any fields if df contains nesting
        try:
            df2_full = flat_table.normalize(df1, expand_dicts=True, expand_lists=True)
            self._logger.info("Json flattening OK.")
        except:
            self._logger.warning(f"No data to flatten.")
            return df1

        # (3) drop automatically created index column if present
        col_full = df2_full.columns.tolist()
        df2 = df2_full.drop(columns=["index"]) if ("index" in col_full) else df2_full
        self._logger.info("Flat table Index column successfully dropped.")

        # (4) find unique column names
        diff1, diff2 = self._diff_col(df1, df2)
        nested_cols = []
        for col_name1 in diff1:
            id1 = col_name1 + "."
            results = [col_name2 for col_name2 in diff2 if id1 in col_name2]
            if results != []:
                nested_cols.append(col_name1)
        self._logger.info(f"Nested cols: {nested_cols}")

        # (5) find shared column names
        col = self._shared_col(df1, df2)

        # (6) combine datasets & remove nested cols that have their data in separate cols
        df3 = df1.merge(df2, how="left", on=col)
        df_clean = df3.drop(nested_cols, axis=1)
        col_clean = df_clean.columns.tolist()
        self._logger.debug(f"Final columns: {col_clean}.")

        # (7) choose format to return data in
        if output == "json":
            js_flat = df_clean.to_dict(orient="records")
            js_clean = [{k: d[k] for k in d if not pd.isna(d[k])} for d in js_flat]
            self._logger.info("Returned flattened json data.")
            return js_clean
        elif output == "df":
            self._logger.debug("Returned flattened df data.")
            return df_clean

    def build_schema(self, df: DataFrame) -> list[SchemaField]:
        """
        Builds a BQ schema from data in DF.
        NOTE: currently, detection is not robust!
        """
        bq_schema = self._build_schema(df)
        return bq_schema

    def format_keys(self, df: DataFrame) -> DataFrame:
        """Creates copy of DF with BQ compatible keys."""
        return self._format_keys(df)

    def shared_cols(self, df1: DataFrame, df2: DataFrame) -> list[str]:
        """Identifies fields (col names) that are shared between 2 DFs."""
        return self._shared_col(df1, df2)

    def diff_cols(self, df1: DataFrame, df2: DataFrame) -> tuple[list[str], list[str]]:
        """
        Identifies columns not shared between two DFs.
        Returns tuple with two list, first list for col names only found in df1,
        second list for col names only found in df2.
        """
        return self._diff_col(df1, df2)

    def format_from_at(self, airtable_records, process: bool = True) -> DataFrame:
        """
        - convert_to_df_processed
        Converts AirTable records JSON blob into a Pandas dataframe object.

        If process = True, these records will be pre-processed to remove nulls and specialValues.
        TODO: Q - Should this be kept? Airtable has processing tools too.. (though probably not pd) where to put?
        TODO: Q - what is the data type inpuyt for airtable_records...
        """
        if process == True:
            airtable_rows = []
            airtable_index = []
            for row in airtable_records:
                newrow = {}
                for k, v in row["fields"].items():
                    if type(v) == dict and v.get("specialValue"):
                        sv = v.get("specialValue")
                        if sv == "Infinity":
                            newrow[k] = inf
                        elif sv == "-Infinity":
                            newrow[k] = -inf
                        elif sv == "NaN":
                            newrow[k] = nan
                    elif type(v) == list:
                        newrow[k] = list(filter(None, v))
                    else:
                        newrow[k] = v
                airtable_rows.append(newrow)
                airtable_index.append(row["id"])
            airtable_dataframe = pd.DataFrame(airtable_rows, index=airtable_index)

        else:
            airtable_rows = []
            airtable_index = []
            for record in airtable_records:
                airtable_rows.append(record["fields"])
                airtable_index.append(record["id"])
            airtable_dataframe = pd.DataFrame(airtable_rows, index=airtable_index)
        return airtable_dataframe

    @staticmethod
    def _default_handler(obj):
        """
        Default handler for decimal and datetimes to be used within the (Pandas) write function as the default_handler param.
        Updated to parse decimal into str, and datetime into ISO 8601 format (DE-915).
        """
        if isinstance(obj, Decimal):
            return str(
                obj
            )  # changed to string vs. previous implementation as a float (DE-915)
        elif isinstance(obj, (datetime, str)) and re.search(
            "\ UTC", obj
        ):  # TODO: THIS RECEIVED A DEPRECATION WARNING WHEN RUNNING PYTEST: 'DeprecationWarning: invalid escape sequence \'
            try:
                # attempt to parse the datetime into ISO 8601 format
                obj = datetime.strptime(obj, "%m %d %Y %H:%M:%S")
                iso_obj = obj.isoformat()
                return str(iso_obj)
            except:
                # if that cannot be done, convert into str
                return str(obj)
        else:
            raise TypeError


class Json(BQC):
    """Methods with API calls that use Json formatting."""

    def _rec_format_keys(self, json_dict: dict) -> dict:
        """
        - _recursive_key_formatting
        Format all keys into BQ compatible standard:
            no spaces, no punctuation; replace everything with underscores
        """
        output = {}
        list_to_sub = "[^_0-9a-zA-Z]+"
        # json_object = json_object
        keylist = [k for k in json_dict.keys()]
        for k in keylist:
            v = json_dict[k]
            # force formatting AFTER you get the value
            k = k.lower()
            if re.search(list_to_sub, k) is not None:
                oldk = k
                k = re.sub(list_to_sub, "_", k)
                print(f"subbing {k} for {oldk}")
            #
            if isinstance(v, dict):
                output[k] = self._rec_format_keys(v)
            elif isinstance(v, list):
                newlist = []
                for x in v:
                    if isinstance(
                        x, dict
                    ):  # NOTE: previous code in use had 'v' here ! error
                        newlist.append(self._rec_format_keys(x))
                    else:
                        newlist.append(x)
                output[k] = newlist
            else:
                output[k] = v
        return output

    def _rec_remove_nulls(self, json_dict: dict) -> dict:
        """
        - _recursive_remove_null_keys
        Remove null keys from json for BQ import.

        NOTE: BQ has a known import error where null keys aren't always accepted.
            Better to leave them out (implicit null) to minimize errors.
        """
        nonulls = json_dict.copy()
        keylist = json_dict.keys()
        for k in keylist:
            v = json_dict[k]
            if v is None:
                nonulls.pop(k, None)
            elif isinstance(v, dict):
                nonulls[k] = self._rec_remove_nulls(v)
                if len(nonulls[k]) == 0:
                    nonulls.pop(k, None)
            elif isinstance(v, list):
                nnlist = []
                for x in v:
                    if isinstance(x, dict):
                        newvalue = self._rec_remove_nulls(x)
                    else:
                        newvalue = x
                    if newvalue != {}:
                        nnlist.append(newvalue)
                if nnlist == []:
                    nonulls.pop(k, None)
                else:
                    nonulls[k] = nnlist
        return nonulls

    def _rec_build_py_schema(
        self,
        json_object: Union[dict, list[dict]],
        py_schema: Optional[dict] = None,
        use_datetime: bool = True,
    ) -> dict:
        """
        - _recursive_map_json_schema
        Build a python based schema from a json object for BQ comparison and BQ schema creation.

        Args:
            json_object : json dictionary or list of dictionary (e.g. row[s] of data) to be mapped
            schema : Any existing schema or part of a schema
            use_datetime: if false, ignores python datetime/date/time types and uses string instead.

        NOTE: datetime => currently only uses datetime options when date(time) is given as datetime type.
            String representation will be interpreted as type string.
        NOTE: Currently type is set by first value encountered. In some cases this may lead
        to conflict. TODO: have option to check against existing schema and update (be aware, that
        this is only useful when schema is purely based on the data provided and BQ does not already
        have values set. BQ does not allow for changes of existing types during schema update)
        """
        if py_schema is None:
            py_schema = {}
        if isinstance(json_object, list):  # for list input
            # NOTE: type is set by first data row encountered,
            #   subsequent rows only used to fill in types for keys not present in first row
            for jo in json_object:
                oldschema = py_schema.copy()
                py_schema = self._rec_build_py_schema(
                    jo, oldschema, use_datetime=use_datetime
                )
        else:
            for k in json_object.keys():
                oldschema = py_schema.copy()
                v = json_object[k]
                # if k in schema and a dict, process
                if isinstance(v, dict):
                    if oldschema.get(k) is None:
                        oldschema[k] = {"BQSCHEMA_repeated": False}
                    py_schema[k] = self._rec_build_py_schema(
                        v, oldschema[k], use_datetime=use_datetime
                    )
                # if k in schema and list, iterate
                elif isinstance(v, list):
                    if isinstance(v[0], dict):
                        if oldschema.get(k) is None:
                            oldschema[k] = {"BQSCHEMA_repeated": True}
                        py_schema[k] = self._rec_build_py_schema(
                            v, oldschema[k], use_datetime=use_datetime
                        )
                    else:
                        # type classification on all items in list!
                        if all(isinstance(x, bool) for x in v):
                            py_schema[k] = [bool]
                        elif all(
                            isinstance(x, int) and not isinstance(x, bool) for x in v
                        ):  # because a bool is also an int when using isinstance
                            py_schema[k] = [int]
                        elif all(isinstance(x, float) for x in v):
                            py_schema[k] = [float]
                        elif all(isinstance(x, Decimal) for x in v):
                            py_schema[k] = [Decimal]
                        elif (
                            all(
                                isinstance(x, date) and not isinstance(x, datetime)
                                for x in v
                            )  # NOTE datetime returns true on isinstance(x, date)
                            and use_datetime
                        ):
                            py_schema[k] = [date]
                        elif all(isinstance(x, time) for x in v) and use_datetime:
                            py_schema[k] = [time]
                        elif all(isinstance(x, datetime) for x in v) and use_datetime:
                            tzs = set([x.tzinfo for x in v])
                            py_schema[k] = (
                                ["datetime|multi"]
                                if len(tzs) > 1
                                else [f"datetime|{tzs}"]
                            )
                        else:
                            py_schema[k] = [str]
                else:
                    # if k in schema and simple, skip
                    # if k not in schema, add k, but value will depend on type
                    if k not in oldschema.keys():
                        if isinstance(v, bool):
                            py_schema[k] = bool
                        elif isinstance(v, int) and not isinstance(v, bool):
                            py_schema[k] = int
                        elif isinstance(v, float):
                            py_schema[k] = float
                        elif isinstance(v, Decimal):
                            py_schema[k] = Decimal
                        elif (
                            isinstance(v, date)
                            and not isinstance(v, datetime)
                            and use_datetime
                        ):
                            py_schema[k] = date
                        elif isinstance(v, time) and use_datetime:
                            py_schema[k] = time
                        elif isinstance(v, datetime) and use_datetime:
                            # needed to distinguish between BQ TIMESTAMP and DATETIME
                            py_schema[k] = f"datetime|{v.tzinfo}"
                        else:
                            py_schema[k] = str
        return py_schema

    def _rec_build_schema(
        self, bq_schema: list[SchemaField], py_schema: dict
    ) -> list[SchemaField]:
        """
        - _recursive_compare_schema
        Compare the bigquery schema (BQ format) with an inferred python schema.
        If json includes fields not in bigquery, create a list of things to add (in order)
        If bq_schema = [], entire py_schema gets converted to BQ format for write jobs.
        NOTE: MAKE SURE NO NULL FIELDS BEFORE RUNNING AND ALL FIELD NAMES FORMATTED
        NOTE: If you want to have datetime/date/time types as str, make sure that is already
            done in the py_schema fed to this function.

        Args:
            bq_schema: existing BQ schema in BQ format.
            py_schema: schema derived from data in internal 'python dict format'

        Returns:
            new_schema : new, combined schema in BQ format.

        # TODO: find out what BQ to python does when having a DATETIME bq field.
        # TODO: can be simplified by doing type matching with dictionary (see bq to json schema)
        """
        new_schema = []
        keylist = list([x.name for x in bq_schema])
        keylist.extend(x for x in py_schema.keys() if x not in keylist)
        for k in keylist:
            # if k exists and is a simple field, skip
            # if k exists and is a repeated/record field, check repeated/record and then check the fields (recursive)
            # if k doesn't exist and is a simple field, add
            # if k doesn't exist and is a repeated/record field, build the fields and then create the parent field
            if k not in py_schema.keys():
                # only in bigquery, go ahead and add and move on
                new_schema.append([x for x in bq_schema if x.name == k][0])
                continue
            v = py_schema[k]
            # TODO: handle v == 'datetime|?'
            if k == "BQSCHEMA_repeated":
                # field derives from json schema generation, internal creation, not real field
                continue
            # (A) simple types
            if v in [
                str,
                int,
                float,
                Decimal,
                bool,
                date,
                time,
                [str],
                [int],
                [float],
                [Decimal],
                [bool],
                [date],
                [time],
            ]:
                if k in [x.name for x in bq_schema]:
                    # this field already exists
                    new_schema.append([x for x in bq_schema if x.name == k][0])
                else:
                    # we have to add this to the new schema
                    # simple field, assume string, cast later if wrong

                    if v == float:
                        new_schema.append(bigquery.SchemaField(k, "FLOAT", "NULLABLE"))
                    elif v == Decimal:
                        new_schema.append(
                            bigquery.SchemaField(k, "NUMERIC", "NULLABLE")
                        )
                    elif v == int:
                        new_schema.append(bigquery.SchemaField(k, "INT64", "NULLABLE"))
                    elif v == bool:
                        new_schema.append(
                            bigquery.SchemaField(k, "BOOLEAN", "NULLABLE")
                        )
                    elif v == date:
                        new_schema.append(bigquery.SchemaField(k, "DATE", "NULLABLE"))
                    elif v == time:
                        new_schema.append(bigquery.SchemaField(k, "TIME", "NULLABLE"))
                    elif v == [str]:
                        new_schema.append(bigquery.SchemaField(k, "STRING", "REPEATED"))
                    elif v == [int]:
                        new_schema.append(bigquery.SchemaField(k, "INT64", "REPEATED"))
                    elif v == [float]:
                        new_schema.append(bigquery.SchemaField(k, "FLOAT", "REPEATED"))
                    elif v == [Decimal]:
                        new_schema.append(
                            bigquery.SchemaField(k, "NUMERIC", "REPEATED")
                        )
                    elif v == [bool]:
                        new_schema.append(
                            bigquery.SchemaField(k, "BOOLEAN", "NULLABLE")
                        )
                    elif v == [date]:
                        new_schema.append(bigquery.SchemaField(k, "DATE", "NULLABLE"))
                    elif v == [time]:
                        new_schema.append(bigquery.SchemaField(k, "TIME", "NULLABLE"))
                    else:
                        new_schema.append(bigquery.SchemaField(k, "STRING", "NULLABLE"))
            # (B) datetime lists
            elif isinstance(v, list):
                if k in [x.name for x in bq_schema]:
                    # this field already exists
                    new_schema.append([x for x in bq_schema if x.name == k][0])
                else:
                    if v == ["datetime|UTC"]:
                        new_schema.append(
                            bigquery.SchemaField(k, "TIMESTAMP", "REPEATED")
                        )
                    elif v == ["datetime|None"]:
                        new_schema.append(
                            bigquery.SchemaField(k, "DATETIME", "REPEATED")
                        )
                    elif v == ["datetime|multi"]:
                        # will be converted to correct timestamp, if field is given as ISO string with tz info
                        new_schema.append(
                            bigquery.SchemaField(k, "TIMESTAMP", "REPEATED")
                        )
                    elif v[0].split("|")[0] == "datetime":
                        tz = v[0].split("|")[1]
                        new_schema.append(
                            bigquery.SchemaField(
                                k, "DATETIME", "REPEATED", f"At time zone {tz}"
                            )
                        )
            # (C) datetime single fields
            elif isinstance(v, str):
                if k in [x.name for x in bq_schema]:
                    # this field already exists
                    new_schema.append([x for x in bq_schema if x.name == k][0])
                else:
                    if v == "datetime|UTC":
                        new_schema.append(
                            bigquery.SchemaField(k, "TIMESTAMP", "NULLABLE")
                        )
                    elif v == "datetime|None":
                        new_schema.append(
                            bigquery.SchemaField(k, "DATETIME", "NULLABLE")
                        )
                    else:
                        tz = v.split("|")[1]
                        new_schema.append(
                            bigquery.SchemaField(
                                k, "DATETIME", "NULLABLE", f"At time zone {tz}."
                            )
                        )
            # (D) this is either a record or a repeated record/array of records
            elif isinstance(v, dict):
                mode = "REPEATED" if py_schema[k]["BQSCHEMA_repeated"] else "NULLABLE"
                if k in [x.name for x in bq_schema]:
                    # already exists, just check fields and update if necessary
                    existing_field = [x for x in bq_schema if x.name == k][0]
                    new_fields = self._rec_build_schema(existing_field.fields, v)
                    if (
                        existing_field.mode == mode
                        and new_fields == existing_field.fields
                    ):
                        new_schema.append(existing_field)
                    else:
                        if existing_field.mode == "REPEATED" or mode == "REPEATED":
                            # if either repeats, update
                            mode == "REPEATED"
                        else:
                            mode = existing_field.mode
                        new_schema.append(
                            bigquery.SchemaField(k, "RECORD", mode, fields=new_fields)
                        )
                else:
                    # this is new
                    new_fields = self._rec_build_schema([], v)
                    new_schema.append(
                        bigquery.SchemaField(k, "RECORD", mode, fields=new_fields)
                    )
        return new_schema

    def _rec_to_iso(
        self, json_object: Union[dict, list[dict]]
    ) -> Union[dict, list[dict]]:
        """
        - recursive_to_iso
        Converts python datetime/date/time values into ISO strings to be read by BQ.

        Returns:
            new/json_object: Converted dictionary, or list of dictionary rows, depending
                on input data format.
        """

        if isinstance(json_object, list):
            new = []
            for jo in json_object:
                new.append(self._rec_to_iso(jo))
            return new
        elif isinstance(json_object, dict):
            for k, v in json_object.items():
                if isinstance(v, (datetime, date, time)):
                    json_object[k] = v.isoformat()
                elif isinstance(v, dict):
                    json_object[k] = self._rec_to_iso(json_object[k])
                elif isinstance(v, list):
                    newlist = []
                    for i in v:
                        if isinstance(i, dict):
                            newlist.append(self._rec_to_iso(i))
                        else:
                            newlist.append(
                                i.isoformat()
                                if isinstance(i, (datetime, date, time))
                                else i
                            )
                    json_object[k] = newlist
                else:
                    json_object[k] = v
            return json_object

    def _rec_convert_decimal(
        self, json_object: Union[dict, list[dict]]
    ) -> Union[dict, list[dict]]:
        """
        - recursive_convert_decimal
        Recursively changes Decimals in json_object to floats,
        as BQ cannot parse Decimals for write (but the read returns
        NUMERIC as Decimal.
        """
        if isinstance(json_object, list):
            new = []
            for jo in json_object:
                new.append(self._rec_convert_decimal(jo))
            return new
        elif isinstance(json_object, dict):
            for k, v in json_object.items():
                if isinstance(v, (Decimal)):
                    json_object[k] = str(v)
                elif isinstance(v, dict):
                    json_object[k] = self._rec_convert_decimal(json_object[k])
                elif isinstance(v, list):
                    newlist = []
                    for i in v:
                        if isinstance(i, dict):
                            newlist.append(self._rec_convert_decimal(i))
                        else:
                            newlist.append(str(i) if isinstance(i, (Decimal)) else i)
                    json_object[k] = newlist
                else:
                    json_object[k] = v
            return json_object

    def _configure_load_job(
        self,
        schema: Optional[Any] = None,
        replace: bool = False,
        partition: bool = False,
    ) -> Any:
        """
        - _configure_json_load_job
        Create BQ load job configuration.

        Args:
            autodetect: whether schema should be autodetected based on json data to write.
            schema: provide biquery schema; if None, uses bq autodetect.
            write_disposition: choose between 'append' and 'overwrite'
            partitioning: If true, partitions on partition_date field.

        Returns:
            load_job : biquery.job.load.LoadJobConfig

        NOTE: Additional properties and methods exist. See
            https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.LoadJobConfig
        """
        # configure jobs
        if replace:
            if schema and partition:
                job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    time_partitioning=bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="partition_date",  # Name of the column to use for partitioning.
                    ),
                    encoding="utf-8",
                )
            elif schema and not partition:
                job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    encoding="utf-8",
                )
            elif not schema and partition:
                job_config = bigquery.LoadJobConfig(
                    autodetect=True,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    time_partitioning=bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="partition_date",  # Name of the column to use for partitioning.
                    ),
                    encoding="utf-8",
                )
            elif not schema and not partition:
                job_config = bigquery.LoadJobConfig(
                    autodetect=True,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    encoding="utf-8",
                )
        else:
            if schema and partition:
                job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    schema_update_options=[
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
                    ],
                    time_partitioning=bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="partition_date",  # Name of the column to use for partitioning.
                    ),
                    encoding="utf-8",
                )
            elif schema and not partition:
                job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    schema_update_options=[
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
                    ],
                    encoding="utf-8",
                )
            elif not schema and partition:
                job_config = bigquery.LoadJobConfig(
                    autodetect=True,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    schema_update_options=[
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
                    ],
                    time_partitioning=bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="partition_date",  # Name of the column to use for partitioning.
                    ),
                    encoding="utf-8",
                )
            elif not schema and not partition:
                job_config = bigquery.LoadJobConfig(
                    autodetect=True,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                    schema_update_options=[
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
                    ],
                    encoding="utf-8",
                )
        return job_config

    def list_datasets(self, project: Optional[str] = None) -> list[str]:
        """
        - list_bq_datasets
        Retrieves a list of available datasets for a given BQ project, sorted
        in alphabetical order (asc).
        If no project is specified, uses client default project.
        """
        project = project if project else self._project_id
        field = "schema_name"
        query = f"SELECT {field} FROM `{project}`.INFORMATION_SCHEMA.SCHEMATA"
        data = self.get(query)
        datasets = sorted([d[field] for d in data])
        return datasets

    def get(self, query: str) -> list[dict]:
        """
        - get_json_from_gbq
        Post an SQL query as a job to BigQuery and get the result as list[dict].
        """
        query_job = self._client.query(query)
        json_list = [dict(row) for row in query_job]
        # NOTE: BQ returns timestamps with tzinfo that is not compatible with datetime.timezone
        for d in json_list:
            for k, v in d.items():
                if isinstance(v, datetime):
                    d[k] = (
                        v.replace(tzinfo=timezone.utc) if f"{v.tzinfo}" == "UTC" else v
                    )
        return json_list

    def get_table(
        self,
        table_id: str,
        order_by: Optional[str] = None,
        row_limit: Optional[int] = None,
    ):
        """
        - get_json_from_gbq_table
        Returns data in BQ table as python dictionary, with optional data ordering and row limit.
        NOTE: for more specific data selection, use get_json_from_gbq TODO get()
        """
        query = (
            f"SELECT * FROM `{table_id}` ORDER BY {order_by}"
            if order_by
            else f"SELECT * FROM `{table_id}`"
        )
        query = f"{query} LIMIT {row_limit}" if row_limit else query
        json_list = self.get(query)
        return json_list

    def write(
        self,
        json_list: list[dict],
        table_id: str,
        replace: bool = False,
        partition: bool = False,
        py_schema: Optional[Any] = None,
        bq_detect: bool = False,
        use_datetime: bool = True,
    ) -> Any:
        """
        - write_json_to_gbq
        Take raw JSON data, prepare and write to BQ.
        NOTE:
        - Partition ONLY supports day partitioning on a partition_date field. Anything else must be custom.
        - If data contains dates of format YYYY-MM-DD HH:MM:SS, BQ autodetect will assume it is UTC and use
            TIMESTAMP datatype. If timeinfo is given (e.g. YYYY-MM-DD HH:MM:SS+05:00), BQ will use timestamp
            in UTC and adjust value (here by subtracting 5h).
            => if you want to keep local time as DATETIME select bq_detect = False. NOTE: will loose tz info, if
                not mentioned in description, BUT prevents changing TODO
            => if you you want to use schema detectyou have bq_detect = False, but would like to convert localised datetimes to UTC, select
                keep_local_time = False.

        Args:
            json_list: Data to write to BQ, formatted as list of data rows as dict. NOTE: if dates/datetimes are formatted as str, schema will
                assume string unless separate schema is provided.
            table_id: Location of BQ table in format <project>.<dataset>.<table>
            replace: Whether a new table should be created, overwriting any existing table of same name.
            partition: Whether table should be partitioned on partition_date field.
            schema: Explicit schema to pass for writing data, if needed. Format: internal python schema format.
            bq_detect: Whether BQ's schema autodetect feature should be used instead of barklion schema detection.
            use_datetime: if False, uses string to represent data stored as python datetime formats. if bq_detect = True, is automatically set to True.
        """
        # setup
        bq_detect = False if py_schema else bq_detect
        use_datetime = True if bq_detect else use_datetime
        # (1) format json for BQ
        cleaned = []
        for jo in json_list:
            ko = self._rec_remove_nulls(jo)
            lo = self._rec_format_keys(ko)
            cleaned.append(lo)
        # (2) write json to BQ
        if replace:
            # (a) replace
            if bq_detect:
                job_config = self._configure_load_job(replace=True, partition=partition)
            else:
                new_schema = (
                    py_schema
                    if py_schema
                    else self._rec_build_py_schema(cleaned, use_datetime=use_datetime)
                )
                replacement_schema = self._rec_build_schema([], new_schema)
                job_config = self._configure_load_job(
                    schema=replacement_schema, replace=True, partition=partition
                )
        else:
            # (b) append
            try:
                # check whether table already exists
                table = self._client.get_table(table_id)
                if bq_detect:
                    # GENERALLY NOT ADVISED WHEN APPENDING TO TABLE
                    job_config = self._configure_load_job(
                        replace=False, partition=partition
                    )
                else:
                    # BETTER SOLUTION
                    new_schema = (
                        py_schema if py_schema else self._rec_build_py_schema(cleaned)
                    )
                    bq_schema = table.schema
                    replacement_schema = self._rec_build_schema(bq_schema, new_schema)
                    job_config = self._configure_load_job(
                        schema=replacement_schema, replace=False, partition=partition
                    )
            except NotFound:
                # table does not exist
                if bq_detect:
                    job_config = self._configure_load_job(
                        replace=False, partition=partition
                    )
                else:
                    new_schema = (
                        py_schema if py_schema else self._rec_build_py_schema(cleaned)
                    )
                    replacement_schema = self._rec_build_schema([], new_schema)
                    job_config = self._configure_load_job(
                        schema=replacement_schema, replace=False, partition=partition
                    )
        # (3) write to BQ
        cleaned_iso = self._rec_to_iso(
            cleaned
        )  # datetime/date/time conversion to ISO string needed for BQ to parse value
        cleaned_iso_dec = self._rec_convert_decimal(cleaned_iso)  # handles Decimal
        load_job = self._client.load_table_from_json(
            json_rows=cleaned_iso_dec,
            destination=table_id,
            job_config=job_config,
            num_retries=3,
        )  # Make an API request.
        try:
            load_job.result()
            return True
        except GoogleAPIError as e:
            raise RuntimeError(
                f"Google API Error: {e} . Contents of error result: {load_job.error_result} . Contents of errors: {load_job.errors} ."
            )

    def delete_table_rows(
        self, table_id: str, where: Union[str, bool] = True
    ) -> Literal[True]:
        """
        - delete_bq_table_rows
        [DANGER] Deletes (!) table content.
        NOTE: if where = True, all (!) content is deleted.
        TODO: maybe have table under json?
        """
        query = f"DELETE {table_id} WHERE {where}"
        data = self.get(query)
        return True

    def delete_table_cols(
        self, table_id: str, col_names: str, timetravel: bool = False
    ) -> Literal[True]:
        """
        - delete_bq_table_cols
        [DANGER] Removes specified columns from BQ table (including their content)
        Args:
            table_id: full BQ id in format <project>.<dataset>.<table>
            col_names: single column name, or name of columns separated by commas
            timetravel : If true, and only 1 col name is given, means the col is dropped
                from the table, and name cannot be reused during BQ timetravel duration (ca. 7 days).
                Reason: For that duration, BQ keeps a records of changes made to the table.
                NOTE: This uses less data, than having to copy entire table onto itself, excluding provided columns.
        """
        multi_col = "," in col_names
        query = (
            f"ALTER TABLE `{table_id}` DROP COLUMN {col_names}"
            if timetravel and not multi_col
            else f"CREATE OR REPLACE TABLE `{table_id}` AS SELECT * EXCEPT ({col_names}) FROM `{table_id}`"
        )
        data = self.get(query)
        return True


class JsonData(Json):
    """Methods w/o API calls that manipulate Json-ic (list or list[dict]) data."""

    def flatten(self, data: Union[dict, list[dict]]) -> Union[dict, list[dict]]:
        """
        - flatten_json

        Takes a pythonic json and flattens the data.
        Handles both single rows (dict) and multiple rows (list[dict]).

        NOTE:
            - original data is NOT overwritten.
            - does not create new rows, thus field values that are lists will be unnested
                to individually numbered fields. May create large number of fields.
                Use with caution.
        Example:
            data = {'a': 1, 'b': {'z': 1, 'y': 2}, 'c': [1, 2, 3], 'd': [{'aa': 11, 'bb': 22}, {'aa': 33, 'bb': 44}]}
            returns = {'a': 1, 'b_z': 1, 'b_y': 2, 'c_0': 1, 'c_1': 2, 'c_2': 3, 'd_0_aa': 11, 'd_0_bb': 22, 'd_1_aa': 33, 'd_1_bb': 44}
        """

        def _rec_flatten(data, flat_dict: dict = {}, name: str = ""):
            if isinstance(data, dict):
                for d in data:
                    _rec_flatten(data[d], flat_dict, name + d + "_")
            elif isinstance(data, list):
                i = 0
                for d in data:
                    _rec_flatten(d, flat_dict, name + str(i) + "_")
                    i += 1
            else:
                flat_dict[name[:-1]] = data
            return flat_dict

        # main
        if isinstance(data, list):
            flat_list = []
            for d in data:
                flat_list.append(_rec_flatten(d, {}))
            return flat_list
        elif isinstance(data, dict):
            flat_dict = _rec_flatten(data, {})
            return flat_dict

    def create_subsample(
        self, json_list: list[dict], keys_list: list[str], exclude_keys=False
    ) -> list[dict]:
        """
        - create_dict_subsample

        Returns a subset of key/value pairs for a given list[dict].

        Args:
            json_list : original data in list[dict] format
            keys_list : keys to be included/excluded for selection (see below).
            exclude_keys : if False, returns data for keys in keys_list only,
                if True, returns data for all keys except those in keys_list.
        """
        subset = []
        for d in json_list:
            d_new = (
                {k: d[k] for k in d if k not in keys_list}
                if exclude_keys
                else {k: d.get(k) for k in keys_list}
            )
            subset.append(d_new)
        return subset

    def find_new_rows(
        self,
        data: list[dict],
        orig_data: list[dict],
        keys: Optional[list[str]] = None,
        exclude: bool = False,
        return_all: bool = True,
        remainder: bool = False,
    ):
        """
        - find_new_rows_in_dict
        Finds data that has changed: checks what data is found in data, that is not in orig_data,
        option to limit comparison to certain named keys, and/or to have already_existing data returned.

        Args:
            data : (json) data that contains possible new data in list[dict] format.
            orig_data : (json) original data to compare 'data' against
            keys : list of key names for which data should be compared/not compared (see exclude option).
                If set to None, all keys get compared
            exclude :  If False, values for key listed in keys compared only; if True, all key/values except for values for keys in
                keys are compared.
            return_all : if False, only data for compared keys is returned, if True, all key/value pairs for rows with new data
                are returned.
            remainder: If True, will also return data is unchaged, not new.

        NOTE: doesn't currenlt identify rows that are in orig_data, but no longer in data!
        """
        # 1. reduce data to relevant keys
        check = (
            self.create_subsample(data, keys, exclude_keys=exclude) if keys else data
        )
        orig = (
            self.create_subsample(orig_data, keys, exclude_keys=exclude)
            if keys
            else orig_data
        )
        # 2. find new data rows
        new_subdata = [d for d in check if d not in orig]

        # 3. return entire data row or only for key selection compared
        if return_all and keys:
            if not exclude:
                new_data = [
                    row for row in data if {k: row[k] for k in keys} in new_subdata
                ]
            elif exclude:
                new_data = [
                    row
                    for row in data
                    if {k: row[k] for k in row if k not in keys} in new_subdata
                ]
        else:
            new_data = new_subdata

        # 4. return data that is not new.
        if remainder:
            data_left = [
                d for d in data if d not in new_data
            ]  # NOTE: MAY HAVE HAD BUG IN PAST
            return new_data, data_left
        else:
            return new_data

    def find_new_values():
        # TODO
        """Identifies new values in existing data rows, as well as in new keys."""
        pass

    def remove_nulls(self, json_object: dict) -> dict:
        """Removes nulls from data in data row (dict)"""
        return self._rec_remove_nulls(json_object)

    def to_iso(self, json_object: Union[dict, list[dict]]) -> Union[dict, list[dict]]:
        """Turns datetime objects (e.g. datetime, date, time) into ISO formatted strings."""
        return self._rec_to_iso(json_object)


class BigQueryClient(object):
    def __init__(
        self,
        logger: Logger,
        project_id: Optional[str] = None,
        region: Optional[str] = "US",  # default for GCP projects is 'US'
        run_loc: Literal["app", "local"] = "app",
    ):
        """
        project_id : project from which credentials for service account should be retrieved, not
            necessarily the GCP/BQ project from/to which data is beingn read/written.
        region : region in which the service account for the app execution should operate (should
            correspond to the dataset regions the app creates/modifies/queries)
        run_location : if set to 'local', uses local machine credentials for authentication.
        """
        self._project_id = project_id
        self._credentials, project_runtime = google.auth.default(
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        self._region = region
        if run_loc == "app":
            self._client = bigquery.Client(
                project=self._project_id,
                credentials=self._credentials,
                location=self._region,
            )
        elif run_loc == "local":
            self._client = bigquery.Client(location=self._region)
        self._logger = logger

        # subclients
        self.df = Pandas(self)
        self.df_data = PandasData(self)
        self.js = Json(self)
        self.js_data = JsonData(self)
        self.schema = Schema(self)
        self.schema_data = SchemaData(self)

    def table_exists(self, table_id: str) -> bool:
        """
        Check whether a table already exists in BigQuery.
        TODO: Q - has API call, so where should I put this?
        """
        try:
            self._client.get_table(table_id)
            self._logger.info("Table already exists in BigQuery.")
            return True
        except NotFound:
            self._logger.warning("Table does not (yet) exist in BigQuery.")
            return False

    def table_id(self, project: Optional[str], dataset: str, table: str) -> str:
        """
        - format_table_id

        Creates BQ table ID string based on project, dataset, and table name.
        NOTE: if project is None, uses BQ client project setting.
        """
        project = project if project else self._project_id
        table_id = ".".join([project, dataset, table])
        return table_id


# -------------------

# TODO: ADD TO AIRTABLE CLIENT!
# def find_updated_values(
#     self, at_client, check_data: list[dict], base_data: list[dict], uid: list[str]
# ):
#     """
#     Returns dict items from check_data that are different to base_data, based on
#     records matching via key names provided in uid. Automatically adds relevant
#     AT record ID.
#     TODO: decide whether this function should be adjusted for BQ client, originally from
#         metadata client.
#     """
#     updated = []
#     old = []
#     for row in check_data:
#         base_list = list(
#             filter(lambda d: all(d[key] == row[key] for key in uid), base_data)
#         )
#         if base_list:
#             if len(base_list) > 1:
#                 raise RuntimeError(
#                     f"Using UID {uid} led to duplicate records found: {base_list}"
#                 )
#             base_row = base_list[0]
#             # pick k/v not in base_row, and prevent updating empty in AT with empty BQ.
#             new_row = {
#                 k: v
#                 for (k, v) in row.items()
#                 if (k, v) not in base_row.items()
#                 and not (k not in base_row.keys() and v == "")
#             }
#             # only add dicts with changed data.
#             if new_row:
#                 new_row[at_client.at_rec_id] = base_row[at_client.at_rec_id] # TODO update
#                 updated.append(new_row)
#                 old_row = {
#                     k: base_row[k]
#                     for k in list(new_row.keys())
#                     if k in list(base_row.keys())
#                 }
#                 old.append(old_row)
#     return updated, old
