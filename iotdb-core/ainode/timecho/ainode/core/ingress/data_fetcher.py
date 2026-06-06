from typing import Dict, List, Tuple

import pandas as pd
import torch

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.ingress.iotdb import get_field_value
from iotdb.ainode.core.log import Logger
from iotdb.table_session import TableSession, TableSessionConfig
from iotdb.utils.Field import Field
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.RowRecord import RowRecord

logger = Logger()


class IoTDBDataFetcher:
    DEFAULT_TAG = "__DEFAULT_TAG__"

    def __init__(
        self,
        ip: str = AINodeDescriptor().get_config().get_ain_cluster_ingress_address(),
        port: int = AINodeDescriptor().get_config().get_ain_cluster_ingress_port(),
        username: str = AINodeDescriptor()
        .get_config()
        .get_ain_cluster_ingress_username(),
        password: str = AINodeDescriptor()
        .get_config()
        .get_ain_cluster_ingress_password(),
    ):
        # Create a table session config
        table_session_config = TableSessionConfig(
            node_urls=[f"{ip}:{port}"],
            username=username,
            password=password,
            use_ssl=AINodeDescriptor()
            .get_config()
            .get_ain_cluster_ingress_ssl_enabled(),
            ca_certs=AINodeDescriptor().get_config().get_ain_thrift_ssl_cert_file(),
        )
        self.session = TableSession(table_session_config)  # Connect to IoTDB

    def fetch_data(
        self, sql_query: str
    ) -> Tuple[Dict[Tuple, Dict[str, torch.Tensor]], Dict[Tuple, List[int]]]:
        """
        Fetch numerical data from IoTDB and organize by tags.

        Arguments:
        - sql_query: The SQL query to execute.

        Returns:
        - series_map: Dict[tag_combination, Dict[column_name, torch.Tensor]],
          each tensor contains the values sorted by timestamp.
        - timestamps_map: Dict[tag_combination, List[int]], the sorted timestamps for each tag.
        """
        series_map: Dict[Tuple, Dict[str, List[int | float]]] = {}
        timestamps_map: Dict[Tuple, List[int]] = {}

        TS_VALUE_DATA_TYPE = (
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
        )

        if not sql_query:
            logger.warning("Empty query provided.")
            return {}, {}

        with self.session.execute_query_statement(sql_query) as target_data:
            # get columns' meta information
            time_col, value_cols, tag_cols = -1, [], []
            column_names = target_data.get_column_names()
            for i, type in enumerate(target_data.get_column_types()):
                if type == TSDataType.TIMESTAMP:
                    time_col = i
                elif type in TS_VALUE_DATA_TYPE:
                    value_cols.append(i)
                elif type == TSDataType.TEXT:
                    tag_cols.append(i)
                else:
                    logger.warning(
                        f"Unsupported data type {type} found in the results of query {sql_query}, which will be ignored."
                    )

            # Check if the data schema is valid
            if time_col == -1 or not value_cols:
                raise ValueError("Invalid data schema, cannot proceed.")
            # Use default tag as tag_values when there are no tag columns
            if not tag_cols:
                tag_values = (self.DEFAULT_TAG,)
                series_map[tag_values] = {}
                timestamps_map[tag_values] = []

            # fetch data by line
            while target_data.has_next():
                cur_data: RowRecord = target_data.next()

                # Retrieve the tag values dynamically (multiple tags)
                if tag_cols:
                    # Combine tag values into a unique key
                    tag_values = tuple(
                        cur_data.get_fields()[i].get_string_value() for i in tag_cols
                    )
                    if tag_values not in series_map:
                        series_map[tag_values] = {}
                        timestamps_map[tag_values] = []

                # construct timestamps_map and series_map
                timestamp: pd.Timestamp = cur_data.get_fields()[
                    time_col
                ].get_timestamp_value()
                timestamps_map[tag_values].append(
                    timestamp.value // 10**6
                )  # covert ns to ms
                for value_col in value_cols:
                    value_col_field: Field = cur_data.get_fields()[value_col]
                    col_name = column_names[value_col]
                    value = get_field_value(value_col_field)
                    if value is None:
                        value = torch.nan

                    if col_name not in series_map[tag_values]:
                        series_map[tag_values][col_name] = []

                    series_map[tag_values][col_name].append(value)

        return self._sort_data_by_timestamp(series_map, timestamps_map)

    def _sort_data_by_timestamp(
        self, series_map, timestamps_map
    ) -> Tuple[Dict[Tuple, Dict[str, torch.Tensor]], Dict[Tuple, List[int]]]:
        """
        Sort each column of series_map according to timestamps.

        Arguments:
        - series_map: Dict[tag_combination, Dict[column_name, List[int|float]]], unsorted values.
        - timestamps_map: Dict[tag_combination, List[int]], timestamps corresponding to values.

        Returns:
        - series_map: same as input, but each column converted to torch.Tensor and sorted.
        - timestamps_map: sorted timestamps.
        """
        for tag_values, cov_map in series_map.items():
            timestamps = timestamps_map[tag_values]
            sorted_indices = sorted(range(len(timestamps)), key=lambda i: timestamps[i])
            timestamps_map[tag_values] = [timestamps[i] for i in sorted_indices]
            for col_name, values_list in cov_map.items():
                sorted_values = [values_list[i] for i in sorted_indices]
                cov_map[col_name] = torch.tensor(sorted_values, dtype=torch.float32)
        return series_map, timestamps_map
