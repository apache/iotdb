import logging
from typing import Dict, List, Optional

import numpy as np
import torch

from standalone_finetune.data_provider.datasets.base_dataset import (
    BasicTimeSeriesDataset,
)
from standalone_finetune.data_provider.processor.data_scaler import ScalerType

logger = logging.getLogger(__name__)


def _get_numeric_value(field):
    """Extract numeric value from an IoTDB Field; return None for null / non-numeric."""
    if field is None:
        return None
    try:
        from iotdb.utils.IoTDBConstants import TSDataType

        dt = field.get_data_type()
        if dt is None:
            return None
        if dt == TSDataType.INT32:
            return float(field.get_int_value())
        if dt == TSDataType.INT64:
            return float(field.get_long_value())
        if dt == TSDataType.FLOAT:
            return float(field.get_float_value())
        if dt == TSDataType.DOUBLE:
            return float(field.get_double_value())
    except Exception:
        pass
    return None


class IoTDBTreeDataset(BasicTimeSeriesDataset):
    """
    Dataset that executes a SQL on IoTDB (tree model) via ``Session``,
    flattens all ``field0`` values from every device into one single series,
    and applies a sliding window — same philosophy as ``TsFileTreeDataset``.
    """

    def __init__(
        self,
        sql: str,
        iotdb_ip: str = "172.29.224.62",
        iotdb_port: int = 6667,
        iotdb_username: str = "root",
        iotdb_password: str = "root",
        train_ratio: float = 0.7,
        seq_len: int = 2880,
        input_token_len: int = 16,
        output_token_lens: List[int] = None,
        window_step: int = 1,
        scale: bool = False,
        scaler_type: ScalerType = "standard",
        use_rate: float = 1.0,
        offset_rate: float = 0.0,
    ):
        if output_token_lens is None:
            output_token_lens = [96]
        super().__init__(
            seq_len,
            input_token_len,
            output_token_lens,
            window_step,
            scale,
            scaler_type,
            use_rate,
            offset_rate,
        )

        self.sql = sql
        self.train_ratio = train_ratio
        self._data: Optional[np.ndarray] = None
        self.total_windows: int = 0
        self._window_offset: int = 0

        self._session = self._open_session(
            iotdb_ip, iotdb_port, iotdb_username, iotdb_password
        )
        self._init_dataset()

    @staticmethod
    def _open_session(ip: str, port: int, username: str, password: str):
        from iotdb.Session import Session

        session = Session.init_from_node_urls(
            node_urls=[f"{ip}:{port}"],
            user=username,
            password=password,
        )
        session.open(False)
        return session

    def _fetch_data(self) -> np.ndarray:
        """Execute SQL, flatten all numeric field0 values into one series."""
        values: List[float] = []

        self._session.execute_query_statement("SELECT field0 FROM root.consensus.client11.*;")
        self._session.execute_query_statement("SELECT field0 FROM root.consensus.client12.*;")
        self._session.execute_query_statement("SELECT field0 FROM root.consensus.client13.*;")
        self._session.execute_query_statement("SELECT field0 FROM root.consensus.client14.*;")
        self._session.execute_query_statement("SELECT field0 FROM root.consensus.client15.*;")
        self._session.execute_query_statement("SELECT field0 FROM root.consensus.client116.*;")
        # self._session.execute_query_statement("SELECT field0 FROM root.consensus.client117.*;")
        # self._session.execute_query_statement("SELECT field0 FROM root.consensus.client118.*;")
        # self._session.execute_query_statement("SELECT field0 FROM root.consensus.client119.*;")
        # self._session.execute_query_statement("SELECT field0 FROM root.consensus.client110.*;")

        logger.info(f"Executing SQL: {self.sql}")
        with self._session.execute_query_statement(self.sql) as result:
            while result.has_next():
                row = result.next()
                for field in row.get_fields():
                    v = _get_numeric_value(field)
                    if v is not None:
                        values.append(v)

        if not values:
            logger.warning("SQL query returned no numeric data")
            return np.array([], dtype=np.float32)

        logger.info(f"Loaded {len(values)} data points from IoTDB")
        return np.array(values, dtype=np.float32)

    def _init_dataset(self):
        self._data = self._fetch_data()

        if len(self._data) == 0:
            logger.warning("No valid data loaded from IoTDB SQL query")
            return

        if self.scale:
            train_end = int(len(self._data) * self.train_ratio)
            self.fit_scaler(self._data[:train_end])
            self._data = self.transform(self._data)

        window_length = self.seq_len + self.output_token_len
        if len(self._data) < window_length:
            logger.warning(
                f"Data length ({len(self._data)}) < window length ({window_length})"
            )
            return

        full_count = (len(self._data) - window_length) // self.window_step + 1
        self.total_windows = int(full_count * self.use_rate)
        self._window_offset = int(full_count * self.offset_rate)

        logger.info(f"Data window amount is {self.total_windows}")

    def __getitem__(self, index: int) -> Dict[str, torch.Tensor]:
        if index < 0 or index >= self.total_windows:
            raise IndexError(f"Index {index} out of range [0, {self.total_windows})")

        start = (index + self._window_offset) * self.window_step
        window_length = self.seq_len + self.output_token_len
        window = self._data[start : start + window_length]

        seq_x = torch.from_numpy(window[: self.seq_len].copy())
        seq_y = torch.from_numpy(window[self.input_token_len : window_length].copy())
        loss_mask = torch.ones(self.token_num, dtype=torch.float32)

        return {"seq_x": seq_x, "seq_y": seq_y, "loss_mask": loss_mask}

    def __len__(self) -> int:
        return self.total_windows

    def close(self):
        if hasattr(self, "_session") and self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
