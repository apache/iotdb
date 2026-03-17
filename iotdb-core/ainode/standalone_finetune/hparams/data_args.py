from dataclasses import asdict, dataclass, field
from typing import Any, List, Literal, Optional

SUPPORTED_DATASET_TYPES = ["iotdb.table", "iotdb.tree", "tsfile", "csv", "parquet"]
SUPPORTED_SCALER_TYPES = ["standard", "minmax", "none"]


@dataclass
class DataArguments:
    """
    Arguments pertaining to data loading and preprocessing.

    Example:
        >>> # IoTDB data source
        >>> args = DataArguments(
        ...     dataset_type="iotdb.table",
        ...     data_schema_list=req.targetDataSchema,
        ...     window_step=1,
        ... )
        >>>
        >>> # TsFile data source
        >>> args = DataArguments(
        ...     dataset_type="tsfile",
        ...     data_path="/path/to/data.tsfile",
        ... )
    """

    dataset_type: Literal["iotdb.table", "iotdb.tree", "tsfile", "csv", "parquet"] = field(
        default="tsfile",
        metadata={
            "help": f"Type of data source. Options: {SUPPORTED_DATASET_TYPES}",
            "choices": SUPPORTED_DATASET_TYPES,
        },
    )

    data_schema_list: List[Any] = field(
        default_factory=list,
        metadata={
            "help": (
                "List of schema objects for IoTDB data sources. "
                "Each schema object has 'schemaName' (SQL query or path pattern) "
                "and optional 'timeRange' [start_time, end_time]. "
                "Passed directly from RPC request (req.targetDataSchema)."
            ),
        },
    )

    data_path: str = field(
        default="",
        metadata={
            "help": (
                "Path to data file(s) for non-IoTDB sources. "
                "For TsFile: path to .tsfile file(s). "
                "For CSV/Parquet: path to file or directory."
            ),
        },
    )

    sql: str = field(
        default="",
        metadata={
            "help": (
                "SQL query for IoTDB tree-model data source (dataset_type='iotdb.tree'). "
                "Example: \"SELECT time, field0 FROM root.sg.* WHERE time < 1000000\""
            ),
        },
    )

    iotdb_ip: str = field(
        default="127.0.0.1",
        metadata={"help": "IoTDB server IP address."},
    )

    iotdb_port: int = field(
        default=6667,
        metadata={"help": "IoTDB server RPC port."},
    )

    iotdb_username: str = field(
        default="root",
        metadata={"help": "IoTDB username."},
    )

    iotdb_password: str = field(
        default="root",
        metadata={"help": "IoTDB password."},
    )

    aggregation_interval: str = field(
        default="1s",
        metadata={
            "help": (
                "IoTDB data downsampling/aggregation interval. "
                "Format: number + unit (s=seconds, m=minutes, h=hours, d=days). "
                "Examples: '1s', '5m', '1h', '1d'. "
                "Note: IoTDB connection parameters are read from AINode config."
            ),
        },
    )

    window_step: int = field(
        default=1,
        metadata={
            "help": (
                "Stride for sliding window sampling. "
                "The number of time points shifting between consecutive windows. "
                "Larger stride reduces sample overlap and total samples."
            ),
        },
    )

    train_ratio: float = field(
        default=0.7,
        metadata={
            "help": "Ratio of data used for training (0-1). Default: 0.7",
        },
    )

    val_ratio: float = field(
        default=0.1,
        metadata={
            "help": "Ratio of data used for validation (0-1). Default: 0.1",
        },
    )

    test_ratio: float = field(
        default=0.2,
        metadata={
            "help": "Ratio of data used for testing (0-1). Default: 0.2",
        },
    )

    scale: bool = field(
        default=False,
        metadata={
            "help": (
                "Whether to apply scaling/normalization to the data. "
                "Note: RevIN is typically applied in the model (via use_revin), "
                "so dataset-level scaling is often disabled."
            ),
        },
    )

    scaler_type: Literal["standard", "minmax", "none"] = field(
        default="standard",
        metadata={
            "help": f"Type of scaler for data normalization. Options: {SUPPORTED_SCALER_TYPES}",
            "choices": SUPPORTED_SCALER_TYPES,
        },
    )

    variable_names: Optional[List[str]] = field(
        default=None,
        metadata={
            "help": (
                "Ordered list of variable / column names to use. "
                "Applies to ALL dataset types: column names for CSV/Parquet, "
                "series paths for TsFile ('table.column'), tag names for IoTDB. "
                "Convention: the LAST entry is the prediction target; all "
                "preceding entries are covariates. This is required by adapter "
                "models (e.g. DualWeaver) that use [:, :, -1:] to extract the "
                "target. If None, the dataset uses all available variables."
            ),
        },
    )

    tsfile_preload: bool = field(
        default=True,
        metadata={
            "help": "Whether to preload all TsFile data into memory for faster training.",
        },
    )

    multivariate: bool = field(
        default=False,
        metadata={
            "help": (
                "Whether to use multivariate mode. "
                "If True, multiple variables are combined into a feature matrix "
                "with shape [seq_len, n_variables]. If False, each variable is "
                "treated independently. Auto-enabled when variable_names has "
                "more than one entry."
            ),
        },
    )

    time_column: str = field(
        default="time",
        metadata={
            "help": "Name of the timestamp column in the data.",
        },
    )

    num_workers: int = field(
        default=0,
        metadata={
            "help": "Number of dataloader workers. 0 means main process only.",
        },
    )

    pin_memory: bool = field(
        default=True,
        metadata={
            "help": "Whether to pin memory in dataloader for faster GPU transfer.",
        },
    )

    def __post_init__(self):
        if self.dataset_type not in SUPPORTED_DATASET_TYPES:
            raise ValueError(
                f"Unsupported dataset_type: {self.dataset_type}. "
                f"Supported: {SUPPORTED_DATASET_TYPES}"
            )

        total = self.train_ratio + self.val_ratio + self.test_ratio
        if abs(total - 1.0) > 0.001:
            self.test_ratio = 1.0 - self.train_ratio - self.val_ratio

        if isinstance(self.variable_names, str):
            self.variable_names = [v.strip() for v in self.variable_names.split(",")]

        # Auto-enable multivariate when multiple variables are specified
        if self.is_multivariate and not self.multivariate:
            self.multivariate = True

    @property
    def is_multivariate(self) -> bool:
        return self.variable_names is not None and len(self.variable_names) > 1

    @property
    def is_iotdb(self) -> bool:
        return self.dataset_type.startswith("iotdb")

    @staticmethod
    def _serialize_schema(schema: Any) -> dict:
        """
        Convert IDataSchema (Thrift object) to serializable dict.

        IDataSchema has attributes: schemaName (str), timeRange (list[int])
        """
        if isinstance(schema, dict):
            return schema

        result = {}
        if hasattr(schema, "schemaName"):
            result["schemaName"] = schema.schemaName
        if hasattr(schema, "timeRange") and schema.timeRange:
            result["timeRange"] = list(schema.timeRange)
        return result

    def to_dict(self) -> dict:
        """Convert arguments to dictionary (JSON-serializable)."""
        d = asdict(self)
        d["data_schema_list"] = [
            self._serialize_schema(s) for s in self.data_schema_list
        ]
        return d
