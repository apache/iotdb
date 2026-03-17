from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union

import numpy as np
import torch
from torch.utils.data import Dataset

from standalone_finetune.data_provider.processor.data_scaler import (
    BaseScaler,
    ScalerType,
    get_scaler,
)


class BasicTimeSeriesDataset(Dataset, ABC):
    """
    Base class for time series datasets.
    """

    def __init__(
        self,
        seq_len: int,
        input_token_len: int,
        output_token_lens: List[int],
        window_step: int = 1,
        scale: bool = False,
        scaler_type: ScalerType = "standard",
        use_rate: float = 1.0,
        offset_rate: float = 0.0,
    ):
        """
        Args:
            seq_len: The number of time series data points of the model input
            input_token_len: The number of time series data points of each model token
            output_token_lens: List of prediction horizons (output token lengths).
                The largest value determines the label window size for slicing.
            window_step: Step size for sliding window
            scale: Whether to apply normalization
            scaler_type: Type of scaler to use ("standard", "minmax", "none")
            use_rate: Fraction of data to use
            offset_rate: Starting offset as fraction of total data
        """
        super().__init__()
        self.seq_len = seq_len
        self.input_token_len = input_token_len
        self.output_token_lens = list(output_token_lens)
        # Derived scalar for slicing: use the largest horizon
        self.output_token_len = max(self.output_token_lens)
        self.token_num = seq_len // input_token_len
        self.window_step = window_step
        self.scale = scale
        self.scaler_type = scaler_type
        self.use_rate = use_rate
        self.offset_rate = offset_rate

        self.scaler: Optional[BaseScaler] = get_scaler(scaler_type) if scale else None

    def fit_scaler(self, train_data: np.ndarray) -> None:
        if self.scaler is not None:
            self.scaler.fit(train_data)

    def transform(
        self, data: Union[np.ndarray, torch.Tensor]
    ) -> Union[np.ndarray, torch.Tensor]:
        if self.scaler is not None and self.scaler.is_fitted:
            return self.scaler.transform(data)
        return data

    def inverse_transform(
        self, data: Union[np.ndarray, torch.Tensor]
    ) -> Union[np.ndarray, torch.Tensor]:
        if self.scaler is not None and self.scaler.is_fitted:
            return self.scaler.inverse_transform(data)
        return data

    def get_scaler_params(self) -> Optional[dict]:
        if self.scaler is not None and self.scaler.is_fitted:
            return self.scaler.get_params()
        return None

    def set_scaler_params(self, **kwargs) -> None:
        if self.scaler is None:
            self.scaler = get_scaler(self.scaler_type)
        self.scaler.set_params(**kwargs)

    @abstractmethod
    def __getitem__(self, index: int) -> Dict[str, torch.Tensor]:
        """
        Get a sample by index.

        Returns:
            Dict with keys:
                - "seq_x": Input sequence [seq_len] or [seq_len, n_features]
                - "seq_y": Label sequence [seq_len + max(output_token_lens) - input_token_len]
                           or [seq_len + max(output_token_lens) - input_token_len, n_features]
                           (from data[input_token_len : seq_len + max(output_token_lens)])
                - "loss_mask": Loss mask [token_num]
                - "y_mask" (optional): Output mask, same length as seq_y
        """
        pass

    @abstractmethod
    def __len__(self) -> int:
        pass
