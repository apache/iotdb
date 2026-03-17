from abc import ABC, abstractmethod
from typing import Dict, Literal, Optional, Union

import numpy as np
import torch

SUPPORTED_SCALERS = ["standard", "minmax", "none"]
ScalerType = Literal["standard", "minmax", "none"]


class BaseScaler(ABC):
    """Abstract base class for all scalers."""

    def __init__(self, eps: float = 1e-8):
        self.eps = eps
        self.is_fitted: bool = False

    @abstractmethod
    def fit(self, data: np.ndarray) -> "BaseScaler":
        pass

    @abstractmethod
    def transform(
        self, data: Union[np.ndarray, torch.Tensor]
    ) -> Union[np.ndarray, torch.Tensor]:
        pass

    @abstractmethod
    def inverse_transform(
        self, data: Union[np.ndarray, torch.Tensor]
    ) -> Union[np.ndarray, torch.Tensor]:
        pass

    def fit_transform(self, data: np.ndarray) -> np.ndarray:
        self.fit(data)
        return self.transform(data)

    @abstractmethod
    def get_params(self) -> Dict[str, np.ndarray]:
        pass

    @abstractmethod
    def set_params(self, **kwargs) -> None:
        pass

    def _to_numpy(self, data: Union[np.ndarray, torch.Tensor]) -> tuple:
        is_tensor = isinstance(data, torch.Tensor)
        if is_tensor:
            device, dtype = data.device, data.dtype
            data = data.cpu().numpy()
            return data, True, device, dtype
        return data, False, None, None

    def _to_tensor(
        self, result: np.ndarray, is_tensor: bool, device, dtype
    ) -> Union[np.ndarray, torch.Tensor]:
        if is_tensor:
            return torch.from_numpy(result).to(device=device, dtype=dtype)
        return result


class StandardScaler(BaseScaler):
    """Standard scaling (z-score normalization): (x - mean) / std"""

    def __init__(self, eps: float = 1e-8):
        super().__init__(eps)
        self.mean: Optional[np.ndarray] = None
        self.std: Optional[np.ndarray] = None

    def fit(self, data: np.ndarray) -> "StandardScaler":
        data = np.asarray(data, dtype=np.float32)
        if data.ndim == 1:
            self.mean = np.nanmean(data)
            self.std = np.nanstd(data)
            self.std = max(self.std, self.eps)
        else:
            self.mean = np.nanmean(data, axis=0, keepdims=True)
            self.std = np.nanstd(data, axis=0, keepdims=True)
            self.std = np.where(self.std < self.eps, 1.0, self.std)
        self.is_fitted = True
        return self

    def transform(
        self, data: Union[np.ndarray, torch.Tensor]
    ) -> Union[np.ndarray, torch.Tensor]:
        if not self.is_fitted:
            raise RuntimeError("Scaler must be fitted before transform")
        data, is_tensor, device, dtype = self._to_numpy(data)
        result = (data - self.mean) / self.std
        return self._to_tensor(result, is_tensor, device, dtype)

    def inverse_transform(
        self, data: Union[np.ndarray, torch.Tensor]
    ) -> Union[np.ndarray, torch.Tensor]:
        if not self.is_fitted:
            raise RuntimeError("Scaler must be fitted before inverse_transform")
        data, is_tensor, device, dtype = self._to_numpy(data)
        result = data * self.std + self.mean
        return self._to_tensor(result, is_tensor, device, dtype)

    def get_params(self) -> Dict[str, np.ndarray]:
        return {"mean": self.mean, "std": self.std}

    def set_params(
        self, mean: np.ndarray = None, std: np.ndarray = None, **kwargs
    ) -> None:
        if mean is not None:
            self.mean = mean
        if std is not None:
            self.std = std
        if self.mean is not None and self.std is not None:
            self.is_fitted = True


class MinMaxScaler(BaseScaler):
    """Min-Max scaling to specified range: (x - min) * scale + range_min"""

    def __init__(self, feature_range: tuple = (0, 1), eps: float = 1e-8):
        super().__init__(eps)
        self.feature_range = feature_range
        self.min: Optional[np.ndarray] = None
        self.max: Optional[np.ndarray] = None
        self.scale: Optional[np.ndarray] = None

    def fit(self, data: np.ndarray) -> "MinMaxScaler":
        data = np.asarray(data, dtype=np.float32)
        if data.ndim == 1:
            self.min = np.nanmin(data)
            self.max = np.nanmax(data)
        else:
            self.min = np.nanmin(data, axis=0, keepdims=True)
            self.max = np.nanmax(data, axis=0, keepdims=True)

        data_range = self.max - self.min
        if isinstance(data_range, np.ndarray):
            data_range = np.where(data_range < self.eps, 1.0, data_range)
        else:
            data_range = max(data_range, self.eps)

        feature_range = self.feature_range[1] - self.feature_range[0]
        self.scale = feature_range / data_range
        self.is_fitted = True
        return self

    def transform(
        self, data: Union[np.ndarray, torch.Tensor]
    ) -> Union[np.ndarray, torch.Tensor]:
        if not self.is_fitted:
            raise RuntimeError("Scaler must be fitted before transform")
        data, is_tensor, device, dtype = self._to_numpy(data)
        result = (data - self.min) * self.scale + self.feature_range[0]
        return self._to_tensor(result, is_tensor, device, dtype)

    def inverse_transform(
        self, data: Union[np.ndarray, torch.Tensor]
    ) -> Union[np.ndarray, torch.Tensor]:
        if not self.is_fitted:
            raise RuntimeError("Scaler must be fitted before inverse_transform")
        data, is_tensor, device, dtype = self._to_numpy(data)
        result = (data - self.feature_range[0]) / self.scale + self.min
        return self._to_tensor(result, is_tensor, device, dtype)

    def get_params(self) -> Dict[str, np.ndarray]:
        return {"min": self.min, "max": self.max, "scale": self.scale}

    def set_params(
        self,
        min: np.ndarray = None,
        max: np.ndarray = None,
        scale: np.ndarray = None,
        **kwargs,
    ) -> None:
        if min is not None:
            self.min = min
        if max is not None:
            self.max = max
        if scale is not None:
            self.scale = scale
        if self.min is not None and self.max is not None and self.scale is not None:
            self.is_fitted = True


class NoneScaler(BaseScaler):
    """No-op scaler that passes through data unchanged."""

    def __init__(self, eps: float = 1e-8):
        super().__init__(eps)
        self.is_fitted = True

    def fit(self, data: np.ndarray) -> "NoneScaler":
        return self

    def transform(
        self, data: Union[np.ndarray, torch.Tensor]
    ) -> Union[np.ndarray, torch.Tensor]:
        return data

    def inverse_transform(
        self, data: Union[np.ndarray, torch.Tensor]
    ) -> Union[np.ndarray, torch.Tensor]:
        return data

    def get_params(self) -> Dict[str, np.ndarray]:
        return {}

    def set_params(self, **kwargs) -> None:
        pass


class RevINScaler:
    """
    Reversible Instance Normalization (RevIN) scaler.

    This scaler normalizes each instance independently at runtime,
    different from StandardScaler/MinMaxScaler which fit on training data.
    """

    def __init__(self, eps: float = 1e-5):
        self.eps = eps

    def normalize(self, data: torch.Tensor, dim: int = 1) -> tuple:
        mean = data.mean(dim=dim, keepdim=True)
        std = data.std(dim=dim, keepdim=True)
        std = torch.clamp(std, min=self.eps)
        normalized = (data - mean) / std
        stats = {"mean": mean, "std": std}
        return normalized, stats

    def denormalize(self, data: torch.Tensor, stats: dict) -> torch.Tensor:
        mean, std = stats["mean"], stats["std"]
        if data.shape[1] != mean.shape[1]:
            mean = mean[:, -1:, :]
            std = std[:, -1:, :]
        return data * std + mean


def get_scaler(scaler_type: ScalerType = "standard", **kwargs) -> BaseScaler:
    """
    Factory function to create a scaler instance.

    Args:
        scaler_type: Type of scaler ("standard", "minmax", "none")
        **kwargs: Additional arguments for the scaler

    Returns:
        BaseScaler instance
    """
    if scaler_type == "standard":
        return StandardScaler(**kwargs)
    elif scaler_type == "minmax":
        return MinMaxScaler(**kwargs)
    elif scaler_type == "none":
        return NoneScaler(**kwargs)
    else:
        raise ValueError(
            f"Unknown scaler_type: {scaler_type}. Supported: {SUPPORTED_SCALERS}"
        )
