from enum import Enum
from typing import Dict, List, Optional, Union

import numpy as np
import torch


class MetricType(str, Enum):
    """Enumeration of supported evaluation metrics."""

    MSE = "mse"  # Mean Squared Error
    MAE = "mae"  # Mean Absolute Error
    MAPE = "mape"  # Mean Absolute Percentage Error
    RMSE = "rmse"  # Root Mean Squared Error
    SMAPE = "smape"  # Symmetric MAPE
    MASE = "mase"  # Mean Absolute Scaled Error


# Default metrics for different tasks
FORECAST_METRICS: List[str] = [
    MetricType.MSE.value,
    MetricType.MAE.value,
    MetricType.MAPE.value,
]


def mse(
    pred: Union[np.ndarray, torch.Tensor], target: Union[np.ndarray, torch.Tensor]
) -> float:
    """Mean Squared Error."""
    if isinstance(pred, torch.Tensor):
        return torch.mean((pred - target) ** 2).item()
    return float(np.mean((pred - target) ** 2))


def mae(
    pred: Union[np.ndarray, torch.Tensor], target: Union[np.ndarray, torch.Tensor]
) -> float:
    """Mean Absolute Error."""
    if isinstance(pred, torch.Tensor):
        return torch.mean(torch.abs(pred - target)).item()
    return float(np.mean(np.abs(pred - target)))


def rmse(
    pred: Union[np.ndarray, torch.Tensor], target: Union[np.ndarray, torch.Tensor]
) -> float:
    """Root Mean Squared Error."""
    return float(np.sqrt(mse(pred, target)))


def mape(
    pred: Union[np.ndarray, torch.Tensor],
    target: Union[np.ndarray, torch.Tensor],
    eps: float = 1e-8,
) -> float:
    """Mean Absolute Percentage Error."""
    if isinstance(pred, torch.Tensor):
        return (
            torch.mean(torch.abs((target - pred) / (torch.abs(target) + eps))).item()
            * 100
        )
    return float(np.mean(np.abs((target - pred) / (np.abs(target) + eps)))) * 100


def smape(
    pred: Union[np.ndarray, torch.Tensor],
    target: Union[np.ndarray, torch.Tensor],
    eps: float = 1e-8,
) -> float:
    """Symmetric Mean Absolute Percentage Error."""
    if isinstance(pred, torch.Tensor):
        numerator = torch.abs(pred - target)
        denominator = (torch.abs(pred) + torch.abs(target)) / 2 + eps
        return torch.mean(numerator / denominator).item() * 100
    numerator = np.abs(pred - target)
    denominator = (np.abs(pred) + np.abs(target)) / 2 + eps
    return float(np.mean(numerator / denominator)) * 100


def compute_metrics(
    pred: Union[np.ndarray, torch.Tensor],
    target: Union[np.ndarray, torch.Tensor],
    metrics: Optional[list] = None,
) -> Dict[str, float]:
    """
    Compute multiple metrics.

    Args:
        pred: Predictions
        target: Ground truth
        metrics: List of metric names (default: ['mse', 'mae', 'mape'])

    Returns:
        Dictionary of metric name to value
    """
    if metrics is None:
        metrics = ["mse", "mae", "mape"]

    metric_fns = {
        "mse": mse,
        "mae": mae,
        "rmse": rmse,
        "mape": mape,
        "smape": smape,
    }

    results = {}
    for name in metrics:
        if name in metric_fns:
            results[name] = metric_fns[name](pred, target)

    return results
