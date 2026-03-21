import os
import random
import time
from datetime import datetime

import numpy as np
import pandas as pd
import torch


def seed_everything(seed: int = 42):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False


def count_parameters(model):
    total_params = sum(p.numel() for p in model.parameters())
    grad_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    no_grad_params = total_params - grad_params
    return total_params, grad_params, no_grad_params


def to_hms(seconds):
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def hms_to_seconds(hms):
    h, m, s = map(int, hms.split(":"))
    return h * 3600 + m * 60 + s


def compute_remaining_time(start_time, current_step, max_steps):
    current_time = time.time()
    elapsed_time = current_time - start_time
    remaining_steps = max_steps - current_step
    remaining_time = elapsed_time * remaining_steps / current_step
    second_per_step = elapsed_time / current_step
    return f"{to_hms(elapsed_time)}<{to_hms(remaining_time)} ({second_per_step:.2f}s/step)"


class Timer:
    def __init__(self, start_step: int = 0, max_step: int = 0):
        self.start_time = time.time()
        self.last_time = time.time()
        self.last_step = start_step
        self.max_step = max_step

    def __call__(self, step):
        current_time = time.time()
        delta_time = current_time - self.last_time
        delta_step = max(step - self.last_step, 1)
        remaining_step = self.max_step - step
        remaining_time = delta_time * remaining_step / delta_step
        elapsed_time = current_time - self.start_time
        second_per_step = delta_time / delta_step
        self.last_time = current_time
        self.last_step = step
        return f"{to_hms(elapsed_time)}<{to_hms(remaining_time)} ({second_per_step:.2f}s/step)"


class StandardScaler:
    """
    A numpy implementation of StandardScaler that mimics sklearn's StandardScaler.
    Standardizes features by removing the mean and scaling to unit variance.
    """

    def __init__(self, with_mean=True, with_std=True):
        self.with_mean = with_mean
        self.with_std = with_std
        self.mean_ = None
        self.scale_ = None
        self.var_ = None
        self.n_samples_seen_ = 0

    def fit(self, X):
        """
        Compute the mean and std to be used for later scaling.

        Parameters:
        -----------
        X : array-like, shape [n_samples, n_features]
            The data used to compute the mean and standard deviation.

        Returns:
        --------
        self : object
            Returns self.
        """
        X = np.array(X, dtype=np.float64)

        if self.with_mean:
            self.mean_ = np.mean(X, axis=0)
        else:
            self.mean_ = np.zeros(X.shape[1], dtype=np.float64)

        if self.with_std:
            self.var_ = np.var(X, axis=0)
            self.scale_ = np.sqrt(self.var_)
            # Handle zeros in scale
            self.scale_ = np.where(self.scale_ == 0, 1.0, self.scale_)
        else:
            self.var_ = np.ones(X.shape[1], dtype=np.float64)
            self.scale_ = np.ones(X.shape[1], dtype=np.float64)

        self.n_samples_seen_ = X.shape[0]

        return self

    def transform(self, X):
        """
        Perform standardization by centering and scaling.

        Parameters:
        -----------
        X : array-like, shape [n_samples, n_features]
            The data to standardize.

        Returns:
        --------
        X_scaled : array-like, shape [n_samples, n_features]
            Standardized data.
        """
        X = np.array(X, dtype=np.float64)

        if self.with_mean:
            X = X - self.mean_

        if self.with_std:
            X = X / self.scale_

        return X

    def fit_transform(self, X):
        """
        Fit to data, then transform it.

        Parameters:
        -----------
        X : array-like, shape [n_samples, n_features]
            The data to be transformed.

        Returns:
        --------
        X_scaled : array-like, shape [n_samples, n_features]
            Standardized data.
        """
        return self.fit(X).transform(X)

    def inverse_transform(self, X):
        """
        Scale back the data to the original representation.

        Parameters:
        -----------
        X : array-like, shape [n_samples, n_features]
            The data to inverse transform.

        Returns:
        --------
        X_orig : array-like, shape [n_samples, n_features]
            Data in original scale.
        """
        X = np.array(X, dtype=np.float64)

        if self.with_std:
            X = X * self.scale_

        if self.with_mean:
            X = X + self.mean_

        return X


class CSVLogger:
    """
    Simple CSV logger that stores training metrics and figure paths.
    """

    def __init__(self, log_dir: str):
        """
        Initialize CSV logger.

        Parameters:
        -----------
        log_dir : str
            Directory to save CSV log files and figures
        """

        self.log_dir = f"{log_dir}/run_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"
        os.makedirs(self.log_dir, exist_ok=True)

        # Initialize dataframe to store scalar metrics
        self.scalar_data = []
        self.scalar_file = os.path.join(self.log_dir, "scalars.csv")

        self.fig_dir = os.path.join(self.log_dir, "figures")
        os.makedirs(self.fig_dir, exist_ok=True)
        os.makedirs(f"{self.fig_dir}/TRAIN")
        os.makedirs(f"{self.fig_dir}/VAL")

    def log_scalar(self, tag: str, value: float, step: int):
        self.scalar_data.append({"timestamp": datetime.now(), "step": step, "tag": tag, "value": value})

    def save(self):
        # Save to CSV
        df = pd.DataFrame(self.scalar_data)
        df.to_csv(self.scalar_file, index=False)

    def log_figure(self, tag: str, figure, step: int):
        # Create figures subdirectory
        fig_path = os.path.join(self.fig_dir, f"{tag}_{step}.png")
        figure.savefig(fig_path)
