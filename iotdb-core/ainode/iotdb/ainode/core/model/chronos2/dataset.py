# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import math
from enum import Enum
from typing import TYPE_CHECKING, Iterator, Mapping, Sequence, TypeAlias, cast

import numpy as np
import torch
from sklearn.preprocessing import OrdinalEncoder, TargetEncoder
from torch.utils.data import IterableDataset

if TYPE_CHECKING:
    import datasets
    import fev


TensorOrArray: TypeAlias = torch.Tensor | np.ndarray


def left_pad_and_cat_2D(tensors: list[torch.Tensor]) -> torch.Tensor:
    """
    Left pads tensors in the list to the length of the longest tensor along the second axis, then concats
    these equal length tensors along the first axis.
    """
    max_len = max(tensor.shape[-1] for tensor in tensors)
    padded = []
    for tensor in tensors:
        n_variates, length = tensor.shape
        if length < max_len:
            padding = torch.full(
                (n_variates, max_len - length),
                fill_value=torch.nan,
                device=tensor.device,
            )
            tensor = torch.cat([padding, tensor], dim=-1)
        padded.append(tensor)

    return torch.cat(padded, dim=0)


def validate_and_prepare_single_dict_task(
    task: Mapping[str, TensorOrArray | Mapping[str, TensorOrArray]],
    idx: int,
    prediction_length: int,
) -> tuple[torch.Tensor, torch.Tensor, int, int, int]:
    """Validates and prepares a single dictionary task for Chronos2Model.

    Parameters
    ----------
    task
        A dictionary representing a time series that contains:
        - `target` (required): a 1-d or 2-d `torch.Tensor` or `np.ndarray` of shape (history_length,) or (n_variates, history_length).
        Forecasts will be generated for items in `target`.
        - `past_covariates` (optional): a dict of past-only covariates or past values of known future covariates. The keys of the dict
        must be names of the covariates and values must be 1-d `torch.Tensor` or `np.ndarray` with length equal to the `history_length`
        of `target`.
        - `future_covariates` (optional): a dict of future values of known future covariates. The keys of the dict must be names of the
        covariates and values must be 1-d `torch.Tensor` or `np.ndarray` with length equal to the `prediction_length`. All keys in
        `future_covariates` must be a subset of the keys in `past_covariates`.
    idx
        Index of this task in the list of tasks, used for error messages
    prediction_length
        Number of future time steps to predict, used to validate future covariates

    Returns
    ------
    A tuple containing:
    - task_context_tensor: Concatenated tensor of target and past covariates of shape (group_size, history_length),
        the first `task_n_targets` items along the first axis contain the target variables and the remaining items contain past-only covariates
        and past values of known future covariates.
    - task_future_covariates_tensor: Tensor of future covariates of shape (group_size, prediction_length). The last `task_n_future_covariates`
        items along the first axis contain future covariates. All the remaining elements corresponding to target and past-only covariates are NaNs.
    - task_n_targets: Number of target variables
    - task_n_covariates: Total number of covariates (sum of past-only and known future covariates)
    - task_n_future_covariates: Number of known future covariates
    """

    allowed_keys = {"target", "past_covariates", "future_covariates"}

    # validate keys
    keys = set(task.keys())
    if not keys.issubset(allowed_keys):
        raise ValueError(
            f"Found invalid keys in element at index {idx}. Allowed keys are {allowed_keys}, but found {keys}"
        )
    if "target" not in keys:
        raise ValueError(
            f"Element at index {idx} does not contain the required key 'target'"
        )

    # validate target
    task_target = task["target"]
    if isinstance(task_target, np.ndarray):
        task_target = torch.from_numpy(task_target)
    assert isinstance(task_target, torch.Tensor)
    if task_target.ndim > 2:
        raise ValueError(
            "When the input is a list of dicts, the `target` should either be 1-d with shape (history_length,) "
            f" or 2-d with shape (n_variates, history_length). Found element at index {idx} with shape {tuple(task_target.shape)}."
        )
    history_length = task_target.shape[-1]
    task_target = task_target.view(-1, history_length)

    # validate past_covariates
    cat_encoders: dict = {}
    task_past_covariates = task.get("past_covariates", {})
    if not isinstance(task_past_covariates, dict):
        raise ValueError(
            f"Found invalid type for `past_covariates` in element at index {idx}. "
            f'Expected dict with {{"feat_1": tensor_1, "feat_2": tensor_2, ...}}, but found {type(task_past_covariates)}'
        )

    # gather keys and ensure known-future keys come last to match downstream assumptions
    task_covariates_keys = sorted(task_past_covariates.keys())

    task_future_covariates = task.get("future_covariates", {})
    if not isinstance(task_future_covariates, dict):
        raise ValueError(
            f"Found invalid type for `future_covariates` in element at index {idx}. "
            f'Expected dict with {{"feat_1": tensor_1, "feat_2": tensor_2, ...}}, but found {type(task_future_covariates)}'
        )
    task_future_covariates_keys = sorted(task_future_covariates.keys())
    if not set(task_future_covariates_keys).issubset(task_covariates_keys):
        raise ValueError(
            f"Expected keys in `future_covariates` to be a subset of `past_covariates` {task_covariates_keys}, "
            f"but found {task_future_covariates_keys} in element at index {idx}"
        )

    # create ordered keys: past-only first, then known-future (so known-future are the last rows)
    task_past_only_keys = [
        k for k in task_covariates_keys if k not in task_future_covariates_keys
    ]  # past_only_keys
    task_ordered_covariate_keys = task_past_only_keys + task_future_covariates_keys

    task_past_covariates_list: list[torch.Tensor] = []
    for key in task_ordered_covariate_keys:
        tensor = task_past_covariates[key]
        if isinstance(tensor, np.ndarray):
            # apply encoding to categorical variates
            if not np.issubdtype(tensor.dtype, np.number):
                # target encoding, if the target is 1-d
                if task_target.shape[0] == 1:
                    cat_encoder = TargetEncoder(target_type="continuous", smooth=1.0)
                    X = tensor.astype(str).reshape(-1, 1)
                    y = task_target.view(-1).numpy()
                    mask = np.isfinite(y)
                    X = X[mask]
                    y = y[mask]
                    cat_encoder.fit(X, y)
                # ordinal encoding, if the target is > 1-d
                else:
                    cat_encoder = OrdinalEncoder(
                        handle_unknown="use_encoded_value", unknown_value=np.nan
                    )
                    cat_encoder.fit(tensor.astype(str).reshape(-1, 1))
                tensor = cat_encoder.transform(
                    tensor.astype(str).reshape(-1, 1)
                ).reshape(tensor.shape)
                cat_encoders[key] = cat_encoder
            tensor = torch.from_numpy(tensor)
        assert isinstance(tensor, torch.Tensor)
        if tensor.ndim != 1 or len(tensor) != history_length:
            raise ValueError(
                f"Individual `past_covariates` must be 1-d with length equal to the length of `target` (= {history_length}), "
                f"found: {key} with shape {tuple(tensor.shape)} in element at index {idx}"
            )
        task_past_covariates_list.append(tensor)
    task_past_covariates_tensor = (
        torch.stack(task_past_covariates_list, dim=0)
        if task_past_covariates_list
        else torch.zeros((0, history_length), device=task_target.device)
    )

    # validate future_covariates (build rows in the same task_ordered_covariate_keys order)
    task_future_covariates_list: list[torch.Tensor] = []
    for key in task_ordered_covariate_keys:
        # future values of past-only covariates are filled with NaNs
        tensor = task_future_covariates.get(
            key, torch.full((prediction_length,), fill_value=torch.nan)
        )
        if isinstance(tensor, np.ndarray):
            # apply encoding to categorical variates
            if not np.issubdtype(tensor.dtype, np.number):
                cat_encoder = cat_encoders[key]
                tensor = cat_encoder.transform(
                    tensor.astype(str).reshape(-1, 1)
                ).reshape(tensor.shape)
            tensor = torch.from_numpy(tensor)
        assert isinstance(tensor, torch.Tensor)
        if tensor.ndim != 1 or len(tensor) != prediction_length:
            raise ValueError(
                f"Individual `future_covariates` must be 1-d with length equal to the {prediction_length=}, "
                f"found: {key} with shape {tuple(tensor.shape)} in element at index {idx}"
            )
        task_future_covariates_list.append(tensor)
    task_future_covariates_tensor = (
        torch.stack(task_future_covariates_list, dim=0)
        if task_future_covariates_list
        else torch.zeros((0, prediction_length), device=task_target.device)
    )
    # future values of target series are filled with NaNs
    task_future_covariates_target_padding = torch.full(
        (task_target.shape[0], prediction_length),
        fill_value=torch.nan,
        device=task_target.device,
    )

    task_context_tensor = torch.cat(
        [task_target, task_past_covariates_tensor], dim=0
    ).to(dtype=torch.float32)
    task_future_covariates_tensor = torch.cat(
        [task_future_covariates_target_padding, task_future_covariates_tensor], dim=0
    ).to(dtype=torch.float32)
    task_n_targets = task_target.shape[0]
    task_n_covariates = task_past_covariates_tensor.shape[0]
    # number of known-future covariates
    task_n_future_covariates = len(task_future_covariates_keys)

    return (
        task_context_tensor,
        task_future_covariates_tensor,
        task_n_targets,
        task_n_covariates,
        task_n_future_covariates,
    )


def convert_list_of_tensors_input_to_list_of_dicts_input(
    list_of_tensors: Sequence[TensorOrArray],
) -> list[dict[str, torch.Tensor]]:
    """Convert a list of tensors input format to a list of dictionaries input format.


    Parameters
    ----------
    list_of_tensors
        A sequence of tensors or numpy arrays, where each element represents a time series.
        Each element should be either 1-d with shape (history_length,) or 2-d with shape
        (n_variates, history_length).

    Returns
    -------
    A list of dictionaries, where each dictionary represents a time series and contains:
    - `target`: a 1-d or 2-d torch.Tensor of shape (history_length,) or (n_variates, history_length).
    """

    output: list[dict[str, torch.Tensor]] = []
    for idx, tensor in enumerate(list_of_tensors):
        if isinstance(tensor, np.ndarray):
            tensor = torch.from_numpy(tensor)
        if tensor.ndim > 2:
            raise ValueError(
                "When the input is a list of torch tensors or numpy arrays, the elements should either be 1-d with shape (history_length,) "
                f" or 2-d with shape (n_variates, history_length). Found element at index {idx} with shape {tuple(tensor.shape)}."
            )
        length = tensor.shape[-1]
        tensor = tensor.view(-1, length)

        output.append({"target": tensor})

    return output


def convert_tensor_input_to_list_of_dicts_input(
    tensor: TensorOrArray,
) -> list[dict[str, torch.Tensor]]:
    """
    Convert a tensor input format to a list of dictionaries input format.

    Parameters
    ----------
    tensor
        A tensor or numpy array representing multiple time series.
        Should be 3-d with shape (n_series, n_variates, history_length).

    Returns
    -------
    A list of dictionaries, where each dictionary represents a time series and contains:
    - `target`: a 2-d torch.Tensor of shape (n_variates, history_length).
    """

    if isinstance(tensor, np.ndarray):
        tensor = torch.from_numpy(tensor)
    if tensor.ndim != 3:
        raise ValueError(
            "When the input is a torch tensor or numpy array, it should be 3-d with shape (n_series, n_variates, history_length). "
            f" Found shape: {tuple(tensor.shape)}."
        )

    output: list[dict[str, torch.Tensor]] = []
    n_series = len(tensor)
    for i in range(n_series):
        output.append({"target": tensor[i]})

    return output


def _cast_fev_features(
    past_data: "datasets.Dataset",
    future_data: "datasets.Dataset",
    target_columns: list[str],
    past_dynamic_columns: list[str],
    known_dynamic_columns: list[str],
) -> tuple["datasets.Dataset", "datasets.Dataset"]:
    import datasets

    dynamic_columns = [*past_dynamic_columns, *known_dynamic_columns]
    cat_cols = []
    for col in dynamic_columns:
        item = past_data[0][col]
        if not np.issubdtype(item.dtype, np.number):
            cat_cols.append(col)

    numeric_cols = target_columns + list(set(dynamic_columns) - set(cat_cols))
    past_feature_updates = {
        col: datasets.Sequence(datasets.Value("float64")) for col in numeric_cols
    } | {col: datasets.Sequence(datasets.Value("string")) for col in cat_cols}
    past_data_features = past_data.features
    past_data_features.update(past_feature_updates)
    past_data = past_data.cast(past_data_features)

    future_cat_cols = [k for k in cat_cols if k in known_dynamic_columns]
    future_numeric_cols = list(set(known_dynamic_columns) - set(future_cat_cols))
    future_feature_updates = {
        col: datasets.Sequence(datasets.Value("float64")) for col in future_numeric_cols
    } | {col: datasets.Sequence(datasets.Value("string")) for col in future_cat_cols}
    future_data_features = future_data.features
    future_data_features.update(future_feature_updates)
    future_data = future_data.cast(future_data_features)

    return past_data, future_data


def convert_fev_window_to_list_of_dicts_input(
    window: "fev.EvaluationWindow", as_univariate: bool
) -> tuple[
    list[dict[str, np.ndarray | dict[str, np.ndarray]]], list[str], list[str], list[str]
]:
    import fev

    if as_univariate:
        past_data, future_data = fev.convert_input_data(
            window, adapter="datasets", as_univariate=True
        )
        target_columns = ["target"]
        past_dynamic_columns = []
        known_dynamic_columns = []
    else:
        past_data, future_data = window.get_input_data()
        target_columns = window.target_columns
        past_dynamic_columns = window.past_dynamic_columns
        known_dynamic_columns = window.known_dynamic_columns

    past_data, future_data = _cast_fev_features(
        past_data=past_data,
        future_data=future_data,
        target_columns=target_columns,
        past_dynamic_columns=past_dynamic_columns,
        known_dynamic_columns=known_dynamic_columns,
    )

    num_series: int = len(past_data)
    num_past_covariates: int = len(past_dynamic_columns)
    num_future_covariates: int = len(known_dynamic_columns)

    # We use numpy format because torch does not support str covariates
    target_data = past_data.select_columns(target_columns).with_format("numpy")
    # past of past-only and known-future covariates
    dynamic_columns = [*past_dynamic_columns, *known_dynamic_columns]
    past_covariate_data = past_data.select_columns(dynamic_columns).with_format("numpy")
    future_known_data = future_data.select_columns(known_dynamic_columns).with_format(
        "numpy"
    )

    if num_past_covariates + num_future_covariates > 0:
        assert len(past_covariate_data) == num_series
    if num_future_covariates > 0:
        assert len(future_known_data) == num_series

    inputs: list[dict[str, np.ndarray | dict[str, np.ndarray]]] = []
    for idx, target_row in enumerate(target_data):
        target_row = cast(dict, target_row)
        # this assumes that the targets have the same length for multivariate tasks
        target_tensor_i = np.stack([target_row[col] for col in target_columns])
        entry: dict[str, np.ndarray | dict[str, np.ndarray]] = {
            "target": target_tensor_i
        }

        if len(dynamic_columns) > 0:
            past_covariate_row = past_covariate_data[idx]
            entry["past_covariates"] = {
                col: past_covariate_row[col] for col in dynamic_columns
            }

        if len(known_dynamic_columns) > 0:
            future_known_row = future_known_data[idx]
            entry["future_covariates"] = {
                col: future_known_row[col] for col in known_dynamic_columns
            }

        inputs.append(entry)

    return inputs, target_columns, past_dynamic_columns, known_dynamic_columns


class DatasetMode(str, Enum):
    TRAIN = "train"
    VALIDATION = "validation"
    TEST = "test"


class Chronos2Dataset(IterableDataset):
    """
    A dataset wrapper for Chronos-2 models.

    Arguments
    ----------
    inputs
        Time series data. Must be a list of dictionaries where each dictionary may have the following keys.
        - `target` (required): a 1-d or 2-d `torch.Tensor` or `np.ndarray` of shape (history_length,) or (n_variates, history_length).
        Forecasts will be generated for items in `target`.
        - `past_covariates` (optional): a dict of past-only covariates or past values of known future covariates. The keys of the dict
        must be names of the covariates and values must be 1-d `torch.Tensor` or `np.ndarray` with length equal to the `history_length`
        of `target`.
        - `future_covariates` (optional): a dict of future values of known future covariates. The keys of the dict must be names of the
        covariates and values must be 1-d `torch.Tensor` or `np.ndarray` with length equal to the `prediction_length`. All keys in
        `future_covariates` must be a subset of the keys in `past_covariates`.
        Note: when the mode is set to TRAIN, the values inside `future_covariates` are not technically used for training the model;
        however, this key is used to infer which covariates are known into the future. Therefore, if your task contains known future covariates,
        make sure that this key exists in `inputs`. The values of individual future covariates may be set to `None` or an empty array.
    context_length
        The maximum context length used for training or inference
    prediction_length
        The prediction horizon
    batch_size
        The batch size for training the model. Note that the batch size here means the number of time series, including target(s) and
        covariates, that are input into the model. If your data has multiple target and/or covariates, the effective number of time series
        tasks in a batch will be lower than this value.
    output_patch_size
        The output patch size of the model. This is used to compute the number of patches needed to cover `prediction_length`
    min_past
        The minimum number of time steps the context must have during training. All time series shorter than `min_past + prediction_length`
        are filtered out, by default 1
    mode
        `DatasetMode` governing whether to generate training, validation or test samples, by default "train"
    """

    def __init__(
        self,
        inputs: Sequence[
            Mapping[str, TensorOrArray | Mapping[str, TensorOrArray | None]]
        ],
        context_length: int,
        prediction_length: int,
        batch_size: int,
        output_patch_size: int,
        min_past: int = 1,
        mode: str | DatasetMode = DatasetMode.TRAIN,
    ) -> None:
        super().__init__()
        assert mode in {
            DatasetMode.TRAIN,
            DatasetMode.VALIDATION,
            DatasetMode.TEST,
        }, f"Invalid mode: {mode}"

        self.tasks = Chronos2Dataset._prepare_tasks(
            inputs, prediction_length, min_past, mode
        )
        self.context_length = context_length
        self.prediction_length = prediction_length
        self.batch_size = batch_size
        self.num_output_patches = math.ceil(prediction_length / output_patch_size)
        self.min_past = min_past
        self.mode = mode

    @staticmethod
    def _prepare_tasks(
        inputs: Sequence[
            Mapping[str, TensorOrArray | Mapping[str, TensorOrArray | None]]
        ],
        prediction_length: int,
        min_past: int,
        mode: str | DatasetMode,
    ):
        tasks = []
        for idx, raw_task in enumerate(inputs):
            if mode != DatasetMode.TEST:
                raw_future_covariates = raw_task.get("future_covariates", {})
                raw_future_covariates = cast(
                    dict[str, TensorOrArray | None], raw_future_covariates
                )
                if raw_future_covariates:
                    fixed_future_covariates = {}
                    for key, value in raw_future_covariates.items():
                        fixed_future_covariates[key] = (
                            np.full(prediction_length, np.nan)
                            if value is None or len(value) == 0
                            else value
                        )
                    raw_task = {
                        **raw_task,
                        "future_covariates": fixed_future_covariates,
                    }

            raw_task = cast(
                dict[str, TensorOrArray | Mapping[str, TensorOrArray]], raw_task
            )
            # convert to a format compatible with model's forward
            task = validate_and_prepare_single_dict_task(
                raw_task, idx, prediction_length
            )

            if (
                mode != DatasetMode.TEST
                and task[0].shape[-1] < min_past + prediction_length
            ):
                # filter tasks based on min_past + prediction_length
                continue
            tasks.append(task)

        if len(tasks) == 0:
            raise ValueError(
                "The dataset is empty after filtering based on the length of the time series (length >= min_past + prediction_length). "
                "Please provide longer time series or reduce `min_past` or `prediction_length`. "
            )
        return tasks

    def _construct_slice(
        self, task_idx: int
    ) -> tuple[torch.Tensor, torch.Tensor | None, torch.Tensor, int]:
        (
            task_past_tensor,  # shape:  (task_n_targets + task_n_covariates, history_length)
            task_future_tensor,
            task_n_targets,
            task_n_covariates,
            task_n_future_covariates,
        ) = self.tasks[task_idx]
        task_past_tensor, task_future_tensor = (
            task_past_tensor.clone(),
            task_future_tensor.clone(),
        )
        task_n_past_only_covariates = task_n_covariates - task_n_future_covariates

        full_length = task_past_tensor.shape[-1]

        if self.mode == DatasetMode.TRAIN:
            # slice a random subsequence from the full series
            slice_idx = np.random.randint(
                self.min_past, full_length - self.prediction_length + 1
            )
        elif self.mode == DatasetMode.VALIDATION:
            # slice the last window for validation
            slice_idx = full_length - self.prediction_length
        else:
            # slice the full series for prediction
            slice_idx = full_length

        if slice_idx >= self.context_length:
            # slice series, if it is longer than context_length
            task_context = task_past_tensor[
                :, slice_idx - self.context_length : slice_idx
            ]
        else:
            task_context = task_past_tensor[:, :slice_idx]

        # In the TEST mode, we have no target available and the task_future_covariates can be directly used
        # In the TRAIN and VALIDATION modes, the target and task_future_covariates need to be constructed from
        # the task_context_tensor by slicing the appropriate indices which we do below
        if self.mode in [DatasetMode.TRAIN, DatasetMode.VALIDATION]:
            # the first task_n_targets elements in task_context_tensor are the targets
            task_future_target = task_past_tensor[
                :, slice_idx : slice_idx + self.prediction_length
            ].clone()
            # mask out all rows corresponding to covariates
            task_future_target[task_n_targets:] = torch.nan

            if task_n_future_covariates > 0:
                # the last task_n_future_covariates elements in task_context_tensor are the known covariates
                task_future_covariates = task_past_tensor[
                    -task_n_future_covariates:,
                    slice_idx : slice_idx + self.prediction_length,
                ]
            else:
                # zero-length tensor for easy concatenation later
                task_future_covariates = torch.zeros((0, self.prediction_length))

            # the leading task_n_targets + task_n_past_only_covariates elements are masked because the target(s)
            # and past-only covariates are not known into the future
            task_future_covariates_padding = torch.full(
                (task_n_targets + task_n_past_only_covariates, self.prediction_length),
                fill_value=torch.nan,
            )
            task_future_covariates = torch.cat(
                [task_future_covariates_padding, task_future_covariates], dim=0
            )
        else:
            task_future_target = None
            task_future_covariates = task_future_tensor

        # task_context: (task_n_targets + task_n_covariates, min(context_length, history_length))
        # task_future_target: (task_n_targets + task_n_covariates, prediction_length), the future values of known future covariates
        # are ignored during loss computation
        # task_future_covariates: (task_n_targets + task_n_past_only_covariates + task_n_future_covariates, prediction_length),
        # the entries corresponding to targets and past-only covariates are NaNs

        return task_context, task_future_target, task_future_covariates, task_n_targets

    def _build_batch(
        self, task_indices: list[int]
    ) -> dict[str, torch.Tensor | int | list[tuple[int, int]] | None]:
        """Build a batch from given task indices."""
        batch_context_tensor_list = []
        batch_future_target_tensor_list = []
        batch_future_covariates_tensor_list = []
        batch_group_ids_list = []
        target_idx_ranges: list[tuple[int, int]] = []

        target_start_idx = 0
        for group_id, task_idx in enumerate(task_indices):
            task_context, task_future_target, task_future_covariates, task_n_targets = (
                self._construct_slice(task_idx)
            )

            group_size = task_context.shape[0]
            task_group_ids = torch.full((group_size,), fill_value=group_id)
            batch_context_tensor_list.append(task_context)
            batch_future_target_tensor_list.append(task_future_target)
            batch_future_covariates_tensor_list.append(task_future_covariates)
            batch_group_ids_list.append(task_group_ids)
            target_idx_ranges.append(
                (target_start_idx, target_start_idx + task_n_targets)
            )
            target_start_idx += group_size

        return {
            "context": left_pad_and_cat_2D(batch_context_tensor_list),
            "future_target": (
                None
                if self.mode == DatasetMode.TEST
                else torch.cat(
                    cast(list[torch.Tensor], batch_future_target_tensor_list), dim=0
                )
            ),
            "future_covariates": torch.cat(batch_future_covariates_tensor_list, dim=0),
            "group_ids": torch.cat(batch_group_ids_list, dim=0),
            "num_output_patches": self.num_output_patches,
            "target_idx_ranges": target_idx_ranges,
        }

    def _generate_train_batches(self):
        while True:
            current_batch_size = 0
            task_indices = []

            while current_batch_size < self.batch_size:
                task_idx = np.random.randint(len(self.tasks))
                task_indices.append(task_idx)
                current_batch_size += self.tasks[task_idx][0].shape[0]

            yield self._build_batch(task_indices)

    def _generate_sequential_batches(self):
        task_idx = 0
        while task_idx < len(self.tasks):
            current_batch_size = 0
            task_indices = []

            while task_idx < len(self.tasks) and current_batch_size < self.batch_size:
                task_indices.append(task_idx)
                current_batch_size += self.tasks[task_idx][0].shape[0]
                task_idx += 1

            yield self._build_batch(task_indices)

    def __iter__(self) -> Iterator:
        """
        Generate batches of data for the Chronos-2 model. In training mode, this iterator is infinite.

        Yields
        ------
        dict
            A dictionary containing:
            - context: torch.Tensor of shape (batch_size, context_length) containing input sequences
            - future_target: torch.Tensor of shape (batch_size, prediction_length) containing future target sequences, None in TEST mode
            - future_covariates: torch.Tensor of shape (batch_size, prediction_length) containing known future covariates
            - group_ids: torch.Tensor of shape (batch_size,) containing the group ID for each sequence
            - num_output_patches: int indicating number of patches the model should output to cover prediction_length
            - target_idx_ranges: (only in TEST mode) list of tuples indicating the start & end indices of targets in context
        """
        if self.mode == DatasetMode.TRAIN:
            for batch in self._generate_train_batches():
                batch.pop("target_idx_ranges")
                yield batch
        elif self.mode == DatasetMode.VALIDATION:
            for batch in self._generate_sequential_batches():
                batch.pop("target_idx_ranges")
                yield batch
        else:
            yield from self._generate_sequential_batches()

    @classmethod
    def convert_inputs(
        cls,
        inputs: (
            TensorOrArray
            | Sequence[TensorOrArray]
            | Sequence[Mapping[str, TensorOrArray | Mapping[str, TensorOrArray | None]]]
        ),
        context_length: int,
        prediction_length: int,
        batch_size: int,
        output_patch_size: int,
        min_past: int = 1,
        mode: str | DatasetMode = DatasetMode.TRAIN,
    ) -> "Chronos2Dataset":
        """Convert from different input formats to a Chronos2Dataset."""
        if isinstance(inputs, (torch.Tensor, np.ndarray)):
            inputs = convert_tensor_input_to_list_of_dicts_input(inputs)
        elif isinstance(inputs, list) and all(
            [isinstance(x, (torch.Tensor, np.ndarray)) for x in inputs]
        ):
            inputs = cast(list[TensorOrArray], inputs)
            inputs = convert_list_of_tensors_input_to_list_of_dicts_input(inputs)
        elif isinstance(inputs, list) and all([isinstance(x, dict) for x in inputs]):
            pass
        else:
            raise ValueError("Unexpected inputs format")

        inputs = cast(list[dict[str, TensorOrArray | dict[str, TensorOrArray]]], inputs)

        return cls(
            inputs,
            context_length=context_length,
            prediction_length=prediction_length,
            batch_size=batch_size,
            output_patch_size=output_patch_size,
            min_past=min_past,
            mode=mode,
        )
