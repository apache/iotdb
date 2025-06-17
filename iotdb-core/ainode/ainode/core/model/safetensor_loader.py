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

from pathlib import Path
from typing import Dict, Iterator, Optional, Tuple, Union

import torch

from ainode.core.constant import WEIGHT_FORMAT_PRIORITY
from ainode.core.log import Logger

logger = Logger()


def load_weights_as_state_dict(
    weights_path: Union[str, Path]
) -> Dict[str, torch.Tensor]:
    """
    Load weight file and return it in state_dict format.
    Priority: .safetensors > .pt > .pth > .bin

    Args:
        weights_path: Path to weight file or directory

    Returns:
        Dictionary of weights {key: tensor}
    """
    weights_path = Path(weights_path)

    if weights_path.is_file():
        return _load_single_weight_file(weights_path)
    elif weights_path.is_dir():
        return _load_weights_from_directory(weights_path)
    else:
        raise FileNotFoundError(f"Weight path does not exist: {weights_path}")


def load_weights_for_from_pretrained(
    model_dir: Union[str, Path], weight_filename: Optional[str] = None
) -> Dict[str, torch.Tensor]:
    """
    Load weights specifically for from_pretrained method

    Args:
        model_dir: Model directory
        weight_filename: Optional weight file name. If None, auto-detect.

    Returns:
        Dictionary of weights
    """
    model_dir = Path(model_dir)

    if weight_filename:
        weight_path = model_dir / weight_filename
        if weight_path.exists():
            return _load_single_weight_file(weight_path)
        else:
            raise FileNotFoundError(f"Specified weight file not found: {weight_path}")
    else:
        return _load_weights_from_directory(model_dir)


def iter_weights(weights_path: Union[str, Path]) -> Iterator[Tuple[str, torch.Tensor]]:
    """
    Load weights using an iterator to save memory

    Args:
        weights_path: Path to weight file or directory

    Yields:
        Tuple (key, tensor)
    """
    weights = load_weights_as_state_dict(weights_path)
    for key, tensor in weights.items():
        yield key, tensor


def _load_single_weight_file(file_path: Path) -> Dict[str, torch.Tensor]:
    """Load a single weight file"""
    suffix = file_path.suffix.lower()

    logger.debug(f"Loading weight file: {file_path}")

    if suffix == ".safetensors":
        return _load_safetensors_file(file_path)
    elif suffix in [".pt", ".pth", ".bin"]:
        return _load_pytorch_file(file_path)
    else:
        raise ValueError(f"Unsupported weight file format: {suffix}")


def _load_weights_from_directory(dir_path: Path) -> Dict[str, torch.Tensor]:
    """Load weights from a directory by checking priority patterns"""
    priority_patterns = WEIGHT_FORMAT_PRIORITY

    # Try to find a single file first
    for pattern in priority_patterns:
        file_path = dir_path / pattern
        if file_path.exists():
            logger.debug(f"Found weight file: {file_path}")
            return _load_single_weight_file(file_path)

    # Look for sharded files
    safetensor_files = list(dir_path.glob("*.safetensors"))
    pytorch_files = list(dir_path.glob("*.pt")) + list(dir_path.glob("*.pth"))

    if safetensor_files:
        logger.info(f"Found {len(safetensor_files)} SafeTensors shard files")
        return _load_sharded_safetensors(safetensor_files)
    elif pytorch_files:
        logger.info(f"Found {len(pytorch_files)} PyTorch shard files")
        return _load_sharded_pytorch(pytorch_files)

    raise FileNotFoundError(f"No weight file found in directory: {dir_path}")


def _load_safetensors_file(file_path: Path) -> Dict[str, torch.Tensor]:
    """Load SafeTensors format weight file"""
    try:
        from safetensors import safe_open
    except ImportError:
        logger.warning("safetensors not installed, falling back to PyTorch format")
        for fallback_suffix in [".pt", ".pth", ".bin"]:
            fallback_path = file_path.with_suffix(fallback_suffix)
            if fallback_path.exists():
                logger.info(f"Falling back to PyTorch format: {fallback_path}")
                return _load_pytorch_file(fallback_path)

        raise ImportError("safetensors is required: pip install safetensors")

    weights = {}
    try:
        with safe_open(file_path, framework="pt", device="cpu") as f:
            for key in f.keys():
                weights[key] = f.get_tensor(key)
    except Exception as e:
        logger.error(f"SafeTensors file is corrupted or malformed: {e}")
        raise

    logger.debug(f"SafeTensors loaded: {len(weights)} parameters")
    return weights


def _load_pytorch_file(file_path: Path) -> Dict[str, torch.Tensor]:
    """Load PyTorch-format weight file"""
    try:
        weights = torch.load(file_path, map_location="cpu")
    except Exception as e:
        logger.error(f"Failed to load PyTorch file: {e}")
        raise

    if isinstance(weights, dict):
        if "state_dict" in weights:
            weights = weights["state_dict"]
        elif "model" in weights:
            weights = weights["model"]
        elif "model_state_dict" in weights:
            weights = weights["model_state_dict"]

    if not isinstance(weights, dict):
        raise ValueError(
            f"Invalid weight file format, expected dict, got: {type(weights)}"
        )

    logger.debug(f"PyTorch weights loaded: {len(weights)} parameters")
    return weights


def _load_sharded_safetensors(file_list: list) -> Dict[str, torch.Tensor]:
    """Load sharded SafeTensors weight files"""
    try:
        from safetensors import safe_open
    except ImportError:
        logger.error("Sharded SafeTensors files require safetensors to be installed")
        raise ImportError("safetensors is required: pip install safetensors")

    weights = {}
    for file_path in sorted(file_list):
        try:
            with safe_open(file_path, framework="pt", device="cpu") as f:
                for key in f.keys():
                    if key in weights:
                        logger.warning(
                            f"Duplicate key detected in shard, overwriting: {key}"
                        )
                    weights[key] = f.get_tensor(key)
        except Exception as e:
            logger.error(f"Failed to load shard file: {file_path}, Error: {e}")
            raise

    logger.debug(
        f"Sharded SafeTensors loaded: {len(file_list)} files, {len(weights)} parameters"
    )
    return weights


def _load_sharded_pytorch(file_list: list) -> Dict[str, torch.Tensor]:
    """Load sharded PyTorch-format weight files"""
    weights = {}
    for file_path in sorted(file_list):
        try:
            shard_weights = torch.load(file_path, map_location="cpu")
            if isinstance(shard_weights, dict):
                for key, value in shard_weights.items():
                    if key in weights:
                        logger.warning(
                            f"Duplicate key detected in shard, overwriting: {key}"
                        )
                    weights[key] = value
        except Exception as e:
            logger.error(f"Failed to load shard file: {file_path}, Error: {e}")
            raise

    logger.debug(
        f"Sharded PyTorch weights loaded: {len(file_list)} files, {len(weights)} parameters"
    )
    return weights


def get_available_weight_files(model_dir: Union[str, Path]) -> Dict[str, list]:
    """
    Get available weight files from a directory

    Args:
        model_dir: Model directory

    Returns:
        {
            "single": [...],
            "sharded_safetensors": [...],
            "sharded_pytorch": [...]
        }
    """
    model_dir = Path(model_dir)
    result = {"single": [], "sharded_safetensors": [], "sharded_pytorch": []}

    for pattern in WEIGHT_FORMAT_PRIORITY:
        file_path = model_dir / pattern
        if file_path.exists():
            result["single"].append(str(file_path))

    result["sharded_safetensors"] = [
        str(p)
        for p in model_dir.glob("*.safetensors")
        if p.name not in WEIGHT_FORMAT_PRIORITY
    ]
    result["sharded_pytorch"] = [
        str(p) for p in model_dir.glob("*.pt") if p.name not in WEIGHT_FORMAT_PRIORITY
    ]
    result["sharded_pytorch"].extend([str(p) for p in model_dir.glob("*.pth")])

    return result
