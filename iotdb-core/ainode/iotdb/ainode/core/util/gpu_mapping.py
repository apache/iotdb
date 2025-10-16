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

import torch


def convert_device_id_to_torch_device(device_id: str) -> torch.device:
    """
    Converts a device ID string to a torch.device object.

    Args:
        device_id (str): The device ID string. It can be "cpu" or a GPU index like "0", "1", etc.

    Returns:
        torch.device: The corresponding torch.device object.

    Raises:
        ValueError: If the device_id is not "cpu" or a valid integer string.
    """
    if device_id.lower() == "cpu":
        return torch.device("cpu")
    try:
        gpu_index = int(device_id)
        if gpu_index < 0:
            raise ValueError
        return torch.device(f"cuda:{gpu_index}")
    except ValueError:
        raise ValueError(
            f"Invalid device_id '{device_id}'. It should be 'cpu' or a non-negative integer string."
        )


def get_available_gpus() -> list[int]:
    """
    Returns a list of available GPU indices if CUDA is available, otherwise returns an empty list.
    """

    if not torch.cuda.is_available():
        return []
    return list(range(torch.cuda.device_count()))


def get_available_devices() -> list[str]:
    """
    Returns: a list of available device IDs as strings, including "cpu".
    """
    device_id_list = get_available_gpus()
    device_id_list = [str(device_id) for device_id in device_id_list]
    device_id_list.append("cpu")
    return device_id_list


def parse_devices(devices):
    """
    Parses the input string of GPU devices and returns a comma-separated string of valid GPU indices.

    Args:
        devices (str): A comma-separated string of GPU indices (e.g., "0,1,2").
    Returns:
        str: A comma-separated string of valid GPU indices corresponding to the input. All available GPUs if no input is provided.
    Exceptions:
        RuntimeError: If no GPUs are available.
        ValueError: If any of the provided GPU indices are not available.
    """
    if devices is None or devices == "":
        gpu_ids = get_available_gpus()
        if not gpu_ids:
            raise RuntimeError("No available GPU")
        return ",".join(map(str, gpu_ids))
    else:
        gpu_ids = [int(gpu) for gpu in devices.split(",")]
        available_gpus = get_available_gpus()
        for gpu_id in gpu_ids:
            if gpu_id not in available_gpus:
                raise ValueError(
                    f"GPU {gpu_id} is not available, the available choices are: {available_gpus}"
                )
        return devices
