1# Licensed to the Apache Software Foundation (ASF) under one
1# or more contributor license agreements.  See the NOTICE file
1# distributed with this work for additional information
1# regarding copyright ownership.  The ASF licenses this file
1# to you under the Apache License, Version 2.0 (the
1# "License"); you may not use this file except in compliance
1# with the License.  You may obtain a copy of the License at
1#
1#     http://www.apache.org/licenses/LICENSE-2.0
1#
1# Unless required by applicable law or agreed to in writing,
1# software distributed under the License is distributed on an
1# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1# KIND, either express or implied.  See the License for the
1# specific language governing permissions and limitations
1# under the License.
1#
1
1import torch
1
1
1def convert_device_id_to_torch_device(device_id: str) -> torch.device:
1    """
1    Converts a device ID string to a torch.device object.
1
1    Args:
1        device_id (str): The device ID string. It can be "cpu" or a GPU index like "0", "1", etc.
1
1    Returns:
1        torch.device: The corresponding torch.device object.
1
1    Raises:
1        ValueError: If the device_id is not "cpu" or a valid integer string.
1    """
1    if device_id.lower() == "cpu":
1        return torch.device("cpu")
1    try:
1        gpu_index = int(device_id)
1        if gpu_index < 0:
1            raise ValueError
1        return torch.device(f"cuda:{gpu_index}")
1    except ValueError:
1        raise ValueError(
1            f"Invalid device_id '{device_id}'. It should be 'cpu' or a non-negative integer string."
1        )
1
1
1def get_available_gpus() -> list[int]:
1    """
1    Returns a list of available GPU indices if CUDA is available, otherwise returns an empty list.
1    """
1
1    if not torch.cuda.is_available():
1        return []
1    return list(range(torch.cuda.device_count()))
1
1
1def get_available_devices() -> list[str]:
1    """
1    Returns: a list of available device IDs as strings, including "cpu".
1    """
1    device_id_list = get_available_gpus()
1    device_id_list = [str(device_id) for device_id in device_id_list]
1    device_id_list.append("cpu")
1    return device_id_list
1
1
1def parse_devices(devices):
1    """
1    Parses the input string of GPU devices and returns a comma-separated string of valid GPU indices.
1
1    Args:
1        devices (str): A comma-separated string of GPU indices (e.g., "0,1,2").
1    Returns:
1        str: A comma-separated string of valid GPU indices corresponding to the input. All available GPUs if no input is provided.
1    Exceptions:
1        RuntimeError: If no GPUs are available.
1        ValueError: If any of the provided GPU indices are not available.
1    """
1    if devices is None or devices == "":
1        gpu_ids = get_available_gpus()
1        if not gpu_ids:
1            raise RuntimeError("No available GPU")
1        return ",".join(map(str, gpu_ids))
1    else:
1        gpu_ids = [int(gpu) for gpu in devices.split(",")]
1        available_gpus = get_available_gpus()
1        for gpu_id in gpu_ids:
1            if gpu_id not in available_gpus:
1                raise ValueError(
1                    f"GPU {gpu_id} is not available, the available choices are: {available_gpus}"
1                )
1        return devices
1