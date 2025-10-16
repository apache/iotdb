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
1import os
1from enum import Enum
1from typing import List
1
1from huggingface_hub import snapshot_download
1from requests import Session
1from requests.adapters import HTTPAdapter
1
1from iotdb.ainode.core.constant import (
1    DEFAULT_CHUNK_SIZE,
1    DEFAULT_RECONNECT_TIMEOUT,
1    DEFAULT_RECONNECT_TIMES,
1)
1from iotdb.ainode.core.exception import UnsupportedError
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.model.model_enums import ModelFileType
1from iotdb.ainode.core.model.model_info import get_model_file_type
1
1HTTP_PREFIX = "http://"
1HTTPS_PREFIX = "https://"
1
1logger = Logger()
1
1
1class UriType(Enum):
1    REPO = "repo"
1    FILE = "file"
1    HTTP = "http"
1    HTTPS = "https"
1
1    @classmethod
1    def values(cls) -> List[str]:
1        return [item.value for item in cls]
1
1    @staticmethod
1    def parse_uri_type(uri: str):
1        """
1        Parse the URI type from the given string.
1        """
1        if uri.startswith("repo://"):
1            return UriType.REPO
1        elif uri.startswith("file://"):
1            return UriType.FILE
1        elif uri.startswith("http://"):
1            return UriType.HTTP
1        elif uri.startswith("https://"):
1            return UriType.HTTPS
1        else:
1            raise ValueError(f"Invalid URI type for {uri}")
1
1
1def get_model_register_strategy(uri: str):
1    """
1    Determine the loading strategy for a model based on its URI/path.
1
1    Args:
1        uri (str): The URI of the model to be registered.
1
1    Returns:
1        uri_type (UriType): The type of the URI, which can be one of: REPO, FILE, HTTP, or HTTPS.
1        parsed_uri (str): Parsed uri to get related file
1        model_file_type (ModelFileType): The type of the model file, which can be one of: SAFETENSORS, PYTORCH, or UNKNOWN.
1    """
1
1    uri_type = UriType.parse_uri_type(uri)
1    if uri_type in (UriType.HTTP, UriType.HTTPS):
1        # TODO: support HTTP(S) URI
1        raise UnsupportedError("CREATE MODEL FROM HTTP(S) URI")
1    else:
1        parsed_uri = uri[7:]
1        if uri_type == UriType.FILE:
1            # handle ~ in URI
1            parsed_uri = os.path.expanduser(parsed_uri)
1            model_file_type = get_model_file_type(uri)
1        elif uri_type == UriType.REPO:
1            # Currently, UriType.REPO only corresponds to huggingface repository with SAFETENSORS format
1            model_file_type = ModelFileType.SAFETENSORS
1        else:
1            raise ValueError(f"Invalid URI type for {uri}")
1    return uri_type, parsed_uri, model_file_type
1
1
1def download_snapshot_from_hf(repo_id: str, local_dir: str):
1    """
1    Download everything from a HuggingFace repository.
1
1    Args:
1        repo_id (str): The HuggingFace repository ID.
1        local_dir (str): The local directory to save the downloaded files.
1    """
1    try:
1        snapshot_download(
1            repo_id=repo_id,
1            local_dir=local_dir,
1        )
1    except Exception as e:
1        logger.error(f"Failed to download HuggingFace model {repo_id}: {e}")
1        raise e
1
1
1def download_file(url: str, storage_path: str) -> None:
1    """
1    Args:
1        url: url of file to download
1        storage_path: path to save the file
1    Returns:
1        None
1    """
1    logger.info(f"Start Downloading file from {url} to {storage_path}")
1    session = Session()
1    adapter = HTTPAdapter(max_retries=DEFAULT_RECONNECT_TIMES)
1    session.mount(HTTP_PREFIX, adapter)
1    session.mount(HTTPS_PREFIX, adapter)
1    response = session.get(url, timeout=DEFAULT_RECONNECT_TIMEOUT, stream=True)
1    response.raise_for_status()
1    with open(storage_path, "wb") as file:
1        for chunk in response.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
1            if chunk:
1                file.write(chunk)
1    logger.info(f"Download file from {url} to {storage_path} success")
1