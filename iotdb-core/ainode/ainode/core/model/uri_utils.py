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
import os
from enum import Enum
from typing import List

from huggingface_hub import snapshot_download
from requests import Session
from requests.adapters import HTTPAdapter

from ainode.core.constant import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_RECONNECT_TIMEOUT,
    DEFAULT_RECONNECT_TIMES,
)
from ainode.core.exception import UnsupportedError
from ainode.core.log import Logger
from ainode.core.model.model_enums import ModelFileType
from ainode.core.model.model_info import get_model_file_type

HTTP_PREFIX = "http://"
HTTPS_PREFIX = "https://"

logger = Logger()


class UriType(Enum):
    REPO = "repo"
    FILE = "file"
    HTTP = "http"
    HTTPS = "https"

    @classmethod
    def values(cls) -> List[str]:
        return [item.value for item in cls]

    @staticmethod
    def parse_uri_type(uri: str):
        """
        Parse the URI type from the given string.
        """
        if uri.startswith("repo://"):
            return UriType.REPO
        elif uri.startswith("file://"):
            return UriType.FILE
        elif uri.startswith("http://"):
            return UriType.HTTP
        elif uri.startswith("https://"):
            return UriType.HTTPS
        else:
            raise ValueError(f"Invalid URI type for {uri}")


def get_model_register_strategy(uri: str):
    """
    Determine the loading strategy for a model based on its URI/path.

    Args:
        uri (str): The URI of the model to be registered.

    Returns:
        uri_type (UriType): The type of the URI, which can be one of: REPO, FILE, HTTP, or HTTPS.
        parsed_uri (str): Parsed uri to get related file
        model_file_type (ModelFileType): The type of the model file, which can be one of: SAFETENSORS, PYTORCH, or UNKNOWN.
    """

    uri_type = UriType.parse_uri_type(uri)
    if uri_type in (UriType.HTTP, UriType.HTTPS):
        # TODO: support HTTP(S) URI
        raise UnsupportedError("CREATE MODEL FROM HTTP(S) URI")
    else:
        parsed_uri = uri[7:]
        if uri_type == UriType.FILE:
            # handle ~ in URI
            parsed_uri = os.path.expanduser(parsed_uri)
            model_file_type = get_model_file_type(uri)
        elif uri_type == UriType.REPO:
            # Currently, UriType.REPO only corresponds to huggingface repository with SAFETENSORS format
            model_file_type = ModelFileType.SAFETENSORS
        else:
            raise ValueError(f"Invalid URI type for {uri}")
    return uri_type, parsed_uri, model_file_type


def download_snapshot_from_hf(repo_id: str, local_dir: str):
    """
    Download everything from a HuggingFace repository.

    Args:
        repo_id (str): The HuggingFace repository ID.
        local_dir (str): The local directory to save the downloaded files.
    """
    try:
        snapshot_download(
            repo_id=repo_id,
            local_dir=local_dir,
        )
    except Exception as e:
        logger.error(f"Failed to download HuggingFace model {repo_id}: {e}")
        raise e


def download_file(url: str, storage_path: str) -> None:
    """
    Args:
        url: url of file to download
        storage_path: path to save the file
    Returns:
        None
    """
    logger.info(f"Start Downloading file from {url} to {storage_path}")
    session = Session()
    adapter = HTTPAdapter(max_retries=DEFAULT_RECONNECT_TIMES)
    session.mount(HTTP_PREFIX, adapter)
    session.mount(HTTPS_PREFIX, adapter)
    response = session.get(url, timeout=DEFAULT_RECONNECT_TIMEOUT, stream=True)
    response.raise_for_status()
    with open(storage_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
            if chunk:
                file.write(chunk)
    logger.info(f"Download file from {url} to {storage_path} success")
