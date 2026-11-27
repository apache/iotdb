<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Apache IoTDB AINode

## Python dependencies

AINode requires Python 3.11 and uses Torch 2.14.1, setuptools 84.2.1, and
PyInstaller 6.22.3. Linux selects the official Torch CUDA 12.6 wheels from
`https://download.pytorch.org/whl/cu126` to retain Volta GPU support. This source
is explicit and applies only to Torch; other dependencies continue to resolve
from PyPI. macOS and Windows use the Torch 2.14.1 wheels from PyPI.

The dependency regression tests cover invalid PackedSequence and LSTM inputs,
TorchScript annotations, and model serialization. Run them and the Sundial
parameter and gradient tests from this directory in the AINode environment:

```bash
python -m unittest discover -s tests -p 'test_dependency_security.py'
python -m unittest discover -s tests -p 'test_sundial_flow_loss.py'
```

These source tests do not validate the packaged executable or GPU support.
The `Cluster IT - 1C1D1A` workflow builds and exercises the packaged AINode,
runs the Python tests, checks the Torch wheel's `sm_70` support, and executes a
CUDA tensor operation on the GPU runner.

## Timestral: weights provided by the user

AINode includes the code of the Timestral forecasting model (`timestral`), adapted from
[mistralai/timestral](https://github.com/mistralai/timestral) under the Apache License 2.0,
but not its weights. The weights are under their own licence, a Modified MIT licence. AINode
never downloads them, neither at start nor on `LOAD MODEL`, and IoTDB's source and binary
distributions do not include or mirror them. Each user obtains them independently and must
have the right to use them.

The licence of the weights grants no rights if the global consolidated monthly revenue of your
company, or of your employer, exceeded USD 20 million (or its equivalent in another currency)
in the preceding month. Read the full licence before you install the weights.

To install Timestral:

1. Obtain these three files of a Timestral checkpoint, for example
   [mistralai/Timestral-209M](https://huggingface.co/mistralai/Timestral-209M):
   - `config.json`
   - `model.safetensors`
   - `LICENSE`, the checkpoint's full licence text
2. Place them in `<ain_models_dir>/builtin/timestral/` of each AINode, by default
   `data/ainode/models/builtin/timestral/` under the AINode directory (`ain_models_dir` is
   set in `conf/iotdb-ainode.properties`). AINode creates this directory at start.
3. Run `LOAD MODEL timestral TO DEVICES ...`. It checks the three files and loads the model;
   if a file is missing or unreadable, it fails with a message that names the file and the
   directory. `SHOW MODELS` lists `timestral` as active once the files are in place.

Until then, `timestral` is listed as inactive, and `LOAD MODEL timestral` and forecasts with it
fail with these instructions.

