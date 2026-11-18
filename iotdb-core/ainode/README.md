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
