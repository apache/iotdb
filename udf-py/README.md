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

# Apache IoTDB Python UDF

## Releasing

To do a release just ensure that you have the right set of dev dependencies.
Then run linting and auto-formatting.
Then, ensure that all tests work (via `pytest .`).
Then you are good to go to do a release!

### Preparing your environment

First, install all necessary dev dependencies via `pip install -r requirements_dev.txt`.

### Doing the Release

There is a convenient script `release.sh` to do all steps for a release.
Namely, these are

* Remove all transient directories from last release (if exists)
* Run Autoformatting (`black .`) 
* Run Linting (`flake8 .`)
* Run Tests via pytest
* Build
* Release to pypi
