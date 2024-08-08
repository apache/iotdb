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

## Motivation
The current E2E framework writes all process logs into a single test log. This approach makes it difficult to find the root cause when there are failed tests with unstable recurrence. So we need a tool that can find the failed tests in this log and separate the logs from the different nodes.

## Usage
1. Download log archive from CI.

2. Parse log.
```
python3 parser.py [filename]
```

3. View the separated logs in the current directory.