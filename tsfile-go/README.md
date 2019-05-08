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

Tsfile-go is a Golang version of tsfile(based on branch 'thanos'), it was developed by Lenovo & TsingHua, and it has the same features with tsfile, including:

1. Writing ts data to a tsfile
2. Reading/querying ts data from an existing tsfile
3. Encoding/decoding ts data with RLE/TS_2DIFF/GORILLA/PLAIN
4. Compression/decompression with snappy
