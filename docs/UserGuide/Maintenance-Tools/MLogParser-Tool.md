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

## MlogParser Tool

After version 0.12.x, IoTDB encodes metadata files into binary format.

If you want to parse metadata into a human-readable way, you can use this tool to parse the specified metadata file.

Currently, the tool can only parse mlog.bin file. 

If the consensus protocol used in cluster for SchemaRegion is RatisConsensus, IoTDB won't use mlog.bin file to store metadata and won't generate mlog.bin file.

### How to use

Linux/MacOS
> ./print-schema-log.sh -f /your path/mlog.bin -o /your path/mlog.txt

Windows

> .\print-schema-log.bat -f \your path\mlog.bin -o \your path\mlog.txt

