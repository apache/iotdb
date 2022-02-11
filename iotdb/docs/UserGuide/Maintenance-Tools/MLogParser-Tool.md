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

If you want to parse metadata into a human readable way, you can use this tool to parse the specified metadata file.

The tool can parse snapshot files and mlog files.

### How to use

Linux/MacOS
> ./mLogParser.sh -f /your path/mlog.bin -o /your path/mlog.txt

Windows

> .\mLogParser.bat -f \your path\mlog.bin -o \your path\mlog.txt

