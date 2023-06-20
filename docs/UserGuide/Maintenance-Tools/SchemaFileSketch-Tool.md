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

## PBTreeFileSketch Tool

Since version 1.1, IoTDB could store schema into a persistent slotted file.

If you want to parse PBTree file into a human-readable way, you can use this tool to parse the specified PBTree file.

The tool can sketch .pst file.

### How to use

Linux/MacOS
> ./print-pbtree-file.sh -f your/path/to/pbtree.pst -o /your/path/to/sketch.txt

Windows

> ./print-pbtree-file.bat -f your/path/to/pbtree.pst -o /your/path/to/sketch.txt

