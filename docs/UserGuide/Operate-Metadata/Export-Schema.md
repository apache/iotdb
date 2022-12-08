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

## Export Schema

The schema export operation exports the information about store group, timeseries, and schema template in the current IoTDB in the form of `mlog.bin` and `tlog.txt` to the specified directory.

The exported `mlog.bin` and `tlog.txt` files can be incrementally loaded into an IoTDB instance.

### Export Schema SQL

```
EXPORT SCHEMA '<path/dir>' 
```

### Load Schema

Please refer to [MLogLoad Tool](https://iotdb.apache.org/UserGuide/V0.13.x/Maintenance-Tools/MLogLoad-Tool.html)

