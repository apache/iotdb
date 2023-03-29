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

# REST API Write

Refer to [insertTablet (v1)](../API/RestServiceV1.md#inserttablet) or [insertTablet (v2)](../API/RestServiceV2.md#inserttablet)

Example：

```JSON
{
      "timestamps": [
            1,
            2,
            3
      ],
      "measurements": [
            "temperature",
            "status"
      ],
      "data_types": [
            "FLOAT",
            "BOOLEAN"
      ],
      "values": [
            [
                  1.1,
                  2.2,
                  3.3
            ],
            [
                  false,
                  true,
                  true
            ]
      ],
      "is_aligned": false,
      "device": "root.ln.wf01.wt01"
}
```