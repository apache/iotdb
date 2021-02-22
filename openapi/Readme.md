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

## Grafana

### query (return by DataFrame) (for IoTDB plugin)

query condition:
series paths, start time, end time, group by time interval, limit

### query (return by Json) (for SimplyJson plugin)

query condition:
series paths, start time, end time, group by time interval, limit

### getChildren  (return next level of a path)

query condition: the path of a parent (support wildcard '*', )

return: a set of children name

root -> ng-bj, ng-sh, sg-sz, sg-gz

root.ng-bj -> 100KW, 1000KW, 10MW

root.ng-sh -> 100KW, 1000KW, 10MW

### concretePaths

root.*.100KW ->
root.ng-bj.100KW root.ng-sh.100KW



