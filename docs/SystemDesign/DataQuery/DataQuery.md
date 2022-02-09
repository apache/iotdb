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

# Data query

There are several types of data queries

* Raw data query
* Aggregate query
* Downsampling query
* Single point supplementary null query
* Latest data query
* Align by device query
* Group by fill query

In order to achieve the above kinds of queries, a basic query component for a single time series is designed in the IoTDB query engine, and on this basis, various query functions are implemented.

## Related documents

* [Query fundamentals](../DataQuery/QueryFundamentals.md)
* [Basic query components](../DataQuery/SeriesReader.md)
* [Raw data query](../DataQuery/RawDataQuery.md)
* [Aggregate query](../DataQuery/AggregationQuery.md)
* [Downsampling query](../DataQuery/GroupByQuery.md)
* [Recent timestamp query](../DataQuery/LastQuery.md)
* [Align by device query](../DataQuery/AlignByDeviceQuery.md)
* [Fill function](../DataQuery/FillFunction.md)
* [Group by fill query](../DataQuery/GroupByFillQuery.md)
