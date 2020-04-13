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

In order to achieve the above kinds of queries, a basic query component for a single time series is designed in the IoTDB query engine, and on this basis, various query functions are implemented.

## Related documents

* [Basic query components](/SystemDesign/5-DataQuery/2-SeriesReader.html)
* [Modification handle](/SystemDesign/5-DataQuery/3-ModificationHandle.html)
* [Raw data query](/SystemDesign/5-DataQuery/4-RawDataQuery.html)
* [Aggregate query](/SystemDesign/5-DataQuery/5-AggregationQuery.html)
* [Downsampling query](/SystemDesign/5-DataQuery/6-GroupByQuery.html)
* [Recent timestamp query](/SystemDesign/5-DataQuery/7-LastQuery.html)
* [Align by device query](/SystemDesign/5-DataQuery/8-AlignByDeviceQuery.html)
* [Fill function](/SystemDesign/5-DataQuery/9-FillFunction.html)