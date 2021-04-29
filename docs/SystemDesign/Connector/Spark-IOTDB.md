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

# Spark IOTDB connector

## aim of design

* Use Spark SQL to read IOTDB data and return it to the client in the form of a Spark DataFrame

## main idea
Because IOTDB has the ability to parse and execute SQL, this part can directly forward SQL to the IOTDB process for execution, and then convert the data to RDD.

## Implementation process
#### 1.Entrance

* src/main/scala/org/apache/iotdb/spark/db/DefaultSource.scala

#### 2. Building Relation
Relation mainly saves RDD meta-information, such as column names, partitioning strategies, and so on. Calling Relation's buildScan method can create RDDs

* src/main/scala/org/apache/iotdb/spark/db/IoTDBRelation.scala

#### 3. Building RDD
RDD executes SQL request to IOTDB and saves cursor

* The compute method in src / main / scala / org / apache / iotdb / spark / db / IoTDBRDD.scala

#### 4.Iterative RDD
Due to Spark's lazy loading mechanism, the RDD iteration is called specifically when the user traverses the RDD, which is the fetch result of IOTDB

* getNext method in src / main / scala / org / apache / iotdb / spark / db / IoTDBRDD.scala


## Wide and narrow table structure conversion
Wide table structure: IOTDB native path format

| time | root.ln.wf02.wt02.temperature | root.ln.wf02.wt02.status | root.ln.wf02.wt02.hardware | root.ln.wf01.wt01.temperature | root.ln.wf01.wt01.status | root.ln.wf01.wt01.hardware |
|------|-------------------------------|--------------------------|----------------------------|-------------------------------|--------------------------|----------------------------|
|    1 | null                          | true                     | null                       | 2.2                           | true                     | null                       |
|    2 | null                          | false                    | aaa                        | 2.2                           | null                     | null                       |
|    3 | null                          | null                     | null                       | 2.1                           | true                     | null                       |
|    4 | null                          | true                     | bbb                        | null                          | null                     | null                       |
|    5 | null                          | null                     | null                       | null                          | false                    | null                       |
|    6 | null                          | null                     | ccc                        | null                          | null                     | null                       |

Narrow table structure: Relational database schema, IOTDB align by device format

| time | device_name                   | status                   | hardware                   | temperature |
|------|-------------------------------|--------------------------|----------------------------|-------------------------------|
|    1 | root.ln.wf02.wt01             | true                     | null                       | 2.2                           |
|    1 | root.ln.wf02.wt02             | true                     | null                       | null                          |
|    2 | root.ln.wf02.wt01             | null                     | null                       | 2.2                          |
|    2 | root.ln.wf02.wt02             | false                    | aaa                        | null                           |
|    3 | root.ln.wf02.wt01             | true                     | null                       | 2.1                           |
|    4 | root.ln.wf02.wt02             | true                     | bbb                        | null                          |
|    5 | root.ln.wf02.wt01             | false                    | null                       | null                          |
|    6 | root.ln.wf02.wt02             | null                     | ccc                        | null                          |

Because the data queried by IOTDB defaults to a wide table structure, a wide-narrow table conversion is required. There are two implementation methods as follows

#### 1. Use the IOTDB group by device statement
This way you can get the narrow table structure directly, and the calculation is done by IOTDB

#### 2. Use Transformer
You can use Transformer to convert between wide and narrow tables. The calculation is done by Spark.

* src/main/scala/org/apache/iotdb/spark/db/Transformer.scala

Wide table to narrow table uses traversing the device list to generate the corresponding narrow table. The parallelization strategy is better (no shuffle). The narrow table to wide table uses a timestamp-based join operation. There is potential for shuffle.  Performance issues