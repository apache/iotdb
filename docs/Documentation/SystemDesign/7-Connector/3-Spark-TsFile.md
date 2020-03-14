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

# Spark Tsfile connector

## aim of design

* Use Spark SQL to read the data of the specified Tsfile and return it to the client in the form of a Spark DataFrame

* Generate Tsfile with data from Spark Dataframe

## Supported formats
Wide table structure: Tsfile native format, IOTDB native path format

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

## Query process steps

#### 1. Table structure inference and generation
This step is to make the table structure of the DataFrame match the table structure of the Tsfile to be queried.
The main logic is inferSchema function in src / main / scala / org / apache / iotdb / spark / tsfile / DefaultSource.scala

#### 2. SQL parsing
The purpose of this step is to transform user SQL statements into Tsfile native query expressions.

The main logic is the buildReader function in src / main / scala / org / apache / iotdb / spark / tsfile / DefaultSource.scala. SQL parsing wide table structure and narrow table structure

#### 3. Wide table structure

The main logic of the SQL analysis of the wide table structure is in src / main / scala / org / apache / iotdb / spark / tsfile / WideConverter.scala. This structure is basically the same as the Tsfile native query structure. No special processing is required, and the SQL statement is directly converted into  Corresponding query expression

#### 4. Narrow table structure
The main logic of the SQL analysis of the wide table structure is src / main / scala / org / apache / iotdb / spark / tsfile / NarrowConverter.scala. After the SQL is converted to an expression, the narrow table structure is different from the Tsfile native query structure.  The expression is converted into a disjunction expression related to the device before it can be converted into a query of Tsfile. The conversion code is in src / main / java / org / apache / iotdb / spark / tsfile / qp

#### 5. 查询实际执行
The actual data query execution is performed by the Tsfile native component, see:

* [Tsfile native query process](../1-TsFile/4-Read.md)

## Write step flow
Writing is mainly to convert the data in the Dataframe structure into Tsfile's RowRecord, and write using Tsfile Writer

#### Wide table structure
The main conversion code is in the following two files:

* src/main/scala/org/apache/iotdb/spark/tsfile/WideConverter.scala responsible for structural transformation

* src/main/scala/org/apache/iotdb/spark/tsfile/WideTsFileOutputWriter.scala responsible for matching the spark interface and performing writes, which will call the structure conversion function in the previous file

#### Narrow table structure
The main conversion code is in the following two files:

* src/main/scala/org/apache/iotdb/spark/tsfile/NarrowConverter.scala responsible for structural transformation

* src/main/scala/org/apache/iotdb/spark/tsfile/NarrowTsFileOutputWriter.scala responsible for matching the spark interface and performing writes, which will call the structure conversion function in the previous file

