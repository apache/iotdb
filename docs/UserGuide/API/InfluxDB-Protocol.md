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

## 0. Import Dependency

```xml
    <dependency>
        <groupId>org.apache.iotdb</groupId>
        <artifactId>influxdb-protocol</artifactId>
        <version>1.0.0</version>
    </dependency>
```

Here are some [examples](https://github.com/apache/iotdb/tree/master/example/inflxudb-protocol-example/src/main/java/org/apache/iotdb/influxdb/InfluxDBExample.java) of connecting IoTDB using the InfluxDB-Protocol adapter.

## 1. Switching Scheme

If your original service code for accessing InfluxDB is as follows:

```java
InfluxDB influxDB = InfluxDBFactory.connect(openurl, username, password);
```

You only need to replace the InfluxDBFactory with **IoTDBInfluxDBFactory** to switch the business to IoTDB：

```java
InfluxDB influxDB = IoTDBInfluxDBFactory.connect(openurl, username, password);
```

## 2. Conceptual Design

### 2.1 InfluxDB-Protocol Adapter

Based on the IoTDB Java ServiceProvider interface, the adapter implements the 'interface InfluxDB' of the java interface of InfluxDB, and provides users with all the interface methods of InfluxDB. End users can use the InfluxDB protocol to initiate write and read requests to IoTDB without perception.

![architecture-design](https://alioss.timecho.com/docs/img/UserGuide/API/IoTDB-InfluxDB/architecture-design.png?raw=true)

![class-diagram](https://alioss.timecho.com/docs/img/UserGuide/API/IoTDB-InfluxDB/class-diagram.png?raw=true)


### 2.2 Metadata Format Conversion
The metadata of InfluxDB is tag field model, and the metadata of IoTDB is tree model. In order to make the adapter compatible with the InfluxDB protocol, the metadata model of InfluxDB needs to be transformed into the metadata model of IoTDB.

#### 2.2.1 InfluxDB Metadata

1. database: database name.
2. measurement: measurement name.
3. tags: various indexed attributes.
4. fields: various record values(attributes without index).

![influxdb-data](https://alioss.timecho.com/docs/img/UserGuide/API/IoTDB-InfluxDB/influxdb-data.png?raw=true)

#### 2.2.2 IoTDB Metadata

1. database: database name.
2. path(time series ID): storage path.
3. measurement: physical quantity.

![iotdb-data](https://alioss.timecho.com/docs/img/UserGuide/API/IoTDB-InfluxDB/iotdb-data.png?raw=true)

#### 2.2.3 Mapping relationship between the two

The mapping relationship between InfluxDB metadata and IoTDB metadata is as follows:
1. The database and measurement in InfluxDB are combined as the database in IoTDB.
2. The field key in InfluxDB is used as the measurement path in IoTDB, and the field value in InfluxDB is the measured point value recorded under the path.
3. Tag in InfluxDB is expressed by the path between database and measurement in IoTDB. The tag key of InfluxDB is implicitly expressed by the order of the path between database and measurement, and the tag value is recorded as the name of the path in the corresponding order.

The transformation relationship from InfluxDB metadata to IoTDB metadata can be represented by the following publicity:

`root.{database}.{measurement}.{tag value 1}.{tag value 2}...{tag value N-1}.{tag value N}.{field key}`

![influxdb-vs-iotdb-data](https://alioss.timecho.com/docs/img/UserGuide/API/IoTDB-InfluxDB/influxdb-vs-iotdb-data.png?raw=true)

As shown in the figure above, it can be seen that:

In IoTDB, we use the path between database and measurement to express the concept of InfluxDB tag, which is the part of the green box on the right in the figure.

Each layer between database and measurement represents a tag. If the number of tag keys is n, the number of layers of the path between database and measurement is n. We sequentially number each layer between database and measurement, and each sequence number corresponds to a tag key one by one. At the same time, we use the **path name** of each layer between database and measurement to remember tag value. Tag key can find the tag value under the corresponding path level through its own serial number.

#### 2.2.4 Key Problem

In the SQL statement of InfluxDB, the different order of tags does not affect the actual execution .

For example: `insert factory, workshop=A1, production=B1, temperature=16.9` and `insert factory, production=B1, workshop=A1, temperature=16.9` have the same meaning (and execution result) of the two InfluxDB SQL.

However, in IoTDB, the above inserted data points can be stored in `root.monitor.factory.A1.B1.temperature` can also be stored in `root.monitor.factory.B1.A1.temperature`. Therefore, the order of the tags of the InfluxDB stored in the IoTDB path needs special consideration because `root.monitor.factory.A1.B1.temperature` and

`root.monitor.factory.B1.A1.temperature` is two different sequences. We can think that iotdb metadata model is "sensitive" to the processing of tag order.

Based on the above considerations, we also need to record the hierarchical order of each tag in the IoTDB path in the IoTDB, as to ensure that the adapter can only operate on a time series in the IoTDB as long as the SQL expresses operations on the same time series, regardless of the order in which the tags appear in the InfluxDB SQL.

Another problem that needs to be considered here is how to persist the tag key and corresponding order relationship of InfluxDB into the IoTDB database to ensure that relevant information will not be lost.

**Solution:**

**The form of tag key correspondence in memory**

Maintain the order of tags at the IoTDB path level by using the map structure of `Map<Measurement,Map<Tag key, order>>` in memory.

``` java
    Map<String, Map<String, Integer>> measurementTagOrder
```

It can be seen that map is a two-tier structure.

The key of the first layer is an InfluxDB measurement of string type, and the value of the first layer is a Map<string,Integer> structure.

The key of the second layer is the InfluxDB tag key of string type, and the value of the second layer is the tag order of Integer type, that is, the order of tags at the IoTDB path level.

When in use, you can first locate the tag through the InfluxDB measurement, then locate the tag through the InfluxDB tag key, and finally get the order of tags at the IoTDB path level.

**Persistence scheme of tag key correspondence order**

Database is `root.TAG_ Info`, using `database_name`,`measurement_ name`, `tag_ Name ` and ` tag_ Order ` under the database to store tag key and its corresponding order relationship by measuring points.

```
+-----------------------------+---------------------------+------------------------------+----------------------+-----------------------+
|                         Time|root.TAG_INFO.database_name|root.TAG_INFO.measurement_name|root.TAG_INFO.tag_name|root.TAG_INFO.tag_order|
+-----------------------------+---------------------------+------------------------------+----------------------+-----------------------+
|2021-10-12T01:21:26.907+08:00|                    monitor|                       factory|              workshop|                      1|
|2021-10-12T01:21:27.310+08:00|                    monitor|                       factory|            production|                      2|
|2021-10-12T01:21:27.313+08:00|                    monitor|                       factory|                  cell|                      3|
|2021-10-12T01:21:47.314+08:00|                   building|                           cpu|              tempture|                      1|
+-----------------------------+---------------------------+------------------------------+----------------------+-----------------------+
```


### 2.3 Example

#### 2.3.1 Insert records

1. Suppose three pieces of data are inserted into the InfluxDB in the following order (database = monitor):

   (1)`insert student,name=A,phone=B,sex=C score=99`

   (2)`insert student,address=D score=98`

   (3)`insert student,name=A,phone=B,sex=C,address=D score=97`

2. Simply explain the timing of the above InfluxDB, and database is monitor; Measurement is student; Tag is name, phone, sex and address respectively; Field is score.

The actual storage of the corresponding InfluxDB is:

```
time                address name phone sex socre
----                ------- ---- ----- --- -----
1633971920128182000         A    B     C   99
1633971947112684000 D                      98
1633971963011262000 D       A    B     C   97
```

3. The process of inserting three pieces of data in sequence by IoTDB is as follows:

   (1) When inserting the first piece of data, we need to update the three new tag keys to the table. The table of the record tag sequence corresponding to IoTDB is:

   | database | measurement | tag_key | Order |
          | -------- | ----------- | ------- | ----- |
   | monitor | student     | name    | 0     |
   | monitor | student     | phone   | 1     |
   | monitor | student     | sex     | 2     |

   (2) When inserting the second piece of data, since there are already three tag keys in the table recording the tag order, it is necessary to update the record with the fourth tag key=address. The table of the record tag sequence corresponding to IoTDB is:

   | database | measurement | tag_key | order |
          | -------- | ----------- | ------- | ----- |
   | monitor | student     | name    | 0     |
   | monitor | student     | phone   | 1     |
   | monitor | student     | sex     | 2     |
   | monitor | student     | address | 3     |

   (3) When inserting the third piece of data, the four tag keys have been recorded at this time, so there is no need to update the record. The table of the record tag sequence corresponding to IoTDB is:

   | database | measurement | tag_key | order |
          | -------- | ----------- | ------- | ----- |
   | monitor | student     | name    | 0     |
   | monitor | student     | phone   | 1     |
   | monitor | student     | sex     | 2     |
   | monitor | student     | address | 3     |

4. (1) The IoTDB sequence corresponding to the first inserted data is root.monitor.student.A.B.C

   (2) The IoTDB sequence corresponding to the second inserted data is root.monitor.student.PH.PH.PH.D (where PH is a placeholder).

   It should be noted that since the tag key = address of this data appears the fourth, but it does not have the corresponding first three tag values, it needs to be replaced by a PH. The purpose of this is to ensure that the tag order in each data will not be disordered, which is consistent with the order in the current order table, so that the specified tag can be filtered when querying data.

   (3) The IoTDB sequence corresponding to the second inserted data is root.monitor.student.A.B.C.D

   The actual storage of the corresponding IoTDB is:

```
+-----------------------------+--------------------------------+-------------------------------------+----------------------------------+
|                         Time|root.monitor.student.A.B.C.score|root.monitor.student.PH.PH.PH.D.score|root.monitor.student.A.B.C.D.score|
+-----------------------------+--------------------------------+-------------------------------------+----------------------------------+
|2021-10-12T01:21:26.907+08:00|                              99|                                 NULL|                              NULL|
|2021-10-12T01:21:27.310+08:00|                            NULL|                                   98|                              NULL|
|2021-10-12T01:21:27.313+08:00|                            NULL|                                 NULL|                                97|
+-----------------------------+--------------------------------+-------------------------------------+----------------------------------+
```

5. If the insertion order of the above three data is different, we can see that the corresponding actual path has changed, because the order of tags in the InfluxDB data has changed, and the order of the corresponding path nodes in IoTDB has also changed.

However, this will not affect the correctness of the query, because once the tag order of InfluxDB is determined, the query will also filter the tag values according to the order recorded in this order table. Therefore, the correctness of the query will not be affected.

#### 2.3.2 Query Data

1. Query the data of phone = B in student. In database = monitor, measurement = student, the order of tag = phone is 1, and the maximum order is 3. The query corresponding to IoTDB is:

   ```sql 
   select * from root.monitor.student.*.B
   ```

2. Query the data with phone = B and score > 97 in the student. The query corresponding to IoTDB is:

   ```sql
   select * from root.monitor.student.*.B where score>97 
   ```

3. Query the data of the student with phone = B and score > 97 in the last seven days. The query corresponding to IoTDB is:

   ```sql
   select * from root.monitor.student.*.B where score>97 and time > now()-7d 
   ```

4. Query the name = a or score > 97 in the student. Since the tag is stored in the path, there is no way to complete the **or** semantic query of tag and field at the same time with one query. Therefore, multiple queries or operation union set are required. The query corresponding to IoTDB is:

   ```sql
   select * from root.monitor.student.A 
   select * from root.monitor.student where score>97
   ```
   Finally, manually combine the results of the above two queries.

5. Query the student (name = a or phone = B or sex = C) with a score > 97. Since the tag is stored in the path, there is no way to use one query to complete the **or** semantics of the tag. Therefore, multiple queries or operations are required to merge. The query corresponding to IoTDB is:

   ```sql
   select * from root.monitor.student.A where score>97
   select * from root.monitor.student.*.B where score>97
   select * from root.monitor.student.*.*.C where score>97
   ```
   Finally, manually combine the results of the above three queries.

## 3. Support

### 3.1 InfluxDB Version Support

Currently, supports InfluxDB 1.x version, which does not support InfluxDB 2.x version.

The Maven dependency of `influxdb-java` supports 2.21 +, and the lower version is not tested.


### 3.2 Function Interface Support

Currently, supports interface functions are as follows:

```java
public Pong ping();

public String version();

public void flush();

public void close();

public InfluxDB setDatabase(final String database);

public QueryResult query(final Query query);

public void write(final Point point);

public void write(final String records);

public void write(final List<String> records);

public void write(final String database,final String retentionPolicy,final Point point);

public void write(final int udpPort,final Point point);

public void write(final BatchPoints batchPoints);

public void write(final String database,final String retentionPolicy,
final ConsistencyLevel consistency,final String records);

public void write(final String database,final String retentionPolicy,
final ConsistencyLevel consistency,final TimeUnit precision,final String records);

public void write(final String database,final String retentionPolicy,
final ConsistencyLevel consistency,final List<String> records);

public void write(final String database,final String retentionPolicy,
final ConsistencyLevel consistency,final TimeUnit precision,final List<String> records);

public void write(final int udpPort,final String records);

public void write(final int udpPort,final List<String> records);
```

### 3.3 Query Syntax Support

The currently supported query SQL syntax is:

```sql
SELECT <field_key>[, <field_key>, <tag_key>]
FROM <measurement_name>
WHERE <conditional_expression > [( AND | OR) <conditional_expression > [...]]
```

WHERE clause supports `conditional_expressions` on `field`，`tag` and `timestamp`.

#### field

```sql
field_key <operator> ['string' | boolean | float | integer]
```

#### tag

```sql
tag_key <operator> ['tag_value']
```

#### timestamp

```sql
timestamp <operator> ['time']
```

At present, the filter condition of timestamp only supports the expressions related to now(), such as now () - 7d. The specific timestamp is not supported temporarily.
