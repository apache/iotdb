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

## 1.切换方案

假如您原先接入 InfluxDB 的业务代码如下：

```java
InfluxDB influxDB = InfluxDBFactory.connect(openurl, username, password);
```

您只需要将 InfluxDBFactory 替换为 **IoTDBInfluxDBFactory** 即可实现业务向 IoTDB 的切换：

```java
InfluxDB influxDB = IoTDBInfluxDBFactory.connect(openurl, username, password);
```

## 2.方案设计

### 2.1 InfluxDB-Protocol适配器

该适配器以 IoTDB Java Session 接口为底层基础，实现了 InfluxDB 的 Java 接口 `interface InfluxDB`，对用户提供了所有 InfluxDB 的接口方法，最终用户可以无感知地使用 InfluxDB 协议向 IoTDB 发起写入和读取请求。

![architecture-design](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/architecture-design.png?raw=true)

![class-diagram](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/class-diagram.png?raw=true)


### 2.2 元数据格式转换

InfluxDB 的元数据是 tag-field 模型，IoTDB 的元数据是树形模型。为了使适配器能够兼容 InfluxDB 协议，需要把 InfluxDB 的元数据模型转换成 IoTDB 的元数据模型。

#### 2.2.1 InfluxDB 元数据

1. database: 数据库名。
2. measurement: 测量指标名。
3. tags : 各种有索引的属性。
4. fields : 各种记录值（没有索引的属性）。

![influxdb-data](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/influxdb-data.png?raw=true)

#### 2.2.2 IoTDB 元数据

1. storage group： 存储组。
2. path(time series ID)：存储路径。
3. measurement： 物理量。

![iotdb-data](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/iotdb-data.png?raw=true)

#### 2.2.3 两者映射关系

InfluxDB 元数据和 IoTDB 元数据有着如下的映射关系：
1. InfluxDB 中的 database 和 measurement 组合起来作为 IoTDB 中的 storage group。
2. InfluxDB 中的 field key 作为 IoTDB 中 measurement 路径，InfluxDB 中的 field value 即是该路径下记录的测点值。
3. InfluxDB 中的 tag 在 IoTDB 中使用 storage group 和 measurement 之间的路径表达。InfluxDB 的 tag key 由 storage group 和 measurement 之间路径的顺序隐式表达，tag value 记录为对应顺序的路径的名称。

InfluxDB 元数据向 IoTDB 元数据的转换关系可以由下面的公示表示：

`root.{database}.{measurement}.{tag value 1}.{tag value 2}...{tag value N-1}.{tag value N}.{field key}`

![influxdb-vs-iotdb-data](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/influxdb-vs-iotdb-data.png?raw=true)

如上图所示，可以看出：

我们在 IoTDB 中使用 storage group 和 measurement 之间的路径来表达 InfluxDB tag 的概念，也就是图中右侧绿色方框的部分。

storage group 和 measurement 之间的每一层都代表一个 tag。如果 tag key 的数量为 N，那么 storage group 和 measurement 之间的路径的层数就是 N。我们对 storage group 和 measurement 之间的每一层进行顺序编号，每一个序号都和一个 tag key 一一对应。同时，我们使用 storage group 和 measurement 之间每一层 **路径的名字** 来记 tag value，tag key 可以通过自身的序号找到对应路径层级下的 tag value.

在InfluxDB中，Tag的顺序不同并不会影响实际的结果。

eg:
`insert factory,workshop=A1,production=B1 tempture=16.9`和`insert factory,production=B1,workshop=A1 tempture=16.9`两条数据表达的含义相等。

但在IoTDB中，一个path由多个部分组成，比如`root.monitor.factory.A1.B1`是由一个存储组`root.monitor.factory`和两个节点`A1`和`B1`组成的。

那么节点的顺序就是需要考虑的，因为`root.monitor.factory.A1.B1`和`root.monitor.factory.B1.A1`是两条不同的序列，因此可以认为IoTDB中对Tag对顺序是敏感的。

所以需要记录InfluxDB每个Tag对应的顺序，确保在InfluxDB中，即使Tag不同的同一条时序对应到IoTDB中也是同一条时序。

需要解决的事情：
   1. 怎样把InfluxDB中tag key映射到IoTDB中path中的节点顺序。
   2. 在不知道InfluxDB中可能会出现所有tag key的情况下，怎么维护它们之间的顺序。

解决方案：
1. 通过利用内存中的Map <Measurement, Map <Tag Key, Order> > Map结构，来维护Tag之间的顺序。

   ```java
   Map<String, Map<String, Integer>> measurementTagOrder
   ```
   可以看出Map是一个两层的，第一层的Key是String类型的Measurement，第一层的Value是一个<String,Integer>的Map。
   
   第二层的Key是String类型的Tag Key，第二层的Value是Integer类型的Tag Order，也就是Tag对应的实际顺序。

   这样就可以先通过measurement定位，再通过tag key定位，最后就可以获得Tag对应的顺序了。
 
3. 除了在内存中维护InfluxDB的Tag顺序外，还需要进行持久化操作，将InfluxDB的Tag顺序持久化到IoTDB数据库中里。
   存储组为`root.TAG_INFO`，分别以`database_name`,`measurement_name`,`tag_name`和`tag_order`来存储节点来记录数据。

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



### 2.3 实例

#### 2.3.1 插入数据

1. 假定按照以下的顺序插入三条数据到InfluxDB中(database=monitor)：
   
   (1)`insert student,name=A,phone=B,sex=C score=99`

   (2)`insert student,address=D score=98` 

   (3)`insert student,name=A,phone=B,sex=C,address=D score=97`

2. 简单对上述InfluxDB的时序进行解释，database是monitor;measurement是student；tag分别是name，phone、sex和address；field是score。

对应的InfluxDB的实际存储为：

```
time                address name phone sex socre
----                ------- ---- ----- --- -----
1633971920128182000         A    B     C   99
1633971947112684000 D                      98
1633971963011262000 D       A    B     C   97
```


3. IoTDB顺序插入三条数据的过程如下：

   (1)插入第一条数据时，需要将新出现的三个tag key更新到table中，IoTDB对应的记录tag顺序的table为：

   | database | measurement | tag_key | Order |
      | -------- | ----------- | ------- | ----- |
   | monitor | student     | name    | 0     |
   | monitor | student     | phone   | 1     |
   | monitor | student     | sex     | 2     |

   (2)插入第二条数据时，由于此时记录tag顺序的table中已经有了三个tag key，因此需要将出现的第四个tag key=address更新记录。IoTDB对应的记录tag顺序的table为：

   | database | measurement | tag_key | order |
      | -------- | ----------- | ------- | ----- |
   | monitor | student     | name    | 0     |
   | monitor | student     | phone   | 1     |
   | monitor | student     | sex     | 2     |
   | monitor | student     | address | 3     |

   (3)插入第三条数据时，此时四个tag key都已经记录过，所以不需要更新记录，IoTDB对应的记录tag顺序的table为：

   | database | measurement | tag_key | order |
      | -------- | ----------- | ------- | ----- |
   | monitor | student     | name    | 0     |
   | monitor | student     | phone   | 1     |
   | monitor | student     | sex     | 2     |
   | monitor | student     | address | 3     |

4. (1)第一条插入数据对应IoTDB时序为root.monitor.student.A.B.C

   (2)第二条插入数据对应IoTDB时序为root.monitor.student.PH.PH.PH.D(其中PH表示占位符)。
    
   需要注意的是，由于该条数据的tag key=address是第四个出现的，但是自身却没有对应的前三个tag值，因此需要用PH占位符来代替。这样做的目的是保证每条数据中的tag顺序不会乱，是符合当前顺序表中的顺序，从而查询数据的时候可以进行指定tag过滤。

   (3)第三条插入数据对应IoTDB时序为root.monitor.student.A.B.C.D 
  
   对应的IoTDB的实际存储为：

```
+-----------------------------+--------------------------------+-------------------------------------+----------------------------------+
|                         Time|root.monitor.student.A.B.C.score|root.monitor.student.PH.PH.PH.D.score|root.monitor.student.A.B.C.D.score|
+-----------------------------+--------------------------------+-------------------------------------+----------------------------------+
|2021-10-12T01:21:26.907+08:00|                              99|                                 NULL|                              NULL|
|2021-10-12T01:21:27.310+08:00|                            NULL|                                   98|                              NULL|
|2021-10-12T01:21:27.313+08:00|                            NULL|                                 NULL|                                97|
+-----------------------------+--------------------------------+-------------------------------------+----------------------------------+
```

5. 如果上面三条数据插入的顺序不一样，我们可以看到对应的实际path路径也就发生了改变，因为InfluxDB数据中的Tag出现顺序发生了变化，所对应的到IoTDB中的path节点顺序也就发生了变化。

   但是这样实际并不会影响查询的正确性，因为一旦Influxdb的Tag顺序确定之后，查询也会按照这个顺序表记录的顺序进行Tag值过滤。所以并不会影响查询的正确性。

#### 2.3.2 查询数据

1. 查询student中phone=B的数据。在database=monitor,measurement=student中tag=phone的顺序为1，order最大值是3，对应到IoTDB的查询为：
 
   ```sql 
   select * from root.monitor.student.*.B
   ```

2. 查询student中phone=B且score>97的数据，对应到IoTDB的查询为：
 
   ```sql
   select * from root.monitor.student.*.B where score>97 
   ```

3. 查询student中phone=B且score>97且时间在最近七天内的的数据，对应到IoTDB的查询为：
 
   ```sql
   select * from root.monitor.student.*.B where score>97 and time > now()-7d 
   ```


4. 查询student中name=A或score>97，由于tag存储在路径中，因此没有办法用一次查询同时完成tag和field的*或*语义查询，因此需要多次查询进行或运算求并集，对应到IoTDB的查询为：

   ```sql
   select * from root.monitor.student.A 
   select * from root.monitor.student where score>97
   ```
   最后手动对上面两次查询结果求并集。

5. 查询student中(name=A或phone=B或sex=C)且score>97，由于tag存储在路径中，因此没有办法用一次查询完成tag的**或**语义， 因此需要多次查询进行或运算求并集，对应到IoTDB的查询为：

   ```sql
   select * from root.monitor.student.A where score>97
   select * from root.monitor.student.*.B where score>97
   select * from root.monitor.student.*.*.C where score>97
   ```
   最后手动对上面三次查询结果求并集。

