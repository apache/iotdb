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

# 1.切换方案

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

适配器是一个继承至InfluxDB基类的子类，实现了InfluxDB接口的所有方法，从而使InfluxDB原有的操作函数没有改变，但是会以IoTDB的协议写入IoTDB数据库中。

![architecture-design](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/architecture-design.png?raw=true)

![class-diagram](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/class-diagram.png?raw=true)


### 2.2 数据格式转换

#### 2.2.1 InfluxDB数据格式

1. database: 数据库名。
2. measurement: 测量指标名。
3. tags : 各种有索引的属性。
4. fields : 各种记录值（没有索引的属性）。

![influxdb-data](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/influxdb-data.png?raw=true)

#### 2.2.2 IoTDB数据格式

1. storage group： 存储组。
2. path(time series ID)：存储路径。
3. measurement： 物理量。

![iotdb-data](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/iotdb-data.png?raw=true)

#### 2.2.3 两者映射关系

1. InfluxDB中的database和measurement可以看做IoTDB中的storage group。
2. InfluxDB中的tags可以看做IoTDB中的path。
3. InfluxDB中的fields可以看做IoTDB中measurement。

![influxdb-vs-iotdb-data](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/influxdb-vs-iotdb-data.png?raw=true)

#### 2.2.4 转换中的问题
1. 问题：InfluxDB中Tag的顺序不敏感，而在IoTDB中是敏感的。
2. 关键点：需要记录每个tag对应的顺序，确保InfluxDB中label顺序不同的同一条时序对应到IoTDB中也是一条时序。
3. 需要解决的事情：
    1. 怎样映射tag key和它对应的order
    2. 在不知道所有的label key的情况下，怎么维护他们之间的顺序

### 2.3 解决方案

#### 2.3.1 主要思想

1. 内存中Map <Measurement, Map <Tag Key, Order> > table结构维护Tag之间的顺序
2. InfluxDB中时序根据label顺序对应到IoTDB

   | `root.TAG_INFO.database_name` | `root.TAG_INFO.measurement_name` | `root.TAG_INFO.tag_name` | `root.TAG_INFO.tag_order` |
            | :---------------------------- | :------------------------------- | :----------------------- | :------------------------ |
   | database                      | student                          | name                     | 0                         |
   | database                      | student                          | phone                    | 1                         |
   | database                      | student                          | sex                      | 2                         |
   | database                      | student                          | address                  | 3                         |

#### 2.3.2 实例

a. 插入数据

1. InfluxDB时序(database=database)：

   (1)student tags:{name=A,phone=B,sex=C} fields:{score=99}

   (2)student tags:{address=D} fields:{score=98}

   (3))student tags:{name=A,phone=B,sex=C,address=D} fields:{score=97}

2. 简单对上述InfluxDB的时序进行解释，database是database；measurement是student；tag分别是name，phone、sex和address；field是score。

对应的InfluxDB的实际存储为：

![influxdb-result](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/influxdb-result.png?raw=true)




3. (1)IoTDB对应的记录tag顺序的table为

   | database | measurement | tag_key | Order |
      | -------- | ----------- | ------- | ----- |
   | database | student     | name    | 0     |
   | database | student     | phone   | 1     |
   | database | student     | sex     | 2     |

   (2)IoTDB对应的记录tag顺序的table为

   | database | measurement | tag_key | order |
      | -------- | ----------- | ------- | ----- |
   | database | student     | name    | 0     |
   | database | student     | phone   | 1     |
   | database | student     | sex     | 2     |
   | database | student     | address | 3     |

   (3)IoTDB对应的记录tag顺序的table为

   | database | measurement | tag_key | order |
      | -------- | ----------- | ------- | ----- |
   | database | student     | name    | 0     |
   | database | student     | phone   | 1     |
   | database | student     | sex     | 2     |
   | database | student     | address | 3     |

4. (1)第一条插入数据对应IoTDB时序为root.database.student.A.B.C

   (2)第二条插入数据对应IoTDB时序为root.database.student.ph.ph.ph.D(其中ph表示占位符)

   (3)第三条插入数据对应IoTDB时序为root.database.student.A.B.C.D 
  
   对应的IoTDB的实际存储为：

| Time | root.database.student.A.B.C.score | root.database.student.PH.PH.PH.D.score | root.database.student.A.B.C.D.score |
| ---- | --------------------------------- | -------------------------------------- | ----------------------------------- |
| 1    | 99                                | null                                   | Null                                |
| 2    | Null                              | 98                                     | Null                                |
| 3    | null                              | null                                   | 97                                  |

b. 查询数据

1. 查询student中phone=B的数据。在database->student中phone的顺序为1，order最大值是3，对应到IoTDB的查询为：select * from root.database.student.*.B
2. 查询student中phone=B且存储的score>97的数据，对应到IoTDB的查询为：select * from root.database.student.*.B where score>98
3. 查询student中phone=B且存储的score>97且时间在最近七天内的的数据，对应到IoTDB的查询为：select * from root.database.student.*.B where score>98 and time > now()-7d