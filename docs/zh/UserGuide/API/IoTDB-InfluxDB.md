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

# IoTDB-InfluxDB适配器


## 1.背景

InfluxDB是当前世界排名第一的时序数据库，具有繁荣的生态系统，目前很多用户使用它来作为自己的第一选择。但是，实际上线后，会有高可用，高扩展的需求。如果换成别的数据库，会有比较高的迁移成本。

## 2.目标 

开发一套Java版本的适配器可以使IoTDB兼容InfluxDB协议，完善IoTDB的功能。

1. IoTDB-InfluxDB：支持InfluxDB写入；支持InfluxDB部分查询；支持完整的InfluxDB查询。
2. 对正确性和性能的测试，不仅要适配InfluxDB，也要知道在繁重的负载下是否可以很好的工作，例如：以非常高的频率生成数据
   1. 正确性测试：通过适配器以influxdb的协议插入数据，然后查询IoTDB数据库，将我们认为发送的内容与我们希望存储的内容进行比较。进行正确性测试
   2. 性能测试：以多线程的方式或者以Fiber多协程方式并发写入和读取，进行性能测试，类似的 demo：https://github.com/Tencent/TencentKona-8/tree/KonaFiber/demo/fiber/iotdb-sync-stress-demo


## 3.方案一

### 3.1 IoTDB-InfluxDB适配器

适配器是一个继承至InfluxDB基类的子类，实现了InfluxDB主要的写入和查询方法，用户通过改变代码中InfluxDB的实现类，从而使InfluxDB原有的操作函数没有改变，但是会以IoTDB的协议写入IoTDB数据库中。

![architecture-design](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/architecture-design.png?raw=true)


### 3.2 将InfluxDB的数据格式转换成IoTDB的数据格式

#### 3.2.1问题

1. 问题：InfluxDB中Tag的顺序不敏感，而在IoTDB中是敏感的。

   a. InfluxDB的时序构成

      i. measurement

      ii.tag key tag value

      iii. field key field value

   b. IoTDB的时序构成

      i. storage group

      ii. path(time series ID)

      iii. measurement

2. 关键点：需要记录每个tag对应的顺序，确保InfluxDB中label顺序不同的同一条时序对应到IoTDB中也是一条时序
3. 需要解决的事情
   1. 怎样映射tag key和它对应的order
   2. 在不知道所有的label key的情况下，怎么维护他们之间的顺序

### 3.2.2解决方案

##### 3.2.2.1主要思想

1. 内存中Map <Database_Measurement, Map <Tag Key, Order> > table结构维护Tag之间的顺序
2. InfluxDB中时序根据label顺序对应到IoTDB

##### 3.2.2.2实例

a. 添加时序

   1.  InfluxDB时序(database=testdabase)：

      (1)student{name=A,phone=B,sex=C}

      (2)student{address=D}

      (3))student{name=A,phone=B,sex=C,address=D} 

   2. 简单对上述InfluxDB的时序进行解释，database是testdatabase；measurement是student；tag分别是name，phone、sex和address

   3.  (1)对应的记录tag顺序的table为

          

      | database_measurement | tag_key | order |
      | -------------------- | ------- | ----- |
      | testdatabase_student | name    | 0     |
      | testdatabase_student | phone   | 1     |
      | testdatabase_student | sex     | 2     |

      (2)对应的记录tag顺序的table为

          

      | database_measurement | tag_key | order |
      | -------------------- | ------- | ----- |
      | testdatabase_student | name    | 0     |
      | testdatabase_student | phone   | 1     |
      | testdatabase_student | sex     | 2     |
      | testdatabase_student | address | 3     |

      (3)对应的记录tag顺序的table为

          

      | database_measurement | tag_key | order |
      | -------------------- | ------- | ----- |
      | testdatabase_student | name    | 0     |
      | testdatabase_student | phone   | 1     |
      | testdatabase_student | sex     | 2     |
      | testdatabase_student | address | 3     |

4. (1)对应IoTDB时序为root.testdatabase.student.A.B.C

   (2)对应IoTDB时序为root.testdatabase.student.ph.ph.ph.D(其中ph表示占位符)

   (3)对应IoTDB时序为root.testdatabase.student.A.B.C.D

5. 为了重启时候对table的恢复，在IoTDB中记录数据

   | `root.TAG_INFO.database_name` | `root.TAG_INFO.measurement_name` | `root.TAG_INFO.tag_name` | `root.TAG_INFO.tag_order` |
   | :------------------------------- | :------------------------------- | :----------------------- | :------------------------ |
   | testdatabase| student             | name                     | 0                         |
   | testdatabase| student             | phone                    | 1                         |
   | testdatabase| student             | sex                      | 2                         |
   | testdatabase| student             | address                  | 3                         |

b. 查询数据

   1. 查询student中phone=B的数据。在testdatabase_student中phone的顺序为1，order最大值是3，对应到IoTDB的查询为：select * from root.testdatabase.student.*.B
   2. 查询student中phone=B且存储的socre>97的数据，对应到IoTDB的查询为：select * from root.testdatabase.student.*.B where socre>98
   3. 查询student中phone=B且存储的socre>97且时间在最近七天内的的数据，对应到IoTDB的查询为：select * from root.testdatabase.student.*.B where socre>98 and time > now()-7d

## 4.方案二

### 4.1关系映射

#### 4.11influxdb：

1. database
2. measurement
3. tag key tag value.  field key field value

#### 4.1.2IoTDB

1. storage group
2. device
3. timeseries

我们将上面对应的三点一一对应，举例如下

### 4.2举例

#### 4.2.1写入

influxdb的写入语句分别为（默认database=testdata）

1. insert cpu,host=serverA,region=us value=0.64（tag为host、region）
2. insert cpu,host=serverA,region=us,sex=n value=0.64（tag为host、region、sex）

最终写入情况为：

![influxdb-write](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/influxdb-write.png?raw=true)

IoTDB的写入数据为

1. insert into root.testdata.cpu(timestamp,host,region,value) values(now(),"serverA","us",0.64)
2. insert into root.testdata.cpu(timestamp,host,region,sex,value) values(now(),"serverA","us","n",0.64)

最终写入情况为：

![iotdb-write](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/iotdb-write.png?raw=true)

我们可以发现，二者实际存储对表面形式也比较类似。

#### 4.2.2查询

当把influxdb的tag和field都当作timeseries写入数据中时，由于存储的表面形式比较类似，最终查询就比较方便了。

1. 根据值查询

   select * from root.testdata.cpu where value = 0.64

2. 根据tag查询

   select * from. root.testdata.cpu where region = "us"

3. 根据time查询

   select * from root.testdata.cpu where time>now()-7d

4. 混合查询

   由于把tag和filed都当作timeseries查询，混合查询也同理。

5. group by

   influxdb中group by可以按照tag分组展示

   ![influxdb-group-by](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/influxdb-group-by.png?raw=true)

   inflxudb中的group by功能不太类似，因此实现方法是：先获取所有的数据，然后在列表中以hash的方式手动分组，理论上复杂度为O（n），复杂度可以接受。

## 5.方案对比-性能测试

### 5.1存储情况

1. influxdb的tag和filed均当作IoTDB的timeseries

![influxdb-test1](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/influxdb-test1.png?raw=true)

2. influxdb的tag当作路径

![influxdb-test2](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/influxdb-test2.png?raw=true)

### 5.2查询情况

1. 查找条件为select * from root.teststress.test1 where RL = 1 and A = 1 and B =1 and C=1

   其中RL是tag，A，B，C均为field

2. 取中间值情况，即当有十个tag时，取中间第五个累积递增

   ```java
           SessionDataSet dataSet = session.executeQueryStatement("select * from root.teststress.test2.*.*.*.*.SL where A=1 and B=1 and C=1");
           dataSet = session.executeQueryStatement("select * from root.teststress.test2.*.*.*.*.SL.* where A=1 and B=1 and C=1");
           dataSet = session.executeQueryStatement("select * from root.teststress.test2.*.*.*.*.SL.*.*.* where A=1 and B=1 and C=1");
           dataSet = session.executeQueryStatement("select * from root.teststress.test2.*.*.*.*.SL.*.*.*.* where A=1 and B=1 and C=1");
           dataSet = session.executeQueryStatement("select * from root.teststress.test2.*.*.*.*.SL.*.*.*.*.* where A=1 and B=1 ");
   ```

### 5.3特殊说明

1. 为了存储的方便，第二种情况的存储，没有把tag的value存入路径中，即直接把tag的key存入路径中。其表现为![influxdb-test-result](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/influxdb-test-result.png?raw=true)

   前面累积的path都是相同的，这样最后会导致的结果是：会加快根据path过滤的查找，如：

   ```sql
   select * from root.teststress.test2.*.*.*.*.SL.*.*.*.*.*
   ```

   这时只有一条路径，所有速度会变快，即第二种查找的时间会比实际的快一些

### 5.4测试结果

第一种查找的时间平均可以达到1000多ms，第二种查找的时间在300ms附近

同时如果第二种查找的时候，在较靠前的路径使用*，(select * from root.teststress.test2.* where A=1 and B=1 )会导致需要查找的path过多，报错信息如下

​```log
Too many paths in one query! Currently allowed max deduplicated path number is 715, this query contains 1000 deduplicated path. Please use slimit to choose what you real want or adjust max_deduplicated_path_num in iotdb-engine.properties.
​```

综上所述，建议采取第二种方案（把tag放在path中存储）(同时需要解决上述提到的问题:Too many paths in one query)。

## 6.查询优化
为了尽可能的降低session查询的次数，应将一些可以合并的过滤条件进行合并，从而减少查询的次数。

同时需要注意的是，由于tag过滤是通过path进行过滤，而filed过滤是通过IoTDB的where值过滤，因此二者的**or**过滤无法合并成一次过滤。即where tag1=value1 or field1=value2这种情况，无法合并成一次过滤。因此下文中尝试解决就是把可以合并的情况进行合并，即将**and**过滤进行合并。为了减少查询的次数，该算法使用递归的方法，对子树进行判断并尝试合并。

举例如下：

​```sql
select * from cpu where (host = 'serverA' and regions='us') or (regions = 'us' and value=0.77)
​```

InfluxDB语法解析器生成的语法树如下图所示：

![influxdb-syntax-tree](https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/API/IoTDB-InfluxDB/influxdb-syntax-tree.png?raw=true)
此时我们由两种解决思路：

1. 分别进行四次查询，host=serverA ；regions=us;regions=us和value=0.77四次查询，然后分别按照树的token，即or或者and进行合并。
2. 将and的子树进行合并，即查询host=serverA and regions=us ；regions=us and value=0.77.只需要进行两次查询。也就是本文提到的查询优化算法。

当然，如果根节点是and，同时左右子树分别对应的两个操作也是and，那么最终会合并成**一次**查询。

## 7. Influxdb所有函数列表

### 1. COUNT（）

返回非空字段值的数目。

#### 语法描述

​```
COUNT(field_key)
​```

返回field key对应的field values的数目。

​```
COUNT(/regular_expression/)
​```

返回匹配正则表达式的field key对应的field values的数目。

​```
COUNT(*)
​```

返回measurement中的每个field key对应的field value的数目。


### 2. DISTINCE（）

返回field value的不同值列表。

### 语法描述

​```
DISTINCT(field_key)
​```

返回field key对应的不同field values。

​```
DISTINCT(/regular_expression/)
​```

返回匹配正则表达式的field key对应的不同field values。

​```
DISTINCT(*)
​```

返回measurement中的每个field key对应的不同field value。

`DISTINCT()`支持所有数据类型的field value，InfluxQL支持`COUNT()`嵌套`DISTINCT()`。


### 3.INTEGRAL()

返回字段曲线下的面积，即是积分。

#### 语法描述

InfluxDB计算字段曲线下的面积，并将这些结果转换为每`unit`的总和面积。`unit`参数是一个整数，后跟一个时间字符串，它是可选的。如果查询未指定单位，则单位默认为1秒（`1s`）。

​```
INTEGRAL(field_key)
​```

返回field key关联的值之下的面积。

​```
INTEGRAL(/regular_expression/)
​```

返回满足正则表达式的每个field key关联的值之下的面积。

​```
INTEGRAL(*)
​```

返回measurement中每个field key关联的值之下的面积。

`INTEGRAL()`不支持`fill()`，`INTEGRAL()`支持int64和float64两个数据类型。


### 4.MEAN（）

返回字段的平均值

#### 语法描述

​```
MEAN(field_key)
​```

返回field key关联的值的平均值。

​```
MEAN(/regular_expression/)
​```

返回满足正则表达式的每个field key关联的值的平均值。

​```
MEAN(*)
​```

返回measurement中每个field key关联的值的平均值。

`MEAN()`支持int64和float64两个数据类型。


### 5.MEDIAN（）

在已经生成的QueryResult中找到满足条件的column，遍历获取不同值的列表

#### 语法描述

​```
MEDIAN(field_key)
​```

返回field key关联的值的中位数。

​```
MEDIAN(/regular_expression/)
​```

返回满足正则表达式的每个field key关联的值的中位数。

​```
MEDIAN(*)
​```

返回measurement中每个field key关联的值的中位数。

`MEDIAN()`支持int64和float64两个数据类型。

> 注意：`MEDIAN()`近似于`PERCENTILE（field_key，50）`，除了如果该字段包含偶数个值，`MEDIAN()`返回两个中间字段值的平均值之外。


### 6. MODE（）

返回字段中出现频率最高的值。

#### 语法描述

​```
MODE(field_key)
​```

返回field key关联的值的出现频率最高的值。

​```
MODE(/regular_expression/)
​```

返回满足正则表达式的每个field key关联的值的出现频率最高的值。

​```
MODE(*)
​```

返回measurement中每个field key关联的值的出现频率最高的值。

`MODE()`支持所有数据类型。

> 注意：`MODE()`如果最多出现次数有两个或多个值，则返回具有最早时间戳的字段值。

### 7.SPREAD()

返回字段中最大和最小值的差值。

#### 语法描述

​```
SPREAD(field_key)
​```

返回field key最大和最小值的差值。

​```
SPREAD(/regular_expression/)
​```

返回满足正则表达式的每个field key最大和最小值的差值。

​```
SPREAD(*)
​```

返回measurement中每个field key最大和最小值的差值。

`SPREAD()`支持所有的数值类型的field。


### 8.STDDEV()

返回字段的标准差。

#### 语法描述

​```
STDDEV(field_key)
​```

返回field key的标准差。

​```
STDDEV(/regular_expression/)
​```

返回满足正则表达式的每个field key的标准差。

​```
STDDEV(*)
​```

返回measurement中每个field key的标准差。

`STDDEV()`支持所有的数值类型的field。


### 9.SUM（）

返回字段值的和。

#### 语法描述

​```
SUM(field_key)
​```

返回field key的值的和。

​```
SUM(/regular_expression/)
​```

返回满足正则表达式的每个field key的值的和。

​```
SUM(*)
​```

返回measurement中每个field key的值的和。

`SUM()`支持所有的数值类型的field。


### 10.BOTTOM()

返回最小的N个field值。

#### 语法描述

​```
BOTTOM(field_key,N)
​```

返回field key的最小的N个field value。

​```
BOTTOM(field_key,tag_key(s),N)
​```

返回某个tag key的N个tag value的最小的field value。

​```
BOTTOM(field_key,N),tag_key(s),field_key(s)
​```

返回括号里的字段的最小N个field value，以及相关的tag或field，或者两者都有。

`BOTTOM()`支持所有的数值类型的field。

> 说明：
>
> - 如果一个field有两个或多个相等的field value，`BOTTOM()`返回时间戳最早的那个。
> - `BOTTOM()`和`INTO`子句一起使用的时候，和其他的函数有些不一样。


### 11.FIRST()

返回时间戳最早的值

#### 语法描述

​```
FIRST(field_key)
​```

返回field key时间戳最早的值。

​```
FIRST(/regular_expression/)
​```

返回满足正则表达式的每个field key的时间戳最早的值。

​```
FIRST(*)
​```

返回measurement中每个field key的时间戳最早的值。

​```
FIRST(field_key),tag_key(s),field_key(s)
​```

返回括号里的字段的时间戳最早的值，以及相关联的tag或field，或者两者都有。

`FIRST()`支持所有类型的field。


### 12.LAST()

返回时间戳最近的值

#### 语法描述

​```
LAST(field_key)
​```

返回field key时间戳最近的值。

​```
LAST(/regular_expression/)
​```

返回满足正则表达式的每个field key的时间戳最近的值。

​```
LAST(*)
​```

返回measurement中每个field key的时间戳最近的值。

​```
LAST(field_key),tag_key(s),field_key(s)
​```

返回括号里的字段的时间戳最近的值，以及相关联的tag或field，或者两者都有。

`LAST()`支持所有类型的field。


### 13.MAX()

返回最大的字段值

#### 语法描述

​```
MAX(field_key)
​```

返回field key的最大值。

​```
MAX(/regular_expression/)
​```

返回满足正则表达式的每个field key的最大值。

​```
MAX(*)
​```

返回measurement中每个field key的最大值。

​```
MAX(field_key),tag_key(s),field_key(s)
​```

返回括号里的字段的最大值，以及相关联的tag或field，或者两者都有。

`MAX()`支持所有数值类型的field。


### 14.MIN()

返回最小的字段值

#### 语法描述

​```
MIN(field_key)
​```

返回field key的最小值。

​```
MIN(/regular_expression/)
​```

返回满足正则表达式的每个field key的最小值。

​```
MIN(*)
​```

返回measurement中每个field key的最小值。

​```
MIN(field_key),tag_key(s),field_key(s)
​```

返回括号里的字段的最小值，以及相关联的tag或field，或者两者都有。

`MIN()`支持所有数值类型的field。


### 15.PERCENTILE()

返回较大百分之N的字段值

#### 语法描述

​```
PERCENTILE(field_key,N)
​```

返回field key较大的百分之N的值。

​```
PERCENTILE(/regular_expression/,N)
​```

返回满足正则表达式的每个field key较大的百分之N的值。

​```
PERCENTILE(*,N)
​```

返回measurement中每个field key较大的百分之N的值。

​```
PERCENTILE(field_key,N),tag_key(s),field_key(s)
​```

返回括号里的字段较大的百分之N的值，以及相关联的tag或field，或者两者都有。

`N`必须是0到100的整数或者浮点数。

`PERCENTILE()`支持所有数值类型的field。


### 16.SAMPLE()

返回`N`个随机抽样的字段值。`SAMPLE()`使用reservoir sampling来生成随机点。

#### 语法描述

​```
SAMPLE(field_key,N)
​```

返回field key的N个随机抽样的字段值。

​```
SAMPLE(/regular_expression/,N)
​```

返回满足正则表达式的每个field key的N个随机抽样的字段值。

​```
SAMPLE(*,N)
​```

返回measurement中每个field key的N个随机抽样的字段值。

​```
SAMPLE(field_key,N),tag_key(s),field_key(s)
​```

返回括号里的字段的N个随机抽样的字段值，以及相关联的tag或field，或者两者都有。

`N`必须是整数。

`SAMPLE()`支持所有类型的field。


### 17.TOP()

返回最大的N个field值。

#### 语法描述

​```
TOP(field_key,N)
​```

返回field key的最大的N个field value。

​```
TOP(field_key,tag_key(s),N)
​```

返回某个tag key的N个tag value的最大的field value。

​```
TOP(field_key,N),tag_key(s),field_key(s)
​```

返回括号里的字段的最大N个field value，以及相关的tag或field，或者两者都有。

`TOP()`支持所有的数值类型的field。

> 说明：
>
> - 如果一个field有两个或多个相等的field value，`TOP()`返回时间戳最早的那个。
> - `TOP()`和`INTO`子句一起使用的时候，和其他的函数有些不一样。


## 8. 支持的函数

1. first()
先利用IoTDB中的first_value函数，然后对每一个device中的first值再重回IoTDB中查询，指定path和where限定条件，查询出对应的time
4. last()
先利用IoTDB中的last_value函数，然后对每一个device中的last值再重回IoTDB中查询，指定path和where限定条件，查询出对应的time
5. max()
通过利用IoTDB中的max_value函数，对多个device对最大值再求最大值，找到最大值后，指定path和where限定条件，再重回IoTDB中查询对应的time
6. min()
通过利用IoTDB中的min_value函数，对多个device对最小值再求最小值，找到最小值后，指定path和where限定条件，再重回IoTDB中查询对应的time
7. count()
通过利用IoTDB中的count函数,将多个设备的count值求和
8. sum()
通过利用IoTDB中的sum函数，对多个device的求和值再进行求和
9. mean()
通过利用IoTDB中的avg和count函数，对多个device的数量乘以avg之和再除以总数量
10. spread()
通过利用IoTDB中的max_value和min_value函数，对多个device查找最大值和最小值，进行求和

## 9.切换方案

 - 原版本
​```java
influxDB = InfluxDBFactory.connect(openurl, username, password);
influxDB.createDatabase(database);
influxDB.insert(ponit);
influxDB.query(query);
​```

- 迁移版本
​```java
influxDB = IotDBInfluxDBFactory.connect(openurl, username, password);
influxDB.createDatabase(database);
influxDB.insert(ponit);
influxDB.query(query);
​```        

只需要把
InfluxDBFactory.connect(openurl, username, password);

改为
**IotDBInfluxDBFactory**.connect(openurl, username, password);

即可

## 10.参考资料

1. https://summer.iscas.ac.cn/#/org/orgdetail/apacheiotdb
2. https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=165224300#Prometheus%E8%BF%9E%E6%8E%A5%E5%99%A8-3.4%E5%8F%82%E8%80%83%E6%96%87%E6%A1%A3

