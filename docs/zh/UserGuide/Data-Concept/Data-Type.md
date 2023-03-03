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

## 数据类型

### 基本数据类型

IoTDB 支持以下六种数据类型：

* BOOLEAN（布尔值）
* INT32（整型）
* INT64（长整型）
* FLOAT（单精度浮点数）
* DOUBLE（双精度浮点数）
* TEXT（字符串）

#### 浮点数精度配置

对于 **FLOAT** 与 **DOUBLE** 类型的序列，如果编码方式采用 [RLE](Encoding.md) 或 [TS_2DIFF](Encoding.md) ，可以在创建序列时通过 `MAX_POINT_NUMBER` 属性指定浮点数的小数点后位数。

例如，
```sql
CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=FLOAT, ENCODING=RLE, 'MAX_POINT_NUMBER'='2';
```

若不指定，系统会按照配置文件 `iotdb-common.properties` 中的 [float_precision](../Reference/Common-Config-Manual.md) 项配置（默认为 2 位）。

#### 数据类型兼容性

当写入数据的类型与序列注册的数据类型不一致时，
- 如果序列数据类型不兼容写入数据类型，系统会给出错误提示。
- 如果序列数据类型兼容写入数据类型，系统会进行数据类型的自动转换，将写入的数据类型更正为注册序列的类型。

各数据类型的兼容情况如下表所示：

| 序列数据类型 | 支持的写入数据类型       |
|--------------|--------------------------|
| BOOLEAN      | BOOLEAN                  |
| INT32        | INT32                    |
| INT64        | INT32 INT64              |
| FLOAT        | INT32 FLOAT              |
| DOUBLE       | INT32 INT64 FLOAT DOUBLE |
| TEXT         | TEXT                     |

### 时间戳类型

时间戳是一个数据到来的时间点，其中包括绝对时间戳和相对时间戳。

#### 绝对时间戳

IOTDB 中绝对时间戳分为二种，一种为 LONG 类型，一种为 DATETIME 类型（包含 DATETIME-INPUT, DATETIME-DISPLAY 两个小类）。

在用户在输入时间戳时，可以使用 LONG 类型的时间戳或 DATETIME-INPUT 类型的时间戳，其中 DATETIME-INPUT 类型的时间戳支持格式如表所示：

<div style="text-align: center;">

**DATETIME-INPUT 类型支持格式**


| format                       |
| :--------------------------- |
| yyyy-MM-dd HH:mm:ss          |
| yyyy/MM/dd HH:mm:ss          |
| yyyy.MM.dd HH:mm:ss          |
| yyyy-MM-dd HH:mm:ssZZ        |
| yyyy/MM/dd HH:mm:ssZZ        |
| yyyy.MM.dd HH:mm:ssZZ        |
| yyyy/MM/dd HH:mm:ss.SSS      |
| yyyy-MM-dd HH:mm:ss.SSS      |
| yyyy.MM.dd HH:mm:ss.SSS      |
| yyyy-MM-dd HH:mm:ss.SSSZZ    |
| yyyy/MM/dd HH:mm:ss.SSSZZ    |
| yyyy.MM.dd HH:mm:ss.SSSZZ    |
| ISO8601 standard time format |


</div>


IoTDB 在显示时间戳时可以支持 LONG 类型以及 DATETIME-DISPLAY 类型，其中 DATETIME-DISPLAY 类型可以支持用户自定义时间格式。自定义时间格式的语法如表所示：

<div style="text-align: center;">

**DATETIME-DISPLAY 自定义时间格式的语法**


| Symbol |           Meaning           | Presentation |              Examples              |
| :----: | :-------------------------: | :----------: | :--------------------------------: |
|   G    |             era             |     era      |                era                 |
|   C    |    century of era (>=0)     |    number    |                 20                 |
|   Y    |      year of era (>=0)      |     year     |                1996                |
|        |                             |              |                                    |
|   x    |          weekyear           |     year     |                1996                |
|   w    |      week of weekyear       |    number    |                 27                 |
|   e    |         day of week         |    number    |                 2                  |
|   E    |         day of week         |     text     |            Tuesday; Tue            |
|        |                             |              |                                    |
|   y    |            year             |     year     |                1996                |
|   D    |         day of year         |    number    |                189                 |
|   M    |        month of year        |    month     |           July; Jul; 07            |
|   d    |        day of month         |    number    |                 10                 |
|        |                             |              |                                    |
|   a    |       halfday of day        |     text     |                 PM                 |
|   K    |   hour of halfday (0~11)    |    number    |                 0                  |
|   h    | clockhour of halfday (1~12) |    number    |                 12                 |
|        |                             |              |                                    |
|   H    |     hour of day (0~23)      |    number    |                 0                  |
|   k    |   clockhour of day (1~24)   |    number    |                 24                 |
|   m    |       minute of hour        |    number    |                 30                 |
|   s    |      second of minute       |    number    |                 55                 |
|   S    |     fraction of second      |    millis    |                978                 |
|        |                             |              |                                    |
|   z    |          time zone          |     text     |     Pacific Standard Time; PST     |
|   Z    |     time zone offset/id     |     zone     | -0800; -08:00; America/Los_Angeles |
|        |                             |              |                                    |
|   '    |       escape for text       |  delimiter   |                                    |
|   ''   |        single quote         |   literal    |                 '                  |

</div>

#### 相对时间戳

  相对时间是指与服务器时间```now()```和```DATETIME```类型时间相差一定时间间隔的时间。
  形式化定义为：

  ```
  Duration = (Digit+ ('Y'|'MO'|'W'|'D'|'H'|'M'|'S'|'MS'|'US'|'NS'))+
  RelativeTime = (now() | DATETIME) ((+|-) Duration)+
  ```

  <div style="text-align: center;">
  
  **The syntax of the duration unit**


  | Symbol |   Meaning   |       Presentation       | Examples |
  | :----: | :---------: | :----------------------: | :------: |
  |   y    |    year     |       1y=365 days        |    1y    |
  |   mo   |    month    |       1mo=30 days        |   1mo    |
  |   w    |    week     |        1w=7 days         |    1w    |
  |   d    |     day     |         1d=1 day         |    1d    |
  |        |             |                          |          |
  |   h    |    hour     |     1h=3600 seconds      |    1h    |
  |   m    |   minute    |      1m=60 seconds       |    1m    |
  |   s    |   second    |       1s=1 second        |    1s    |
  |        |             |                          |          |
  |   ms   | millisecond | 1ms=1000_000 nanoseconds |   1ms    |
  |   us   | microsecond |   1us=1000 nanoseconds   |   1us    |
  |   ns   | nanosecond  |     1ns=1 nanosecond     |   1ns    |

  </div>

  例子：

  ```
  now() - 1d2h //比服务器时间早 1 天 2 小时的时间
  now() - 1w //比服务器时间早 1 周的时间
  ```

  > 注意：'+'和'-'的左右两边必须有空格