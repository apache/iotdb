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

# 运算符和函数

本章介绍 IoTDB 支持的运算符和函数。IoTDB 提供了丰富的内置运算符和函数来满足您的计算需求，同时支持通过[用户自定义函数](./User-Defined-Function.md)能力进行扩展。

可以使用 `SHOW FUNCTIONS` 显示所有可用函数的列表，包括内置函数和自定义函数。

关于运算符和函数在 SQL 中的行为，可以查看文档 [选择表达式](../Query-Data/Select-Expression.md)。

## 运算符列表

### 算数运算符
|运算符                       |含义|
|----------------------------|-----------|
|`+`                         |取正（单目）|
|`-`                         |取负（单目）|
|`*`                         |乘|
|`/`                         |除|
|`%`                         |取余|
|`+`                         |加|
|`-`                         |减|

详细说明及示例见文档 [算数运算符和函数](./Mathematical.md)。

### 比较运算符
|运算符                       |含义|
|----------------------------|-----------|
|`>`                         |大于|
|`>=`                        |大于等于|
|`<`                         |小于|
|`<=`                        |小于等于|
|`==`                        |等于|
|`!=` / `<>`                 |不等于|
|`BETWEEN ... AND ...`       |在指定范围内|
|`NOT BETWEEN ... AND ...`   |不在指定范围内|
|`LIKE`                      |匹配简单模式|
|`NOT LIKE`                  |无法匹配简单模式|
|`REGEXP`                    |匹配正则表达式|
|`NOT REGEXP`                |无法匹配正则表达式|
|`IS NULL`                   |是空值|
|`IS NOT NULL`               |不是空值|
|`IN` / `CONTAINS`           |是指定列表中的值|
|`NOT IN` / `NOT CONTAINS`   |不是指定列表中的值|

详细说明及示例见文档 [比较运算符和函数](./Comparison.md)。

### 逻辑运算符
|运算符                       |含义|
|----------------------------|-----------|
|`NOT` / `!`                 |取非（单目）|
|`AND` / `&` / `&&`          |逻辑与|
|`OR`/ &#124; / &#124;&#124; |逻辑或|
<!--- &#124;即管道符 转义不能用在``里, 表格内不允许使用管道符 -->

详细说明及示例见文档 [逻辑运算符](./Logical.md)。

### 运算符优先级

运算符的优先级从高到低如下所示排列，同一行的运算符具有相同的优先级。
```sql
!, - (单目), + (单目)
*, /, DIV, %, MOD
-, +
=, ==, <=>, >=, >, <=, <, <>, !=
LIKE, REGEXP, NOT LIKE, NOT REGEXP
BETWEEN ... AND ..., NOT BETWEEN ... AND ...
IS NULL, IS NOT NULL
IN, CONTAINS, NOT IN, NOT CONTAINS
AND, &, &&
OR, |, ||
```

## 内置函数列表

### 聚合函数

| 函数名      | 功能描述                                                     | 允许的输入类型           | 输出类型       |
| ----------- | ------------------------------------------------------------ | ------------------------ | -------------- |
| SUM         | 求和。                                                       | INT32 INT64 FLOAT DOUBLE | DOUBLE         |
| COUNT       | 计算数据点数。                                               | 所有类型                 | INT            |
| AVG         | 求平均值。                                                   | INT32 INT64 FLOAT DOUBLE | DOUBLE         |
| EXTREME     | 求具有最大绝对值的值。如果正值和负值的最大绝对值相等，则返回正值。 | INT32 INT64 FLOAT DOUBLE | 与输入类型一致 |
| MAX_VALUE   | 求最大值。                                                   | INT32 INT64 FLOAT DOUBLE | 与输入类型一致 |
| MIN_VALUE   | 求最小值。                                                   | INT32 INT64 FLOAT DOUBLE | 与输入类型一致 |
| FIRST_VALUE | 求时间戳最小的值。                                           | 所有类型                 | 与输入类型一致 |
| LAST_VALUE  | 求时间戳最大的值。                                           | 所有类型                 | 与输入类型一致 |
| MAX_TIME    | 求最大时间戳。                                               | 所有类型                 | Timestamp      |
| MIN_TIME    | 求最小时间戳。                                               | 所有类型                 | Timestamp      |
详细说明及示例见文档 [聚合函数](./Aggregation.md)。

### 数学函数 

| 函数名  | 输入序列类型                   | 输出序列类型             | Java 标准库中的对应实现                                      |
| ------- | ------------------------------ | ------------------------ | ------------------------------------------------------------ |
| SIN     | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#sin(double)                                             |
| COS     | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#cos(double)                                             |
| TAN     | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#tan(double)                                             |
| ASIN    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#asin(double)                                            |
| ACOS    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#acos(double)                                            |
| ATAN    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#atan(double)                                            |
| SINH    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#sinh(double)                                            |
| COSH    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#cosh(double)                                            |
| TANH    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#tanh(double)                                            |
| DEGREES | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#toDegrees(double)                                       |
| RADIANS | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#toRadians(double)                                       |
| ABS     | INT32 / INT64 / FLOAT / DOUBLE | 与输入序列的实际类型一致 | Math#abs(int) / Math#abs(long) /Math#abs(float) /Math#abs(double) |
| SIGN    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#signum(double)                                          |
| CEIL    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#ceil(double)                                            |
| FLOOR   | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#floor(double)                                           |
| ROUND   | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#rint(double)                                            |
| EXP     | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#exp(double)                                             |
| LN      | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#log(double)                                             |
| LOG10   | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#log10(double)                                           |
| SQRT    | INT32 / INT64 / FLOAT / DOUBLE | DOUBLE                   | Math#sqrt(double)                                            |


详细说明及示例见文档 [算数运算符和函数](./Mathematical.md)。

### 比较函数

| 函数名    | 可接收的输入序列类型                | 必要的属性参数                               | 输出序列类型     | 功能类型                                             |
|----------|--------------------------------|---------------------------------------|------------|--------------------------------------------------|
| ON_OFF   | INT32 / INT64 / FLOAT / DOUBLE | `threshold`:DOUBLE                  | BOOLEAN | 返回`ts_value >= threshold`的bool值                  |
| IN_RANGE | INT32 / INT64 / FLOAT / DOUBLE | `lower`:DOUBLE<br/>`upper`:DOUBLE | BOOLEAN | 返回`ts_value >= lower && ts_value <= upper`的bool值 |                                                    |

详细说明及示例见文档 [比较运算符和函数](./Comparison.md)。

### 字符串处理函数

| 函数名          | 输入序列类型 | 必要的属性参数                       | 输出序列类型 | 功能描述                                  |
| --------------- | ------------ | ------------------------------------ | ------------ | ----------------------------------------- |
| STRING_CONTAINS | TEXT         | `s`: 待搜寻的字符串                  | BOOLEAN      | 判断字符串中是否存在`s`                   |
| STRING_MATCHES  | TEXT         | `regex`: Java 标准库风格的正则表达式 | BOOLEAN      | 判断字符串是否能够被正则表达式`regex`匹配 |
| LENGTH | TEXT | 无 | INT32 | 返回字符串的长度 |
| LOCATE | TEXT | `target`: 需要被定位的子串 <br/> `reverse`: 指定是否需要倒序定位，默认值为`false`, 即从左至右定位 | INT32 | 获取`target`子串第一次出现在输入序列的位置，如果输入序列中不包含`target`则返回 -1 |
| STARTSWITH | TEXT | `target`: 需要匹配的前缀 | BOOLEAN | 判断字符串是否有指定前缀 |
| ENDSWITH | TEXT | `target`: 需要匹配的后缀 | BOOLEAN | 判断字符串是否有指定后缀 |
| CONCAT | TEXT | `targets`: 一系列 K-V, key需要以`target`为前缀且不重复, value是待拼接的字符串。<br/>`series_behind`: 指定拼接时时间序列是否在后面，默认为`false`。 | TEXT | 拼接字符串和`target`字串 |
| SUBSTR | TEXT | `start`: 指定子串开始下标 <br/>`end`: 指定子串结束下标  | TEXT | 获取下标从`start`到`end - 1`的子串 |
| UPPER | TEXT | 无 | TEXT | 将字符串转化为大写 |
| LOWER | TEXT | 无 | TEXT | 将字符串转化为小写 |
| TRIM | TEXT | 无 | TEXT | 移除字符串前后的空格 |
| STRCMP | TEXT | 无 | TEXT | 用于比较两个输入序列，如果值相同返回 `0` , 序列1的值小于序列2的值返回一个`负数`，序列1的值大于序列2的值返回一个`正数` |
| STRREPLACE | TEXT | `target`: 需要替换的字符子串 <br/> `replace`: 替换后的字符串 <br/> `limit`: 替换次数，大于等于 -1 的整数，默认为 -1 表示所有匹配的子串都会被替换 <br/>`offset`: 需要跳过的匹配次数，即前`offset`次匹配到的字符子串并不会被替换，默认为 0 <br/>`reverse`: 是否需要反向计数，默认为 false 即按照从左向右的次序 | TEXT | 将字符串中的子串替换为指定的字符串|
| REGEXMATCH | TEXT | `regex`: Java 标准库风格的正则表达式 <br/> `group`: 输出的匹配组序号，根据 java.util.regex 规定，第 0 组为整个正则表达式，此后的组按照左括号出现的顺序依次编号 | TEXT | 用于正则表达式匹配文本中的具体内容并返回 |
| REGEXREPLACE | TEXT | `regex`: Java 标准库风格的正则表达式 <br/> `replace`: 替换后的字符串，支持 Java 正则表达式中的后向引用 <br/> `limit`: 替换次数，大于等于 -1 的整数，默认为 -1 表示所有匹配的子串都会被替换 <br/> `offset`: 需要跳过的匹配次数，即前`offset`次匹配到的字符子串并不会被替换，默认为 0 <br/> `reverse`: 是否需要反向计数，默认为 false 即按照从左向右的次序 | TEXT | 用于将文本中符合正则表达式的匹配结果替换为指定的字符串 |
| REGEXSPLIT | TEXT | `regex`: Java 标准库风格的正则表达式 <br/> `index`: 输出结果在切分后数组中的序号 | TEXT | 用于使用给定的正则表达式切分文本，并返回指定的项 |

详细说明及示例见文档 [字符串处理函数](./String.md)。

### 数据类型转换函数

| 函数名 | 必要的属性参数                                               | 输出序列类型             | 功能类型                           |
| ------ | ------------------------------------------------------------ | ------------------------ | ---------------------------------- |
| CAST   | `type`:输出的数据点的类型，只能是 INT32 / INT64 / FLOAT / DOUBLE / BOOLEAN / TEXT | 由输入属性参数`type`决定 | 将数据转换为`type`参数指定的类型。 |

详细说明及示例见文档 [数据类型转换](./Conversion.md)。

### 常序列生成函数

| 函数名 | 必要的属性参数                                               | 输出序列类型               | 功能描述                                                     |
| ------ | ------------------------------------------------------------ | -------------------------- | ------------------------------------------------------------ |
| CONST  | `value`: 输出的数据点的值 <br />`type`: 输出的数据点的类型，只能是 INT32 / INT64 / FLOAT / DOUBLE / BOOLEAN / TEXT | 由输入属性参数 `type` 决定 | 根据输入属性 `value` 和 `type` 输出用户指定的常序列。        |
| PI     | 无                                                           | DOUBLE                     | 常序列的值：`π` 的 `double` 值，圆的周长与其直径的比值，即圆周率，等于 *Java标准库* 中的`Math.PI`。 |
| E      | 无                                                           | DOUBLE                     | 常序列的值：`e` 的 `double` 值，自然对数的底，它等于 *Java 标准库*  中的 `Math.E`。 |

详细说明及示例见文档 [常序列生成函数](./Constant.md)。

### 选择函数

| 函数名   | 输入序列类型                          | 必要的属性参数                                    | 输出序列类型             | 功能描述                                                     |
| -------- | ------------------------------------- | ------------------------------------------------- | ------------------------ | ------------------------------------------------------------ |
| TOP_K    | INT32 / INT64 / FLOAT / DOUBLE / TEXT | `k`: 最多选择的数据点数，必须大于 0 小于等于 1000 | 与输入序列的实际类型一致 | 返回某时间序列中值最大的`k`个数据点。若多于`k`个数据点的值并列最大，则返回时间戳最小的数据点。 |
| BOTTOM_K | INT32 / INT64 / FLOAT / DOUBLE / TEXT | `k`: 最多选择的数据点数，必须大于 0 小于等于 1000 | 与输入序列的实际类型一致 | 返回某时间序列中值最小的`k`个数据点。若多于`k`个数据点的值并列最小，则返回时间戳最小的数据点。 |

详细说明及示例见文档 [选择函数](./Selection.md)。

### 区间查询函数

| 函数名               | 输入序列类型                               | 属性参数                                           | 输出序列类型 | 功能描述                                                             |
|-------------------|--------------------------------------|------------------------------------------------|-------|------------------------------------------------------------------|
| ZERO_DURATION     | INT32/ INT64/ FLOAT/ DOUBLE/ BOOLEAN | `min`:可选，默认值0<br>`max`:可选，默认值`Long.MAX_VALUE` | Long  | 返回时间序列连续为0(false)的开始时间与持续时间，持续时间t(单位ms)满足`t >= min && t <= max`  |
| NON_ZERO_DURATION | INT32/ INT64/ FLOAT/ DOUBLE/ BOOLEAN | `min`:可选，默认值0<br>`max`:可选，默认值`Long.MAX_VALUE` | Long  | 返回时间序列连续不为0(false)的开始时间与持续时间，持续时间t(单位ms)满足`t >= min && t <= max` |               |
| ZERO_COUNT        | INT32/ INT64/ FLOAT/ DOUBLE/ BOOLEAN | `min`:可选，默认值1<br>`max`:可选，默认值`Long.MAX_VALUE` | Long  | 返回时间序列连续为0(false)的开始时间与其后数据点的个数，数据点个数n满足`n >= min && n <= max`   |               |
| NON_ZERO_COUNT    | INT32/ INT64/ FLOAT/ DOUBLE/ BOOLEAN | `min`:可选，默认值1<br>`max`:可选，默认值`Long.MAX_VALUE` | Long  | 返回时间序列连续不为0(false)的开始时间与其后数据点的个数，数据点个数n满足`n >= min && n <= max`  |               |

详细说明及示例见文档 [区间查询函数](./Continuous-Interval.md)。

### 趋势计算函数

| 函数名                  | 输入序列类型                                    | 输出序列类型             | 功能描述                                                     |
| ----------------------- | ----------------------------------------------- | ------------------------ | ------------------------------------------------------------ |
| TIME_DIFFERENCE         | INT32 / INT64 / FLOAT / DOUBLE / BOOLEAN / TEXT | INT64                    | 统计序列中某数据点的时间戳与前一数据点时间戳的差。范围内第一个数据点没有对应的结果输出。 |
| DIFFERENCE              | INT32 / INT64 / FLOAT / DOUBLE                  | 与输入序列的实际类型一致 | 统计序列中某数据点的值与前一数据点的值的差。范围内第一个数据点没有对应的结果输出。 |
| NON_NEGATIVE_DIFFERENCE | INT32 / INT64 / FLOAT / DOUBLE                  | 与输入序列的实际类型一致 | 统计序列中某数据点的值与前一数据点的值的差的绝对值。范围内第一个数据点没有对应的结果输出。 |
| DERIVATIVE              | INT32 / INT64 / FLOAT / DOUBLE                  | DOUBLE                   | 统计序列中某数据点相对于前一数据点的变化率，数量上等同于 DIFFERENCE /  TIME_DIFFERENCE。范围内第一个数据点没有对应的结果输出。 |
| NON_NEGATIVE_DERIVATIVE | INT32 / INT64 / FLOAT / DOUBLE                  | DOUBLE                   | 统计序列中某数据点相对于前一数据点的变化率的绝对值，数量上等同于 NON_NEGATIVE_DIFFERENCE /  TIME_DIFFERENCE。范围内第一个数据点没有对应的结果输出。 |


| 函数名  | 输入序列类型                         | 参数                                                                                                                     | 输出序列类型 | 功能描述                                           |
|------|--------------------------------|------------------------------------------------------------------------------------------------------------------------|--------|------------------------------------------------|
| DIFF | INT32 / INT64 / FLOAT / DOUBLE | `ignoreNull`：可选，默认为true；为true时，前一个数据点值为null时，忽略该数据点继续向前找到第一个出现的不为null的值；为false时，如果前一个数据点为null，则不忽略，使用null进行相减，结果也为null | DOUBLE | 统计序列中某数据点的值与前一数据点的值的差。第一个数据点没有对应的结果输出，输出值为null |

详细说明及示例见文档 [趋势计算函数](./Variation-Trend.md)。

### 采样函数

| 函数名      | 可接收的输入序列类型                     | 必要的属性参数                               | 输出序列类型     | 功能类型                                             |
|----------|--------------------------------|---------------------------------------|------------|--------------------------------------------------|
| EQUAL_SIZE_BUCKET_RANDOM_SAMPLE   | INT32 / INT64 / FLOAT / DOUBLE | 降采样比例 `proportion`，取值范围为`(0, 1]`，默认为`0.1`  | INT32 / INT64 / FLOAT / DOUBLE | 返回符合采样比例的等分桶随机采样                |
| EQUAL_SIZE_BUCKET_AGG_SAMPLE   | INT32 / INT64 / FLOAT / DOUBLE | `proportion`取值范围为`(0, 1]`，默认为`0.1`<br>`type`:取值类型有`avg`, `max`, `min`, `sum`, `extreme`, `variance`, 默认为`avg`  | INT32 / INT64 / FLOAT / DOUBLE | 返回符合采样比例的等分桶聚合采样                |
| EQUAL_SIZE_BUCKET_M4_SAMPLE   | INT32 / INT64 / FLOAT / DOUBLE | `proportion`取值范围为`(0, 1]`，默认为`0.1`| INT32 / INT64 / FLOAT / DOUBLE | 返回符合采样比例的等分桶M4采样                |
| EQUAL_SIZE_BUCKET_OUTLIER_SAMPLE   | INT32 / INT64 / FLOAT / DOUBLE | `proportion`取值范围为`(0, 1]`，默认为`0.1`<br>`type`取值为`avg`或`stendis`或`cos`或`prenextdis`，默认为`avg`<br>`number`取值应大于0，默认`3`| INT32 / INT64 / FLOAT / DOUBLE | 返回符合采样比例和桶内采样个数的等分桶离群值采样                |
| M4     | INT32 / INT64 / FLOAT / DOUBLE | 包含固定点数的窗口和滑动时间窗口使用不同的属性参数。包含固定点数的窗口使用属性`windowSize`和`slidingStep`。滑动时间窗口使用属性`timeInterval`、`slidingStep`、`displayWindowBegin`和`displayWindowEnd`。更多细节见下文。 | INT32 / INT64 / FLOAT / DOUBLE | 返回每个窗口内的第一个点（`first`）、最后一个点（`last`）、最小值点（`bottom`）、最大值点（`top`）。在一个窗口内的聚合点输出之前，M4会将它们按照时间戳递增排序并且去重。 |

## 数据质量函数库

对基于时序数据的应用而言，数据质量至关重要。基于用户自定义函数能力，IoTDB 提供了一系列关于数据质量的函数，包括数据画像、数据质量评估与修复等，能够满足工业领域对数据质量的需求。

**该函数库中的函数不是内置函数，使用前要先加载到系统中。** 操作流程如下：
1. 下载包含全部依赖的 jar 包和注册脚本 [【点击下载】](https://archive.apache.org/dist/iotdb/0.14.0-preview3/apache-iotdb-0.14.0-preview3-library-udf-bin.zip) ；
2. 将 jar 包复制到 IoTDB 程序目录的 `ext\udf` 目录下 (若您使用的是集群，请将jar包复制到所有DataNode的该目录下)；
3. 启动 IoTDB；
4. 将注册脚本复制到 IoTDB 的程序目录下（与`sbin`目录同级的根目录下），修改脚本中的参数（如果需要）并运行注册脚本以注册 UDF。