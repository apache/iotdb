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
# RegexReplace

## 函数简介

本函数用于将文本中符合正则表达式的匹配结果替换为指定的字符串。

**函数名：** REGEXREPLACE

**输入序列：** 仅支持单个输入序列，类型为 TEXT。

**参数：**

+ `regex`: 需要替换的正则表达式，支持所有Java正则表达式语法。
+ `replace`: 替换后的字符串，支持Java正则表达式中的后向引用，
  形如'$1'指代了正则表达式`regex`中的第一个分组，并会在替换时自动填充匹配到的子串。
+ `limit`: 替换次数，大于等于-1的整数，默认为-1表示所有匹配的子串都会被替换。
+ `offset`: 需要跳过的匹配次数，即前`offset`次匹配到的字符子串并不会被替换，默认为0。
+ `reverse`: 是否需要反向计数，默认为false即按照从左向右的次序。

**输出序列：** 输出单个序列，类型为TEXT。

## 使用示例

输入序列：

```
+-----------------------------+-------------------------------+
|                         Time|                root.test.d1.s1|
+-----------------------------+-------------------------------+
|2021-01-01T00:00:01.000+08:00|        [192.168.0.1] [SUCCESS]|
|2021-01-01T00:00:02.000+08:00|       [192.168.0.24] [SUCCESS]|
|2021-01-01T00:00:03.000+08:00|           [192.168.0.2] [FAIL]|
|2021-01-01T00:00:04.000+08:00|        [192.168.0.5] [SUCCESS]|
|2021-01-01T00:00:05.000+08:00|      [192.168.0.124] [SUCCESS]|
+-----------------------------+-------------------------------+
```

用于查询的SQL语句：

```sql
select regexreplace(s1, "regex"="192\.168\.0\.(\d+)", "replace"="cluster-$1", "limit"="1") from root.test.d1
```

输出序列：

```
+-----------------------------+-----------------------------------------------------------+
|                         Time|regexreplace(root.test.d1.s1, "regex"="192\.168\.0\.(\d+)",|
|                             |                       "replace"="cluster-$1", "limit"="1")|
+-----------------------------+-----------------------------------------------------------+
|2021-01-01T00:00:01.000+08:00|                                      [cluster-1] [SUCCESS]|
|2021-01-01T00:00:02.000+08:00|                                     [cluster-24] [SUCCESS]|
|2021-01-01T00:00:03.000+08:00|                                         [cluster-2] [FAIL]|
|2021-01-01T00:00:04.000+08:00|                                      [cluster-5] [SUCCESS]|
|2021-01-01T00:00:05.000+08:00|                                    [cluster-124] [SUCCESS]|
+-----------------------------+-----------------------------------------------------------+
```
