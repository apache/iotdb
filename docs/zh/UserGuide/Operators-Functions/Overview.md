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



下表列出了 IoTDB 中支持的全部运算符和内置函数。

|运算符                       |含义|
|----------------------------|-----------|
|`-`                         |单目运算符负号|
|`+`                         |单目运算符正号|
|`NOT` / `!`                 |单目运算符取非|
|`*`                         |双目运算符乘|
|`/`                         |双目运算符除|
|`%`                         |双目运算符取余|
|`+`                         |双目运算符加|
|`-`                         |双目运算符减|
|`>`                         |双目比较运算符大于|
|`>=`                        |双目比较运算符大于等于|
|`<`                         |双目比较运算符小于|
|`<=`                        |双目比较运算符小于等于|
|`==`                        |双目比较运算符等于|
|`!=` / `<>`                 |双目比较运算符不等于|
|`LIKE`                      |`LIKE`运算符|
|`NOT LIKE`                  |`LIKE`运算符|
|`REGEXP`                    |`REGEXP`运算符|
|`NOT REGEXP`                |`REGEXP`运算符|
|`BETWEEN ... AND ...`       |`BETWEEN`运算符|
|`NOT BETWEEN ... AND ...`   |`NOT BETWEEN`运算符|
|`IS NULL`                   |`IS NULL`运算符|
|`IS NOT NULL`               |`IS NOT NULL`运算符|
|`IN` / `CONTAINS`           |`IN`运算符|
|`NOT IN` / `NOT CONTAINS`   |`NOT IN`运算符|
|`AND` / `&` / `&&`          |双目逻辑运算符与|
|`OR`/ &#124; / &#124;&#124; |双目逻辑运算符或|
<!--- &#124;即管道符 转义不能用在``里, 表格内不允许使用管道符 -->

