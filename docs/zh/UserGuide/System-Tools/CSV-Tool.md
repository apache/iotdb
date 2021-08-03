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

## 导入导出 CSV

CSV 工具可帮您将 CSV 格式的数据导入到 IoTDB 或者将数据从 IoTDB 导出到 CSV 文件。

### 使用 import-csv.sh

#### 创建元数据（可选）

```sql
SET STORAGE GROUP TO root.fit.d1;
SET STORAGE GROUP TO root.fit.d2;
SET STORAGE GROUP TO root.fit.p;
CREATE TIMESERIES root.fit.d1.s1 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.d1.s2 WITH DATATYPE=TEXT,ENCODING=PLAIN;
CREATE TIMESERIES root.fit.d2.s1 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.d2.s3 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.p.s1 WITH DATATYPE=INT32,ENCODING=RLE;
```
IoTDB 具有类型推断的能力，因此在数据导入前创建元数据不是必须的。但我们仍然推荐在使用 CSV 导入工具导入数据前创建元数据，因为这可以避免不必要的类型转换错误。

#### 待导入 CSV 文件示例

```sql
Time,root.fit.d1.s1,root.fit.d1.s2,root.fit.d2.s1,root.fit.d2.s3,root.fit.p.s1
1,100,hello,200,300,400
2,500,world,600,700,800
3,900,"hello, \"world\"",1000,1100,1200
```

#### 运行方法

```shell
# Unix/OS X
> tools/import-csv.sh -h <ip> -p <port> -u <username> -pw <password> -f <xxx.csv>

# Windows
> tools\import-csv.bat -h <ip> -p <port> -u <username> -pw <password> -f <xxx.csv>
```

#### 运行示例

```shell
# Unix/OS X
> tools/import-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv

# Windows
> tools\import-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv
```

#### 注意事项

注意，在导入数据前，需要特殊处理下列的字符：

1. `,` : 包含`,`的字段需要使用单引号或者双引号括起来
2. `"` : "字段中的`"`需要被替换成转义字符`\"`或者用`\'`将字段括起来。
3. `'` : "字段中的`'`需要被替换成转义字符`\'`或者用`\"`将字段括起来。
4. 你可以输入时间格式像`yyyy-MM-dd'T'HH:mm:ss`, `yyy-MM-dd HH:mm:ss`, 或者`yyyy-MM-dd'T'HH:mm:ss.SSSZ`.

### 使用 export-csv.sh

#### 运行方法

```shell
# Unix/OS X
> tools/export-csv.sh -h <ip> -p <port> -u <username> -pw <password> -td <directory> [-tf <time-format> -s <sqlfile>]

# Windows
> tools\export-csv.bat -h <ip> -p <port> -u <username> -pw <password> -td <directory> [-tf <time-format> -s <sqlfile>]
```

在运行导出脚本之后，您需要输入一些查询或指定一些 SQL 文件。如果在一个 SQL 文件中有多个 SQL，SQL 应该被换行符分割。

#### SQL 文件示例

```sql
select * from root.fit.d1
select * from root.sg1.d1
```

#### 运行示例

```shell
# Unix/OS X
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./
# Or
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss
# Or
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s sql.txt
# Or
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss -s sql.txt

# Windows
> tools/export-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./
# Or
> tools/export-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss
# Or
> tools/export-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s sql.txt
# Or
> tools/export-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss -s sql.txt
```

#### 注意事项

注意，如果导出字段存在如下特殊字符：

1. `,` : 整个字段会被用`"`括起来。
2. `"` : 整个字段会被用`"`括起来且`"`会被替换为`\"`。
