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

### 使用 export-csv.sh

#### 运行方法

```shell
# Unix/OS X
> tools/export-csv.sh  -h <ip> -p <port> -u <username> -pw <password> -td <directory> [-tf <time-format> -datatype <true/false> -q <query command> -s <sql file>]

# Windows
> tools\export-csv.bat -h <ip> -p <port> -u <username> -pw <password> -td <directory> [-tf <time-format> -datatype <true/false> -q <query command> -s <sql file>]
```

参数:

* `-datatype`:
  - true (默认): 在CSV文件的header中时间序列的后面打印出对应的数据类型。例如：`Time, root.sg1.d1.s1(INT32), root.sg1.d1.s2(INT64)`.
  - false: 只在CSV的header中打印出时间序列的名字, `Time, root.sg1.d1.s1 , root.sg1.d1.s2`
* `-q <query command>`:
  - 在命令中直接指定想要执行的查询语句。
  - 例如: `select * from root limit 100`, or `select * from root limit 100 align by device`
* `-s <sql file>`:
  - 指定一个SQL文件，里面包含一条或多条SQL语句。如果一个SQL文件中包含多条SQL语句，SQL语句之间应该用换行符进行分割。每一条SQL语句对应一个输出的CSV文件。
* `-td <directory>`:
  - 为导出的CSV文件指定输出路径。
* `-tf <time-format>`:
  - 指定一个你想要得到的时间格式。时间格式必须遵守[ISO 8601](https://calendars.wikia.org/wiki/ISO_8601)标准。如果说你想要以时间戳来保存时间，那就设置为`-tf timestamp`。
  - 例如: `-tf yyyy-MM-dd\ HH:mm:ss` or `-tf timestamp`

除此之外，如果你没有使用`-s`和`-q`参数，在导出脚本被启动之后你需要按照程序提示输入查询语句，不同的查询结果会被保存到不同的CSV文件中。

#### 运行示例

```shell
# Unix/OS X
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./
# Or
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss
# or
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -q "select * from root"
# Or
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s sql.txt
# Or
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss -s sql.txt

# Windows
> tools/export-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./
# Or
> tools/export-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss
# or
> tools/export-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -q "select * from root"
# Or
> tools/export-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s sql.txt
# Or
> tools/export-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss -s sql.txt
```

#### SQL 文件示例

```sql
select * from root;
select * from root align by device;
```

`select * from root`的执行结果：

```sql
Time,root.ln.wf04.wt04.status(BOOLEAN),root.ln.wf03.wt03.hardware(TEXT),root.ln.wf02.wt02.status(BOOLEAN),root.ln.wf02.wt02.hardware(TEXT),root.ln.wf01.wt01.hardware(TEXT),root.ln.wf01.wt01.status(BOOLEAN)
1970-01-01T08:00:00.001+08:00,true,"v1",true,"v1",v1,true
1970-01-01T08:00:00.002+08:00,true,"v1",,,,true
```

`select * from root align by device`的执行结果：

```sql
Time,Device,hardware(TEXT),status(BOOLEAN)
1970-01-01T08:00:00.001+08:00,root.ln.wf01.wt01,"v1",true
1970-01-01T08:00:00.002+08:00,root.ln.wf01.wt01,,true
1970-01-01T08:00:00.001+08:00,root.ln.wf02.wt02,"v1",true
1970-01-01T08:00:00.001+08:00,root.ln.wf03.wt03,"v1",
1970-01-01T08:00:00.002+08:00,root.ln.wf03.wt03,"v1",
1970-01-01T08:00:00.001+08:00,root.ln.wf04.wt04,,true
1970-01-01T08:00:00.002+08:00,root.ln.wf04.wt04,,true
```

布尔类型的数据用`true`或者`false`来表示，此处没有用双引号括起来。文本数据需要使用双引号括起来。

#### 注意

注意，如果导出字段存在如下特殊字符:

1. `,`: 导出程序会在`,`字符前加`\`来进行转义。

### 使用import-csv.sh

#### 创建元数据 (可选)

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

通过时间对齐，并且header中不包含数据类型的数据。

```sql
Time,root.test.t1.str,root.test.t2.str,root.test.t2.int
1970-01-01T08:00:00.001+08:00,"123hello world","123\,abc",100
1970-01-01T08:00:00.002+08:00,"123",,
```

通过时间对齐，并且header中包含数据类型的数据。（Text类型数据支持加双引号和不加双引号）

```sql
Time,root.test.t1.str(TEXT),root.test.t2.str(TEXT),root.test.t2.int(INT32)
1970-01-01T08:00:00.001+08:00,"123hello world","123\,abc",100
1970-01-01T08:00:00.002+08:00,123,hello world,123
1970-01-01T08:00:00.003+08:00,"123",,
1970-01-01T08:00:00.004+08:00,123,,12
```

通过设备对齐，并且header中不包含数据类型的数据。

```sql
Time,Device,str,int
1970-01-01T08:00:00.001+08:00,root.test.t1,"123hello world",
1970-01-01T08:00:00.002+08:00,root.test.t1,"123",
1970-01-01T08:00:00.001+08:00,root.test.t2,"123\,abc",100
```

通过时间对齐，并且header中包含数据类型的数据。（Text类型数据支持加双引号和不加双引号）

```sql
Time,Device,str(TEXT),int(INT32)
1970-01-01T08:00:00.001+08:00,root.test.t1,"123hello world",
1970-01-01T08:00:00.002+08:00,root.test.t1,"123",
1970-01-01T08:00:00.001+08:00,root.test.t2,"123\,abc",100
1970-01-01T08:00:00.002+08:00,root.test.t1,hello world,123
```

#### 运行方法

```shell
# Unix/OS X
>tools/import-csv.sh -h <ip> -p <port> -u <username> -pw <password> -f <xxx.csv> [-fd <./failedDirectory>] [-aligned <true>]
# Windows
>tools\import-csv.bat -h <ip> -p <port> -u <username> -pw <password> -f <xxx.csv> [-fd <./failedDirectory>] [-aligned <true>]
```

参数:

* `-f`:
  - 指定你想要导入的数据，这里可以指定文件或者文件夹。如果指定的是文件夹，将会把文件夹中所有的后缀为txt与csv的文件进行批量导入。
  - 例如: `-f filename.csv`

* `-fd`:
  - 指定一个目录来存放保存失败的行的文件，如果你没有指定这个参数，失败的文件将会被保存到源数据的目录中，然后文件名是源文件名加上`.failed`的后缀。
  - 例如: `-fd ./failed/`

* `-aligned`:
  - 是否使用`aligned`接口？ 默认参数为`false`。
  - 例如: `-aligned true`

* `-batch`:
  - 用于指定每一批插入的数据的点数。如果程序报了`org.apache.thrift.transport.TTransportException: Frame size larger than protect max size`这个错的话，就可以适当的调低这个参数。
  - 例如: `-batch 100000`，`100000`是默认值。

#### 运行示例

```sh
# Unix/OS X
>tools/import-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv -fd ./failed
# or
>tools/import-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv -fd ./failed
# Windows
>tools\import-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv
# or
>tools\import-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv -fd .\failed
```

#### 注意

注意，在导入数据前，需要特殊处理下列的字符：

1. `,` :如果text类型的字段中包含`,`那么需要用`\`来进行转义。
2. 你可以导入像`yyyy-MM-dd'T'HH:mm:ss`， `yyy-MM-dd HH:mm:ss`， 或者 `yyyy-MM-dd'T'HH:mm:ss.SSSZ`格式的时间。
3. `Time`这一列应该放在第一列。