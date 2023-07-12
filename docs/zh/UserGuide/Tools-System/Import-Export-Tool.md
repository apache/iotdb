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

# 导入导出工具

针对于不同场景，IoTDB 为用户提供多种批量导入数据的操作方式，本章节向大家介绍最为常用的两种方式为 CSV文本形式的导入 和 TsFile文件形式的导入。

## TsFile导入导出工具

TsFile 是在 IoTDB 中使用的时间序列的文件格式，您可以通过CLI等工具直接将存有时间序列的一个或多个 TsFile 文件导入到另外一个正在运行的IoTDB实例中。
### 介绍
加载外部 tsfile 文件工具允许用户向正在运行中的 Apache IoTDB 中加载 tsfile 文件。或者您也可以使用脚本的方式将tsfile加载进IoTDB。

### 使用SQL加载
用户通过 Cli 工具或 JDBC 向 Apache IoTDB 系统发送指定命令实现文件加载的功能。

#### 加载 tsfile 文件

加载 tsfile 文件的指令为：`load '<path/dir>' [sglevel=int][verify=true/false][onSuccess=delete/none]`

该指令有两种用法：

1. 通过指定文件路径(绝对路径)加载单 tsfile 文件。

第一个参数表示待加载的 tsfile 文件的路径。load 命令有三个可选项，分别是 sglevel，值域为整数，verify，值域为 true/false，onSuccess，值域为delete/none。不同选项之间用空格隔开，选项之间无顺序要求。

SGLEVEL 选项，当 tsfile 对应的 database 不存在时，用户可以通过 sglevel 参数的值来制定 database 的级别，默认为`iotdb-datanode.properties`中设置的级别。例如当设置 level 参数为1时表明此 tsfile 中所有时间序列中层级为1的前缀路径是 database，即若存在设备 root.sg.d1.s1，此时 root.sg 被指定为 database。

VERIFY 选项表示是否对载入的 tsfile 中的所有时间序列进行元数据检查，默认为 true。开启时，若载入的 tsfile 中的时间序列在当前 iotdb 中也存在，则会比较该时间序列的所有 Measurement 的数据类型是否一致，如果出现不一致将会导致载入失败，关闭该选项会跳过检查，载入更快。

ONSUCCESS选项表示对于成功载入的tsfile的处置方式，默认为delete，即tsfile成功加载后将被删除，如果是none表明tsfile成功加载之后依然被保留在源文件夹。

若待加载的 tsfile 文件对应的`.resource`文件存在，会被一并加载至 Apache IoTDB 数据文件的目录和引擎中，否则将通过 tsfile 文件重新生成对应的`.resource`文件，即加载的 tsfile 文件所对应的`.resource`文件不是必要的。

示例：

* `load '/Users/Desktop/data/1575028885956-101-0.tsfile'`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=true`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=false`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' sglevel=1`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' onSuccess=delete`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=true sglevel=1`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=false sglevel=1`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=true onSuccess=none`
* `load '/Users/Desktop/data/1575028885956-101-0.tsfile' verify=false sglevel=1 onSuccess=delete`


2. 通过指定文件夹路径(绝对路径)批量加载文件。

第一个参数表示待加载的 tsfile 文件夹的路径。选项意义与加载单个 tsfile 文件相同。

示例：

* `load '/Users/Desktop/data'`
* `load '/Users/Desktop/data' verify=false`
* `load '/Users/Desktop/data' verify=true`
* `load '/Users/Desktop/data' verify=true sglevel=1`
* `load '/Users/Desktop/data' verify=false sglevel=1 onSuccess=delete`

**注意**，如果`$IOTDB_HOME$/conf/iotdb-datanode.properties`中`enable_auto_create_schema=true`时会在加载tsfile的时候自动创建tsfile中的元数据，否则不会自动创建。

### 使用脚本加载

若您在Windows环境中，请运行`$IOTDB_HOME/tools/load-tsfile.bat`，若为Linux或Unix，请运行`load-tsfile.sh`

```bash
./load-tsfile.bat -f filePath [-h host] [-p port] [-u username] [-pw password] [--sgLevel int] [--verify true/false] [--onSuccess none/delete]
-f 			待加载的文件或文件夹路径，必要字段
-h 			IoTDB的Host地址，可选，默认127.0.0.1
-p 			IoTDB的端口，可选，默认6667
-u 			IoTDb登录用户名，可选，默认root
-pw 		IoTDB登录密码，可选，默认root
--sgLevel 	加载TsFile自动创建Database的路径层级，可选，默认值为iotdb-common.properties指定值
--verify 	是否对加载TsFile进行元数据校验，可选，默认为True
--onSuccess 对成功加载的TsFile的处理方法，可选，默认为delete，成功加载之后删除源TsFile，设为none时会				保留源TsFile
```

#### 使用范例

假定服务器192.168.0.101:6667上运行一个IoTDB实例，想从将本地保存的TsFile备份文件夹D:\IoTDB\data中的所有的TsFile文件都加载进此IoTDB实例。

首先移动至`$IOTDB_HOME/tools/`，打开命令行，然后执行

```bash
./load-tsfile.bat -f D:\IoTDB\data -h 192.168.0.101 -p 6667 -u root -pw root
```

等待脚本执行完成之后，可以检查IoTDB实例中数据已经被正确加载

#### 常见问题

- 找不到或无法加载主类
  - 可能是由于未设置环境变量$IOTDB_HOME，请设置环境变量之后重试
- 提示-f option must be set!
  - 输入命令缺少待-f字段（加载文件或文件夹路径），请添加之后重新执行
- 执行到中途崩溃了想重新加载怎么办
  - 重新执行刚才的命令，重新加载数据不会影响加载之后的正确性

## 导出 TsFile

TsFile 工具可帮您 通过执行指定sql、命令行sql、sql文件的方式将结果集以TsFile文件的格式导出至指定路径.

### 使用 export-tsfile.sh

#### 运行方法

```shell
# Unix/OS X
> tools/export-tsfile.sh  -h <ip> -p <port> -u <username> -pw <password> -td <directory> [-f <export filename> -q <query command> -s <sql file>]

# Windows
> tools\export-tsfile.bat -h <ip> -p <port> -u <username> -pw <password> -td <directory> [-f <export filename> -q <query command> -s <sql file>]
```

参数:
* `-h <host>`:
  - IoTDB服务的主机地址。
* `-p <port>`:
  - IoTDB服务的端口号。
* `-u <username>`:
  - IoTDB服务的用户名。
* `-pw <password>`:
  - IoTDB服务的密码。
* `-td <directory>`:
  - 为导出的TsFile文件指定输出路径。
* `-f <tsfile name>`:
  - 为导出的TsFile文件的文件名，只需写文件名称，不能包含文件路径和后缀。如果sql文件或控制台输入时包含多个sql，会按照sql顺序生成多个TsFile文件。
  - 例如：文件中或命令行共有3个SQL，-f 为"dump"，那么会在目标路径下生成 dump0.tsfile、dump1.tsfile、dump2.tsfile三个TsFile文件。
* `-q <query command>`:
  - 在命令中直接指定想要执行的查询语句。
  - 例如: `select * from root.** limit 100`
* `-s <sql file>`:
  - 指定一个SQL文件，里面包含一条或多条SQL语句。如果一个SQL文件中包含多条SQL语句，SQL语句之间应该用换行符进行分割。每一条SQL语句对应一个输出的TsFile文件。
* `-t <timeout>`:
  - 指定session查询时的超时时间，单位为ms


除此之外，如果你没有使用`-s`和`-q`参数，在导出脚本被启动之后你需要按照程序提示输入查询语句，不同的查询结果会被保存到不同的TsFile文件中。

#### 运行示例

```shell
# Unix/OS X
> tools/export-tsfile.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./
# or
> tools/export-tsfile.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -q "select * from root.**"
# Or
> tools/export-tsfile.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s ./sql.txt
# Or
> tools/export-tsfile.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s ./sql.txt -f myTsFile
# Or
> tools/export-tsfile.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s ./sql.txt -f myTsFile -t 10000

# Windows
> tools/export-tsfile.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./
# Or
> tools/export-tsfile.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -q "select * from root.**"
# Or
> tools/export-tsfile.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s ./sql.txt
# Or
> tools/export-tsfile.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s ./sql.txt -f myTsFile
# Or
> tools/export-tsfile.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s ./sql.txt -f myTsFile -t 10000
```

#### Q&A

- 建议在导入数据时不要同时执行写入数据命令，这将有可能导致JVM内存不足的情况。

## CSV导入导出工具

CSV 是以纯文本形式存储表格数据，您可以在CSV文件中写入多条格式化的数据，并批量的将这些数据导入到 IoTDB 中，在导入数据之前，建议在IoTDB中创建好对应的元数据信息。如果忘记创建元数据也不要担心，IoTDB 可以自动将CSV中数据推断为其对应的数据类型，前提是你每一列的数据类型必须唯一。除单个文件外，此工具还支持以文件夹的形式导入多个 CSV 文件，并且支持设置如时间精度等优化参数。具体操作方式请参考 [CSV 导入导出工具](../Maintenance-Tools/CSV-Tool.md)。

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
  - 例如: `select * from root.** limit 100`, or `select * from root.** limit 100 align by device`
* `-s <sql file>`:
  - 指定一个SQL文件，里面包含一条或多条SQL语句。如果一个SQL文件中包含多条SQL语句，SQL语句之间应该用换行符进行分割。每一条SQL语句对应一个输出的CSV文件。
* `-td <directory>`:
  - 为导出的CSV文件指定输出路径。
* `-tf <time-format>`:
  - 指定一个你想要得到的时间格式。时间格式必须遵守[ISO 8601](https://calendars.wikia.org/wiki/ISO_8601)标准。如果说你想要以时间戳来保存时间，那就设置为`-tf timestamp`。
  - 例如: `-tf yyyy-MM-dd\ HH:mm:ss` or `-tf timestamp`
* `-linesPerFile <int>`:
  - 指定导出的dump文件最大行数，默认值为`10000`。
  - 例如： `-linesPerFile 1`
* `-t <timeout>`:
  - 指定session查询时的超时时间，单位为ms

除此之外，如果你没有使用`-s`和`-q`参数，在导出脚本被启动之后你需要按照程序提示输入查询语句，不同的查询结果会被保存到不同的CSV文件中。

#### 运行示例

```shell
# Unix/OS X
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./
# Or
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss
# or
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -q "select * from root.**"
# Or
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s sql.txt
# Or
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss -s sql.txt
# Or
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss -s sql.txt -linesPerFile 10
# Or
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss -s sql.txt -linesPerFile 10 -t 10000

# Windows
> tools/export-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./
# Or
> tools/export-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss
# or
> tools/export-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -q "select * from root.**"
# Or
> tools/export-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s sql.txt
# Or
> tools/export-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss -s sql.txt
# Or
> tools/export-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss -s sql.txt -linesPerFile 10
# Or
> tools/export-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -tf yyyy-MM-dd\ HH:mm:ss -s sql.txt -linesPerFile 10 -t 10000
```

#### SQL 文件示例

```sql
select * from root.**;
select * from root.** align by device;
```

`select * from root.**`的执行结果：

```sql
Time,root.ln.wf04.wt04.status(BOOLEAN),root.ln.wf03.wt03.hardware(TEXT),root.ln.wf02.wt02.status(BOOLEAN),root.ln.wf02.wt02.hardware(TEXT),root.ln.wf01.wt01.hardware(TEXT),root.ln.wf01.wt01.status(BOOLEAN)
1970-01-01T08:00:00.001+08:00,true,"v1",true,"v1",v1,true
1970-01-01T08:00:00.002+08:00,true,"v1",,,,true
```

`select * from root.** align by device`的执行结果：

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

### 使用 import-csv.sh

#### 创建元数据 (可选)

```sql
CREATE DATABASE root.fit.d1;
CREATE DATABASE root.fit.d2;
CREATE DATABASE root.fit.p;
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

通过设备对齐，并且header中包含数据类型的数据。（Text类型数据支持加双引号和不加双引号）

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
>tools/import-csv.sh -h <ip> -p <port> -u <username> -pw <password> -f <xxx.csv> [-fd <./failedDirectory>] [-aligned <true>] [-tp <ms/ns/us>] [-typeInfer <boolean=text,float=double...>] [-linesPerFailedFile <int_value>]
# Windows
>tools\import-csv.bat -h <ip> -p <port> -u <username> -pw <password> -f <xxx.csv> [-fd <./failedDirectory>] [-aligned <true>] [-tp <ms/ns/us>] [-typeInfer <boolean=text,float=double...>] [-linesPerFailedFile <int_value>]
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

* `-tp`:
  - 用于指定时间精度，可选值包括`ms`（毫秒），`ns`（纳秒），`us`（微秒），默认值为`ms`。

* `-typeInfer <srcTsDataType1=dstTsDataType1,srcTsDataType2=dstTsDataType2,...>`:
  - 用于指定类型推断规则.
  - `srcTsDataType` 包括 `boolean`,`int`,`long`,`float`,`double`,`NaN`.
  - `dstTsDataType` 包括 `boolean`,`int`,`long`,`float`,`double`,`text`.
  - 当`srcTsDataType`为`boolean`, `dstTsDataType`只能为`boolean`或`text`.
  - 当`srcTsDataType`为`NaN`, `dstTsDataType`只能为`float`, `double`或`text`.
  - 当`srcTsDataType`为数值类型, `dstTsDataType`的精度需要高于`srcTsDataType`.
  - 例如:`-typeInfer boolean=text,float=double`

* `-linesPerFailedFile <int>`:
  - 用于指定每个导入失败文件写入数据的行数，默认值为10000。
  - 例如：`-linesPerFailedFile 1`

#### 运行示例

```sh
# Unix/OS X
>tools/import-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv -fd ./failed
# or
>tools/import-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv -fd ./failed
# or
> tools\import-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv -fd ./failed -tp ns
# or
> tools\import-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv -fd ./failed -tp ns -typeInfer boolean=text,float=double
# or
> tools\import-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv -fd ./failed -tp ns -typeInfer boolean=text,float=double -linesPerFailedFile 10
# Windows
>tools\import-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv
# or
>tools\import-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv -fd .\failed
# or
> tools\import-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv -fd .\failed -tp ns
# or
> tools\import-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv -fd .\failed -tp ns -typeInfer boolean=text,float=double
# or
> tools\import-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv -fd .\failed -tp ns -typeInfer boolean=text,float=double -linesPerFailedFile 10
```

#### 注意

注意，在导入数据前，需要特殊处理下列的字符：

1. `,` :如果text类型的字段中包含`,`那么需要用`\`来进行转义。
2. 你可以导入像`yyyy-MM-dd'T'HH:mm:ss`， `yyy-MM-dd HH:mm:ss`， 或者 `yyyy-MM-dd'T'HH:mm:ss.SSSZ`格式的时间。
3. `Time`这一列应该放在第一列。