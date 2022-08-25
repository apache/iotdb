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

# 导入导出 TsFile

TsFile 工具可帮您 通过执行指定sql、命令行sql、sql文件的方式将结果集以TsFile文件的格式导出至指定路径.

## 使用 export-tsfile.sh

### 运行方法

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


除此之外，如果你没有使用`-s`和`-q`参数，在导出脚本被启动之后你需要按照程序提示输入查询语句，不同的查询结果会被保存到不同的TsFile文件中。

### 运行示例

```shell
# Unix/OS X
> tools/export-tsfile.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./
# or
> tools/export-tsfile.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -q "select * from root.**"
# Or
> tools/export-tsfile.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s ./sql.txt
# Or
> tools/export-tsfile.sh -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s ./sql.txt -f myTsFile

# Windows
> tools/export-tsfile.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./
# Or
> tools/export-tsfile.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -q "select * from root.**"
# Or
> tools/export-tsfile.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s ./sql.txt
# Or
> tools/export-tsfile.bat -h 127.0.0.1 -p 6667 -u root -pw root -td ./ -s ./sql.txt -f myTsFile
```