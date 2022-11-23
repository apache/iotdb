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

# TsFile Tool

TsFile can help you export the result set in the format of TsFile file to the specified path by executing the sql, command line sql, and sql file.

## Usage of export-tsfile.sh

### Syntax

```shell
# Unix/OS X
> tools/export-tsfile.sh  -h <ip> -p <port> -u <username> -pw <password> -td <directory> [-f <export filename> -q <query command> -s <sql file>]

# Windows
> tools\export-tsfile.bat -h <ip> -p <port> -u <username> -pw <password> -td <directory> [-f <export filename> -q <query command> -s <sql file>]
```

* `-h <host>`:
  - The host address of the IoTDB service.
* `-p <port>`:
  - The port number of the IoTDB service.
* `-u <username>`:
  - The username of the IoTDB service.
* `-pw <password>`:
  - Password for IoTDB service.
* `-td <directory>`:
  - Specify the output path for the exported TsFile file.
* `-f <tsfile name>`:
  - For the file name of the exported TsFile file, just write the file name, and cannot include the file path and suffix. If the sql file or console input contains multiple sqls, multiple files will be generated in the order of sql.
  - Example: There are three SQLs in the file or command line, and -f param is "dump", then three TsFile files: dump0.tsfile、dump1.tsfile、dump2.tsfile will be generated in the target path.
* `-q <query command>`:
  - Directly specify the query statement you want to execute in the command.
  - Example: `select * from root.** limit 100`
* `-s <sql file>`:
  - Specify a SQL file that contains one or more SQL statements. If an SQL file contains multiple SQL statements, the SQL statements should be separated by newlines. Each SQL statement corresponds to an output TsFile file.


In addition, if you do not use the `-s` and `-q` parameters, after the export script is started, you need to enter the query statement as prompted by the program, and different query results will be saved to different TsFile files.

### example

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

### example
- It is recommended not to execute the write data command at the same time when loading data, which may lead to insufficient memory in the JVM.