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

# CSV Tool

The CSV tool can help you import data in CSV format to IoTDB or export data from IoTDB to a CSV file.

## Usage of export-csv.sh

### Syntax

```shell
# Unix/OS X
> tools/export-csv.sh  -h <ip> -p <port> -u <username> -pw <password> -td <directory> [-tf <time-format> -datatype <true/false> -q <query command> -s <sql file>]

# Windows
> tools\export-csv.bat -h <ip> -p <port> -u <username> -pw <password> -td <directory> [-tf <time-format> -datatype <true/false> -q <query command> -s <sql file>]
```

Description:

* `-datatype`:
  - true (by default): print the data type of timesries in the head line of CSV file. i.e., `Time, root.sg1.d1.s1(INT32), root.sg1.d1.s2(INT64)`.
  - false: only print the timeseries name in the head line of the CSV file. i.e., `Time, root.sg1.d1.s1 , root.sg1.d1.s2`
* `-q <query command>`:
  - specifying a query command that you want to execute
  - example: `select * from root limit 100`, or `select * from root limit 100 align by device`
* `-s <sql file>`:
  - specifying a SQL file which can consist of more than one sql. If there are multiple SQLs in one SQL file, the SQLs should be separated by line breaks. And, for each SQL, a output CSV file will be generated.
* `-td <directory>`:
  - specifying  the directory that the data will be exported
* `-tf <time-format>`:
  - specifying a time format that you want. The time format have to obey [ISO 8601](https://calendars.wikia.org/wiki/ISO_8601) standard. If you want to save the time as the timestamp, then setting `-tf timestamp`
  - example: `-tf yyyy-MM-dd\ HH:mm:ss` or `-tf timestamp`

More, if you don't use one of `-s` and `-q`, you need to enter some queries after running the export script. The results of the different query will be saved to different CSV files.

### example

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

### Sample SQL file

```sql
select * from root;
select * from root align by device;
```

The result of `select * from root`

```sql
Time,root.ln.wf04.wt04.status(BOOLEAN),root.ln.wf03.wt03.hardware(TEXT),root.ln.wf02.wt02.status(BOOLEAN),root.ln.wf02.wt02.hardware(TEXT),root.ln.wf01.wt01.hardware(TEXT),root.ln.wf01.wt01.status(BOOLEAN)
1970-01-01T08:00:00.001+08:00,true,"v1",true,"v1",v1,true
1970-01-01T08:00:00.002+08:00,true,"v1",,,,true
```

The result of `select * from root align by device`

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

The data of boolean type signed by `true` and `false` without double quotes. And the text data will be enclosed in double quotes.

### Note

Note that if fields exported by the export tool have the following special characters:

1. `,`: the field will be escaped by `\`.

## Usage of import-csv.sh

### Create metadata (optional)

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

IoTDB has the ability of type inference, so it is not necessary to create metadata before data import. However, we still recommend creating metadata before importing data using the CSV import tool, as this can avoid unnecessary type conversion errors.

### Sample CSV file to be imported

The data aligned by time, and headers without data type.

```sql
Time,root.test.t1.str,root.test.t2.str,root.test.t2.int
1970-01-01T08:00:00.001+08:00,"123hello world","123\,abc",100
1970-01-01T08:00:00.002+08:00,"123",,
```

The data aligned by time, and headers with data type.（Text type data supports double quotation marks and no double quotation marks）

```sql
Time,root.test.t1.str(TEXT),root.test.t2.str(TEXT),root.test.t2.int(INT32)
1970-01-01T08:00:00.001+08:00,"123hello world","123\,abc",100
1970-01-01T08:00:00.002+08:00,123,hello world,123
1970-01-01T08:00:00.003+08:00,"123",,
1970-01-01T08:00:00.004+08:00,123,,12
```

The data aligned by device, and headers without data type.

```sql
Time,Device,str,int
1970-01-01T08:00:00.001+08:00,root.test.t1,"123hello world",
1970-01-01T08:00:00.002+08:00,root.test.t1,"123",
1970-01-01T08:00:00.001+08:00,root.test.t2,"123\,abc",100
```

The data aligned by time, and headers with data type.（Text type data supports double quotation marks and no double quotation marks）

```sql
Time,Device,str(TEXT),int(INT32)
1970-01-01T08:00:00.001+08:00,root.test.t1,"123hello world",
1970-01-01T08:00:00.002+08:00,root.test.t1,hello world,123
1970-01-01T08:00:00.003+08:00,root.test.t1,,123
```

### Syntax

```shell
# Unix/OS X
> tools/import-csv.sh -h <ip> -p <port> -u <username> -pw <password> -f <xxx.csv> [-fd <./failedDirectory>] [-aligned <true>]
# Windows
> tools\import-csv.bat -h <ip> -p <port> -u <username> -pw <password> -f <xxx.csv> [-fd <./failedDirectory>] [-aligned <true>]
```

Description:

* `-f`:
  - the CSV file that you want to import, and it could be a file or a folder. If a folder is specified, all TXT and CSV files in the folder will be imported in batches.
  - example: `-f filename.csv`

* `-fd`:
  - specifying a directory to save files which save failed lines. If you don't use this parameter, the failed file will be saved at original directory, and the filename will be the source filename with suffix `.failed`.
  - example: `-fd ./failed/`
  
* `-aligned`:
  - whether to use the aligned interface? The option `false` is default.
  - example: `-aligned true`

* `-batch`:
  - specifying the point's number of a batch. If the program throw the exception `org.apache.thrift.transport.TTransportException: Frame size larger than protect max size`, you can lower this parameter as appropriate.
  - example: `-batch 100000`, `100000` is the default value.

### Example

```sh
# Unix/OS X
> tools/import-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv -fd ./failed
# or
> tools/import-csv.sh -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv -fd ./failed
# Windows
> tools\import-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv
# or
> tools\import-csv.bat -h 127.0.0.1 -p 6667 -u root -pw root -f example-filename.csv -fd .\failed
```

### Note

Note that the following special characters in fields need to be checked before importing:

1. `,` : fields containing `,` should be escaped by `\`.
2. you can input time format like `yyyy-MM-dd'T'HH:mm:ss`, `yyy-MM-dd HH:mm:ss`, or `yyyy-MM-dd'T'HH:mm:ss.SSSZ`.
3. the `Time` column must be the first one.