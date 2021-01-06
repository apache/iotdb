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
# Csv tool

Csv tool is that you can import csv file into IoTDB or export csv file from IoTDB.

## Usage of import-csv.sh

### Create metadata
```
SET STORAGE GROUP TO root.fit.d1;
SET STORAGE GROUP TO root.fit.d2;
SET STORAGE GROUP TO root.fit.p;
CREATE TIMESERIES root.fit.d1.s1 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.d1.s2 WITH DATATYPE=TEXT,ENCODING=PLAIN;
CREATE TIMESERIES root.fit.d2.s1 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.d2.s3 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.p.s1 WITH DATATYPE=INT32,ENCODING=RLE;
```

### An example of import csv file

```
Time,root.fit.d1.s1,root.fit.d1.s2,root.fit.d2.s1,root.fit.d2.s3,root.fit.p.s1
1,100,'hello',200,300,400
2,500,'world',600,700,800
3,900,'IoTDB',1000,1100,1200
```

### Run import shell
```
# Unix/OS X
> tools/import-csv.sh -h <ip> -p <port> -u <username> -pw <password> -f <xxx.csv>

# Windows
> tools\import-csv.bat -h <ip> -p <port> -u <username> -pw <password> -f <xxx.csv>
```

### Error data file

`csvInsertError.error`

## Usage of export-csv.sh

### Run export shell
```
# Unix/OS X
> tools/export-csv.sh -h <ip> -p <port> -u <username> -pw <password> -td <directory> [-tf <time-format>]

# Windows
> tools\export-csv.bat -h <ip> -p <port> -u <username> -pw <password> -td <directory> [-tf <time-format>]
```

### Input query

```
select * from root.fit.d1
```
