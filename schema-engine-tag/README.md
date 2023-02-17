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

# Tag Schema Region
`TagSchemaRegion` is an implementation of `SchemaRegion`
<pre>
  _____             _____      _                           ______           _             
|_   _|           /  ___|    | |                          | ___ \         (_)            
  | | __ _  __ _  \ `--.  ___| |__   ___ _ __ ___   __ _  | |_/ /___  __ _ _  ___  _ __  
  | |/ _` |/ _` |  `--. \/ __| '_ \ / _ \ '_ ` _ \ / _` | |    // _ \/ _` | |/ _ \| '_ \ 
  | | (_| | (_| | /\__/ / (__| | | |  __/ | | | | | (_| | | |\ \  __/ (_| | | (_) | | | |
  \_/\__,_|\__, | \____/ \___|_| |_|\___|_| |_| |_|\__,_| \_| \_\___|\__, |_|\___/|_| |_|
            __/ |                                                     __/ |              
           |___/                                                     |___/ > version 1.0.0
</pre>

# How To Use

Firstly, you should package **schema-engine-tag** by the following command:

```shell
mvn clean package -pl schema-engine-tag -am -DskipTests
```

After that, you can get a **conf** directory and a **lib** directory in
schema-engine-tag/target/schema-engine-tag. Copy the file in the conf directory to the conf directory of server,
and copy the files in the lib directory to the lib directory of server.

Then, open the **iotdb-datanode.properties** in the conf directory of server, and set the `schema_engine_mode` to
**Tag**, set the `enable_id_table` to **true**. Restart the IoTDB, the system will use `TagSchemaRegion` to manage
the metadata.

## Use Cli 

IoTDB offers different ways to interact with server, here we introduce the basic steps of using Cli tool to insert and query data.
The command line cli is interactive, so you should see the welcome logo and statements if everything is ready:
```sql
---------------------
Starting IoTDB Cli
---------------------
 _____       _________  ______   ______    
|_   _|     |  _   _  ||_   _ `.|_   _ \   
  | |   .--.|_/ | | \_|  | | `. \ | |_) |  
  | | / .'`\ \  | |      | |  | | |  __'.  
 _| |_| \__. | _| |_    _| |_.' /_| |__) | 
|_____|'.__.' |_____|  |______.'|_______/  version 1.0.0
                                           

IoTDB> login successfully
```
### create timeseries

- create timeseries

```sql
IoTDB> create timeseries root.ln.tag1.a.tag2.b.status with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
```
- create aligned timeseries

```sql
IoTDB> CREATE ALIGNED TIMESERIES root.ln.tag1.a.tag2.c(latitude FLOAT encoding=PLAIN compressor=SNAPPY, longitude FLOAT  encoding=PLAIN compressor=SNAPPY)

Msg: The statement is executed successfully.
```

### show timeserie

- point query

enter a full path

```sql
IoTDB> show timeseries root.ln.tag2.c.tag1.a
+-------------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                     timeseries|alias|database|dataType|encoding|compression|tags|attributes|
+-------------------------------+-----+-------------+--------+--------+-----------+----+----------+
| root.ln.tag1.a.tag2.c.latitude| null|      root.ln|   FLOAT|   PLAIN|     SNAPPY|null|      null|
|root.ln.tag1.a.tag2.c.longitude| null|      root.ln|   FLOAT|   PLAIN|     SNAPPY|null|      null|
+-------------------------------+-----+-------------+--------+--------+-----------+----+----------+
```

- batch query

paths ending in ".**" indicate batch query

```sql
IoTDB> show timeseries root.ln.tag1.a.**
+-------------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                     timeseries|alias|database|dataType|encoding|compression|tags|attributes|
+-------------------------------+-----+-------------+--------+--------+-----------+----+----------+
|   root.ln.tag1.a.tag2.b.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
| root.ln.tag1.a.tag2.c.latitude| null|      root.ln|   FLOAT|   PLAIN|     SNAPPY|null|      null|
|root.ln.tag1.a.tag2.c.longitude| null|      root.ln|   FLOAT|   PLAIN|     SNAPPY|null|      null|
+-------------------------------+-----+-------------+--------+--------+-----------+----+----------+

IoTDB> show timeseries root.ln.tag2.c.**
+-------------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                     timeseries|alias|database|dataType|encoding|compression|tags|attributes|
+-------------------------------+-----+-------------+--------+--------+-----------+----+----------+
| root.ln.tag1.a.tag2.c.latitude| null|      root.ln|   FLOAT|   PLAIN|     SNAPPY|null|      null|
|root.ln.tag1.a.tag2.c.longitude| null|      root.ln|   FLOAT|   PLAIN|     SNAPPY|null|      null|
+-------------------------------+-----+-------------+--------+--------+-----------+----+----------+

IoTDB> show timeseries root.ln.tag2.b.**
+----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                  timeseries|alias|database|dataType|encoding|compression|tags|attributes|
+----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.tag1.a.tag2.b.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+----------------------------+-----+-------------+--------+--------+-----------+----+----------+
```

### insert

- insert a single column of data

```sql
IoTDB> insert into root.ln.tag2.d(timestamp,status) values(1,true)
Msg: The statement is executed successfully.
IoTDB> insert into root.ln.tag2.d(timestamp,status) values(2,false)
Msg: The statement is executed successfully.
IoTDB> insert into root.ln.tag2.d(timestamp,status) values(3,true)
Msg: The statement is executed successfully.
IoTDB> insert into root.ln.tag1.a.tag2.d(timestamp,status) values(1,true)
Msg: The statement is executed successfully.
```

- insert alignment data

```sql
IoTDB> insert into root.sg1.tag1.a(time, s1, s2) aligned values(2, 2, 2), (3, 3, 3)
Msg: The statement is executed successfully.
```

### select

- point query

```sql
IoTDB> select * from root.sg1.tag1.a
+-----------------------------+------------------+------------------+
|                         Time|root.sg1.tag1.a.s1|root.sg1.tag1.a.s2|
+-----------------------------+------------------+------------------+
|1970-01-01T08:00:00.002+08:00|               2.0|               2.0|
|1970-01-01T08:00:00.003+08:00|               3.0|               3.0|
+-----------------------------+------------------+------------------+
```

- align by device

```sql
IoTDB> select * from root.sg1.tag1.a align by device
+-----------------------------+---------------+---+---+
|                         Time|         Device| s1| s2|
+-----------------------------+---------------+---+---+
|1970-01-01T08:00:00.002+08:00|root.sg1.tag1.a|2.0|2.0|
|1970-01-01T08:00:00.003+08:00|root.sg1.tag1.a|3.0|3.0|
+-----------------------------+---------------+---+---+
```

- batch query

```sql
IoTDB> select status from root.ln.tag2.d.** where time < 2017-11-01T00:08:00.000
+-----------------------------+----------------------------+---------------------+
|                         Time|root.ln.tag1.a.tag2.d.status|root.ln.tag2.d.status|
+-----------------------------+----------------------------+---------------------+
|1970-01-01T08:00:00.001+08:00|                        true|                 true|
|1970-01-01T08:00:00.002+08:00|                        null|                false|
|1970-01-01T08:00:00.003+08:00|                        null|                 true|
+-----------------------------+----------------------------+---------------------+
```