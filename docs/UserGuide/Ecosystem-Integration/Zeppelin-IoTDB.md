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

## Zeppelin-IoTDB

### About Zeppelin

Zeppelin is a web-based notebook that enables interactive data analytics. You can connect to data sources and perform interactive operations with SQL, Scala, etc. The operations can be saved as documents, just like Jupyter. Zeppelin has already supported many data sources, including Spark, ElasticSearch, Cassandra, and InfluxDB. Now, we have enabled Zeppelin to operate IoTDB via SQL. 

![iotdb-note-snapshot](https://alioss.timecho.com/docs/img/github/102752947-520a3e80-43a5-11eb-8fb1-8fac471c8c7e.png)



### Zeppelin-IoTDB Interpreter

#### System Requirements

| IoTDB Version | Java Version  | Zeppelin Version |
| :-----------: | :-----------: | :--------------: |
|  >=`0.12.0`   | >=`1.8.0_271` |    `>=0.9.0`     |

Install IoTDB: Reference to [IoTDB Quick Start](../QuickStart/QuickStart.html). Suppose IoTDB is placed at `$IoTDB_HOME`.

Install Zeppelin:
> Method A. Download directly: You can download [Zeppelin](https://zeppelin.apache.org/download.html#) and unpack the binary package. [netinst](http://www.apache.org/dyn/closer.cgi/zeppelin/zeppelin-0.9.0/zeppelin-0.9.0-bin-netinst.tgz) binary package is recommended since it's relatively small by excluding irrelevant interpreters.
> 
> Method B. Compile from source code: Reference to [build Zeppelin from source](https://zeppelin.apache.org/docs/latest/setup/basics/how_to_build.html). The command is `mvn clean package -pl zeppelin-web,zeppelin-server -am -DskipTests`.

Suppose Zeppelin is placed at `$Zeppelin_HOME`.

#### Build Interpreter

```
 cd $IoTDB_HOME
 mvn clean package -pl zeppelin-interpreter -am -DskipTests -P get-jar-with-dependencies
```

The interpreter will be in the folder:

```
 $IoTDB_HOME/zeppelin-interpreter/target/zeppelin-{version}-SNAPSHOT-jar-with-dependencies.jar
```



#### Install Interpreter

Once you have built your interpreter, create a new folder under the Zeppelin interpreter directory and put the built interpreter into it. 

```
 cd $IoTDB_HOME
 mkdir -p $Zeppelin_HOME/interpreter/iotdb
 cp $IoTDB_HOME/zeppelin-interpreter/target/zeppelin-{version}-SNAPSHOT-jar-with-dependencies.jar $Zeppelin_HOME/interpreter/iotdb
```



#### Running Zeppelin and IoTDB

Go to `$Zeppelin_HOME` and start Zeppelin by running: 

```
 ./bin/zeppelin-daemon.sh start
```

or in Windows:

```
 .\bin\zeppelin.cmd
```

Go to `$IoTDB_HOME` and start IoTDB server:

```
 # Unix/OS X
 > nohup sbin/start-server.sh >/dev/null 2>&1 &
 or
 > nohup sbin/start-server.sh -c <conf_path> -rpc_port <rpc_port> >/dev/null 2>&1 &
 
 # Windows
 > sbin\start-server.bat -c <conf_path> -rpc_port <rpc_port>
```



### Use Zeppelin-IoTDB

Wait for Zeppelin server to start, then visit http://127.0.0.1:8080/

In the interpreter page: 

1. Click the `Create new node` button
2. Set the note name
3. Configure your interpreter

Now you are ready to use your interpreter.

![iotdb-create-note](https://alioss.timecho.com/docs/img/github/102752945-5171a800-43a5-11eb-8614-53b3276a3ce2.png)

We provide some simple SQL to show the use of Zeppelin-IoTDB interpreter:

```sql
 CREATE DATABASE root.ln.wf01.wt01;
 CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN;
 CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN;
 CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN;
 
 INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)
 VALUES (1, 1.1, false, 11);
 
 INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)
 VALUES (2, 2.2, true, 22);
 
 INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)
 VALUES (3, 3.3, false, 33);
 
 INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)
 VALUES (4, 4.4, false, 44);
 
 INSERT INTO root.ln.wf01.wt01 (timestamp, temperature, status, hardware)
 VALUES (5, 5.5, false, 55);
 
 
 SELECT *
 FROM root.ln.wf01.wt01
 WHERE time >= 1
   AND time <= 6;
```

The screenshot is as follows:

![iotdb-note-snapshot2](https://alioss.timecho.com/docs/img/github/102752948-52a2d500-43a5-11eb-9156-0c55667eb4cd.png)

You can also design more fantasy documents referring to [[1]](https://zeppelin.apache.org/docs/0.9.0/usage/display_system/basic.html) and others.

The above demo notebook can be found at  `$IoTDB_HOME/zeppelin-interpreter/Zeppelin-IoTDB-Demo.zpln`.



### Configuration

You can configure the connection parameters in http://127.0.0.1:8080/#/interpreter :

![iotdb-configuration](https://alioss.timecho.com/docs/img/github/102752940-50407b00-43a5-11eb-94fb-3e3be222183c.png)

The parameters you can configure are as follows:

| Property                     | Default   | Description                     |
| ---------------------------- | --------- | ------------------------------- |
| iotdb.host                   | 127.0.0.1 | IoTDB server host to connect to |
| iotdb.port                   | 6667      | IoTDB server port to connect to |
| iotdb.username               | root      | Username for authentication     |
| iotdb.password               | root      | Password for authentication     |
| iotdb.fetchSize              | 10000     | Query fetch size                |
| iotdb.zoneId                 |           | Zone Id                         |
| iotdb.enable.rpc.compression | FALSE     | Whether enable rpc compression  |
| iotdb.time.display.type      | default   | The time format to display      |

