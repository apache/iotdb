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



# What's Zeppelin

Zeppelin is a web-based notebook that enables interactive data analytics. You can connect to data sources and perform interactive operations with SQL, Scala, etc. The operations can be saved as documents, just like Jupyter. Zeppelin has already supported many data sources, including Spark, ElasticSearch, Cassandra, and InfluxDB. Now, we have enabled Zeppelin to operate IoTDB via SQL. 

![image-20201124145203621](https://tva1.sinaimg.cn/large/0081Kckwly1gl09mzzfhzj314q0q70xo.jpg)



# IoTDB zeppelin-interpreter

## Environment

To use IoTDB and Zeppelin, you need to have:

1. Java >= 1.8.0_271 (Please make sure the environment path has been set. You can run `java -version` to confirm the current Java version.
2. IoTDB >= 0.11.0-SNAPSHOT.
3. Zeppelin >= 0.8.2

You can install IoTDB according to [IoTDB Quick Start](http://iotdb.apache.org/UserGuide/V0.10.x/Get%20Started/QuickStart.html). IoTDB is placed at `$IoTDB_HOME`.

You can download [Zeppelin](https://zeppelin.apache.org/download.html#) and unpack the binary package directly or [build Zeppelin from source](https://zeppelin.apache.org/docs/latest/setup/basics/how_to_build.html). Zeppelin is placed at `$Zeppelin_HOME`.



## Build interpreter

```shell
cd $IoTDB_HOME
mvn clean package -pl zeppelin-interpreter -am -DskipTests
```

The interpreter will be in the folder:

```shell
$IoTDB_HOME/zeppelin-interpreter/target/zeppelin-{version}-SNAPSHOT-jar-with-dependencies.jar
```



## Install interpreter

Once you have built your interpreter, create a new folder under the Zeppelin interpreter directory and put the built interpreter into it. 

```shell
mkdir $Zeppelin_HOME/interpreter/iotdb
cp $IoTDB_HOME/zeppelin-interpreter/target/zeppelin-{version}-SNAPSHOT-jar-with-dependencies.jar $Zeppelin_HOME/interpreter/iotdb
```



##Configure your interpreter

To configure your interpreter you need to follow these steps:

1. If it's the first start, create `conf/zeppelin-site.xml` by copying `conf/zeppelin-site.xml.template` to `conf/zeppelin-site.xml`.

2. Append your interpreter class name to  `zeppelin.interpreters` property in `conf/zeppelin-site.xml`, for example:

    ```xml
    <configuration>
      ...
      <property>
        <name>zeppelin.interpreters</name>
        <value>org.apache.zeppelin.spark.SparkInterpreter,org.apache.zeppelin.spark.PySparkInterpreter,org.apache.zeppelin.spark.SparkSqlInterpreter,org.apache.zeppelin.spark.DepInterpreter,org.apache.zeppelin.markdown.Markdown,org.apache.zeppelin.shell.ShellInterpreter,org.apache.zeppelin.hive.HiveInterpreter,org.apache.iotdb.zeppelin.IoTDBInterpreter</value>
      </property>
      ...
    </configuration>
    ```

## Running Zeppelin and IoTDB

Go to `$Zeppelin_HOME` and start Zeppelin by running: 

```shell
./bin/zeppelin-daemon.sh start
```

or in Windows:
```shell
./bin/zeppelin.cmd
```

Go to `$IoTDB_HOME` and [start IoTDB server](https://github.com/apache/iotdb#start-iotdb):

```shell
# Unix/OS X
> nohup sbin/start-server.sh >/dev/null 2>&1 &
or
> nohup sbin/start-server.sh -c <conf_path> -rpc_port <rpc_port> >/dev/null 2>&1 &

# Windows
> sbin\start-server.bat -c <conf_path> -rpc_port <rpc_port>
```

## Zeppelin to IoTDB

Wait for Zeppelin server to start, then visit http://localhost:8080/

1. In the interpreter page: 1.click the `Create new node` button, 2. set the note name and 3. configure your interpreter. Now you are ready to use your interpreter.

![image-20201123112330976](https://tva1.sinaimg.cn/large/0081Kckwly1gl09n7cpibj30sz0opagn.jpg)

# Use IoTDB-Zeppelin

We provide some simple SQL to show the use of Zeppelin-IoTDB-interpreter:

```sql
SET STORAGE GROUP TO root.ln.wf01.wt01;
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

![image-20201124142635276](https://tva1.sinaimg.cn/large/0081Kckwly1gl09nd3vimj30pj0nz0w3.jpg)

You can also design more fantasy documents referring to [[1]](https://zeppelin.apache.org/docs/0.9.0-SNAPSHOT/usage/display_system/basic.html) and others.

The above demo notebook can be found at `./IoTDB-Zeppelin-Demo.zpln`.





