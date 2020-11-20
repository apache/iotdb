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

# IoTDB zeppelin-interpreter

## Build interpreter
Under the root path of iotdb:

> mvn clean package -pl zeppelin-interpreter -am -DskipTests

After build, the zeppelin-interpreter will be in the folder "[IOTDB_HOME]/zeppelin-interpreter/target/zeppelin-iotdb-{version}-jar-with-dependencies.jar"

## Install interpreter
Once you have built your interpreter, create a new iotdb folder under the interpreter directory. Then place the .jar file under this directory,i.e. [ZEPPELIN_HOME]/interpreter/iotdb/

##Configure your interpreter
To configure your interpreter you need to follow these steps:

1. Add your interpreter class name to the zeppelin.interpreters property in *[ZEPPELIN_HOME]/conf/zeppelin-site.xml.*

    Property value is comma separated [INTERPRETER_CLASS_NAME]. For example,
    ```
    <property>
    <name>zeppelin.interpreters</name>
    <value>org.apache.zeppelin.spark.SparkInterpreter,org.apache.zeppelin.spark.PySparkInterpreter,org.apache.zeppelin.spark.SparkSqlInterpreter,org.apache.zeppelin.spark.DepInterpreter,org.apache.zeppelin.markdown.Markdown,org.apache.zeppelin.shell.ShellInterpreter,org.apache.zeppelin.hive.HiveInterpreter,org.apache.zeppelin.iotdb.IoTDBInterpreter</value>
    </property>
    ```

2. Start Zeppelin by running 
    >./bin/zeppelin-daemon.sh start
    
    or in Windows:
    >./bin/zeppelin.cmd
    
    Wait for Zeppelin server to start, then visit http://localhost:8080/
3. In the interpreter page, click the *+Create* button and configure your interpreter properties. Now you are done and ready to use your interpreter.
