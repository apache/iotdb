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

# JMX Tool

Java VisualVM is a tool that provides a visual interface for viewing detailed information about Java applications while they are running on a Java Virtual Machine (JVM), and for troubleshooting and profiling these applications. 

## Usage

Step1: Fetch IoTDB-sever.

Step2: Edit configuration.

* IoTDB is LOCAL
View `$IOTDB_HOME/conf/jmx.password`, and use default user or add new users here.
If new users are added, remember to edit `$IOTDB_HOME/conf/jmx.access` and add new users' access

* IoTDB is not LOCAL
Edit `$IOTDB_HOME/conf/iotdb-env.sh`, and modify config below:
```
JMX_LOCAL="false"
JMX_IP="the_real_iotdb_server_ip"  # Write the actual IoTDB IP address
```
View `$IOTDB_HOME/conf/jmx.password`, and use default user or add new users here.
If new users are added, remember to edit `$IOTDB_HOME/conf/jmx.access` and add new users' access

Step 3: Start IoTDB-server.

Step 4: Use jvisualvm
1. Make sure jdk 8 is installed. For versions later than jdk 8, you need to [download visualvm](https://visualvm.github.io/download.html) 
2. Open jvisualvm
3. Right-click at the left navigation area -> Add JMX connection
<img style="width:100%; max-width:300px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/github/81464569-725e0200-91f5-11ea-9ff9-49745f4c9ef2.png">

4. Fill in information and log in as below. Remember to check "Do not require SSL connection".
An example is:
Connection：192.168.130.15:31999
Username：iotdb
Password：passw!d
<img style="width:100%; max-width:300px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/github/81464639-ed271d00-91f5-11ea-91a0-b4fe9cb8204e.png">

