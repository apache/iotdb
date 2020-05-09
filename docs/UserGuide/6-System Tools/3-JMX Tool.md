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
1. Make sure jdk is installed
2. Open jvisualvm
3. Right-click at the left navigation area -> Add JMX connection
<img style="width:100%; max-width:400px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/81462914-5738c580-91e8-11ea-94d1-4ff6607e7e2c.png">

4. Fill in information and log in as below:
<img style="width:100%; max-width:400px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/81462914-5738c580-91e8-11ea-94d1-4ff6607e7e2c.png">

