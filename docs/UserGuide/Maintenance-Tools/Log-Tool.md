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

## System log

IoTDB allows users to configure IoTDB system logs (such as log output level) by modifying the log configuration file. The default location of the system log configuration file is in \$IOTDB_HOME/conf folder. 

The default log configuration file is named logback.xml. The user can modify the configuration of the system running log by adding or changing the xml tree node parameters. It should be noted that the configuration of the system log using the log configuration file does not take effect immediately after the modification, instead, it will take effect after restarting the system. The usage of logback.xml is just as usual.

At the same time, in order to facilitate the debugging of the system by the developers and DBAs, we provide several JMX interfaces to dynamically modify the log configuration, and configure the Log module of the system in real time without restarting the system.

### Dynamic System Log Configuration

#### Connect JMX

Here we use JConsole to connect with JMX. 

Start the JConsole, establish a new JMX connection with the IoTDB Server (you can select the local process or input the IP and PORT for remote connection, the default operation port of the IoTDB JMX service is 31999). Fig 4.1 shows the connection GUI of JConsole.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/51577195-f94d7500-1ef3-11e9-999a-b4f67055d80e.png">

After connected, click `MBean` and find `ch.qos.logback.classic.default.ch.qos.logback.classic.jmx.JMXConfigurator`(As shown in fig 4.2).
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/51577204-fe122900-1ef3-11e9-9e89-2eb1d46e24b8.png">

In the JMXConfigurator Window, there are 6 operations provided, as shown in fig 4.3. You can use these interfaces to perform operation.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="/img/github/51577216-09fdeb00-1ef4-11e9-9005-542ad7d9e9e0.png">

#### Interface Instruction

* reloadDefaultConfiguration

This method is to reload the default logback configuration file. The user can modify the default configuration file first, and then call this method to reload the modified configuration file into the system to take effect.

* reloadByFileName

This method loads a logback configuration file with the specified path and name, and then makes it take effect. This method accepts a parameter of type String named p1, which is the path to the configuration file that needs to be specified for loading.

* getLoggerEffectiveLevel

This method is to obtain the current log level of the specified Logger. This method accepts a String type parameter named p1, which is the name of the specified Logger. This method returns the log level currently in effect for the specified Logger.

* getLoggerLevel

This method is to obtain the log level of the specified Logger. This method accepts a String type parameter named p1, which is the name of the specified Logger. This method returns the log level of the specified Logger.
It should be noted that the difference between this method and the `getLoggerEffectiveLevel` method is that the method returns the log level that the specified Logger is set in the configuration file. If the user does not set the log level for the Logger, then return empty. According to Logger's log-level inheritance mechanism, a Logger's level is not explicitly set, it will inherit the log level settings from its nearest ancestor. At this point, calling the `getLoggerEffectiveLevel` method will return the log level in which the Logger is in effect; calling `getLoggerLevel` will return null.

* setLoggerLevel

This method sets the log level of the specified Logger. The method accepts a parameter of type String named p1 and a parameter of type String named p2, specifying the name of the logger and the log level of the target, respectively.