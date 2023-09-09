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

# Sparkplug B 1.0 Connector

## Installing the plugin

In the Apache IoTDB directory, create a directory `ext/mqtt`.

Copy the `jar` of thit plugin into this directory.

Edit the `conf/iotdb-common.properties` file to contain the following configuration: 

    ####################
    ### MQTT Broker Configuration
    ####################
    
    # whether to enable the mqtt service.
    # Datatype: boolean
    enable_mqtt_service=true
    
    # the mqtt service binding host.
    # Datatype: String
    mqtt_host=127.0.0.1
    
    # the mqtt service binding port.
    # Datatype: int
    mqtt_port=1883
    
    # the handler pool size for handing the mqtt messages.
    # Datatype: int
    mqtt_handler_pool_size=1
    
    # the mqtt message payload formatter.
    # Datatype: String
    mqtt_payload_formatter=spB1.0
    
    # max length of mqtt message in byte
    # Datatype: int
    mqtt_max_message_size=1048576
