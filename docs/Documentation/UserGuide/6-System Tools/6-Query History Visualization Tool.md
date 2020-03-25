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

# Query History Visualization Tool

IoTDB Query History Visualization Tool uses a monitoring web page to provide metrics service for viewing the query history and SQL execution time. It can also provide the memory and CPU usage of the current host.

The port of IoTDB Query History Visualization Tool is `8181`. Just print your `ip:8181` in your browser, and you can view page like this:

<img style="width:100%; max-width:800px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/65688727-3038e380-e09e-11e9-8266-24ff0a1efa96.png">

> Note: Currently, we only support showing CPU ratio of Windows and Linux os. If you are using other OS, you may get a warning information: "Can't get the cpu ratio, because this OS is not support".
