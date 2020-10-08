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
# MQTT协议

[MQTT](http://mqtt.org/)是机器对机器（M2M）/“物联网”连接协议。

它被设计为一种非常轻量级的发布/订阅消息传递。

对于与需要较小代码占用和/或网络带宽非常宝贵的远程位置的连接很有用。

IoTDB支持MQTT v3.1（OASIS标准）协议。
IoTDB服务器包括内置的MQTT服务，该服务允许远程设备将消息直接发送到IoTDB服务器。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/6711230/78357432-0c71cf80-75e4-11ea-98aa-c43a54d469ce.png">


## 内置MQTT服务
内置的MQTT服务提供了通过MQTT直接连接到IoTDB的能力。 它侦听来自MQTT客户端的发布消息，然后立即将数据写入存储。
MQTT主题与IoTDB时间序列相对应。
消息有效载荷可以由Java SPI加载的`PayloadFormatter`格式化为事件，默认实现为`JSONPayloadFormatter` 
   默认的`json`格式化程序支持两种json格式，以下是MQTT消息有效负载示例：

```json
 {
      "device":"root.sg.d1",
      "timestamp":1586076045524,
      "measurements":["s1","s2"],
      "values":[0.530635,0.530635]
 }
```
或者
```json
{
      "device":"root.sg.d1",
      "timestamps":[1586076045524,1586076065526],
      "measurements":["s1","s2"],
      "values":[[0.530635,0.530635], [0.530655,0.530695]]
  }
```

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/6711230/78357469-1bf11880-75e4-11ea-978f-a53996667a0d.png">

## MQTT配置
默认情况下，IoTDB MQTT服务从`${IOTDB_HOME}/${IOTDB_CONF}/iotdbengine.properties`加载配置。

配置如下：

| 名称      | 描述         | 默认 |
| ------------- |:-------------:|:------:|
| enable_mqtt_service      | 是否启用mqtt服务 | false |
| mqtt_host      | mqtt服务绑定主机 | 0.0.0.0 |
| mqtt_port      | mqtt服务绑定端口 |   1883 |
| mqtt_handler_pool_size | 处理mqtt消息的处理程序池大小 |    1 |
| mqtt_payload_formatter | mqtt消息有效负载格式化程序 |    json |
| mqtt_max_message_size | mqtt消息最大长度（字节）|   1048576 |

## 例子
以下是mqtt客户端将消息发送到IoTDB服务器的示例。

 ```java
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", 1883);
        mqtt.setUserName("root");
        mqtt.setPassword("root");

        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            String payload = String.format("{\n" +
                    "\"device\":\"root.sg.d1\",\n" +
                    "\"timestamp\":%d,\n" +
                    "\"measurements\":[\"s1\"],\n" +
                    "\"values\":[%f]\n" +
                    "}", System.currentTimeMillis(), random.nextDouble());

            connection.publish("root.sg.d1.s1", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
        }

        connection.disconnect();
    }
 ```
