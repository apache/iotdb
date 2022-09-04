/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.mqtt;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

import java.util.Random;

public class MQTTClient {
  public static void main(String[] args) throws Exception {
    MQTT mqtt = new MQTT();
    mqtt.setHost("127.0.0.1", 1883);
    mqtt.setUserName("root");
    mqtt.setPassword("root");

    BlockingConnection connection = mqtt.blockingConnection();
    connection.connect();

    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      String payload =
          String.format(
              "{\n"
                  + "\"device\":\"root.sg.d1\",\n"
                  + "\"timestamp\":%d,\n"
                  + "\"measurements\":[\"s1\"],\n"
                  + "\"values\":[%f]\n"
                  + "}",
              System.currentTimeMillis(), random.nextDouble());

      Thread.sleep(1);
      connection.publish("root.sg.d1.s1", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
    }

    connection.disconnect();
  }
}
