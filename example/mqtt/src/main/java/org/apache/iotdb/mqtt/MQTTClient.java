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

  private static final String DATABASE = "myMqttTest";

  public static void main(String[] args) throws Exception {
    MQTT mqtt = new MQTT();
    mqtt.setHost("127.0.0.1", 1883);
    mqtt.setUserName("root");
    mqtt.setPassword("root");
    mqtt.setConnectAttemptsMax(3);
    mqtt.setReconnectDelay(10);

    BlockingConnection connection = mqtt.blockingConnection();
    connection.connect();
    // the config mqttPayloadFormatter must be tree-json
    // jsonPayloadFormatter(connection);
    // the config mqttPayloadFormatter must be table-line
    linePayloadFormatter(connection);
    connection.disconnect();
  }

  private static void jsonPayloadFormatter(BlockingConnection connection) throws Exception {
    Random random = new Random();
    StringBuilder sb = new StringBuilder();
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
      sb.append(payload).append(",");

      // publish a json object
      Thread.sleep(1);
      connection.publish("root.sg.d1.s1", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
    }
    // publish a json array
    sb.insert(0, "[");
    sb.replace(sb.lastIndexOf(","), sb.length(), "]");
    connection.publish("root.sg.d1.s1", sb.toString().getBytes(), QoS.AT_LEAST_ONCE, false);
  }

  // The database must be created in advance
  private static void linePayloadFormatter(BlockingConnection connection) throws Exception {
    // myTable,tag1=t1,tag2=t2 fieldKey1="1,2,3" 1740109006001
    String payload = "myTable,tag1=t1,tag2=t2 fieldKey1=\"1,2,3\" 1740109006001";
    connection.publish(DATABASE + "/myTopic", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
    Thread.sleep(10);

    payload = "myTable,tag1=t1,tag2=t2 fieldKey1=\"1,2,3\" 1740109006002";
    connection.publish(DATABASE + "/myTopic", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
    Thread.sleep(10);

    payload = "myTable,tag1=t1,tag2=t2 fieldKey1=\"1,2,3\" 1740109006003";
    connection.publish(DATABASE + "/myTopic", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
    Thread.sleep(10);
    payload =
        "test1,tag1=t1,tag2=t2 attr3=a5,attr4=a4 field1=\"fieldValue1\",field2=1i,field3=1u 1";
    connection.publish(DATABASE + "/myTopic", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
    Thread.sleep(10);

    payload = "test1,tag1=t1,tag2=t2  field4=2,field5=2i32,field6=2f 2";
    connection.publish(DATABASE, payload.getBytes(), QoS.AT_LEAST_ONCE, false);
    Thread.sleep(10);

    payload =
        "test1,tag1=t1,tag2=t2  field7=t,field8=T,field9=true 3 \n "
            + "test1,tag1=t1,tag2=t2  field7=f,field8=F,field9=FALSE 4";
    connection.publish(DATABASE + "/myTopic", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
    Thread.sleep(10);

    payload =
        "test1,tag1=t1,tag2=t2 attr1=a1,attr2=a2 field1=\"fieldValue1\",field2=1i,field3=1u 4 \n "
            + "test1,tag1=t1,tag2=t2 field4=2,field5=2i32,field6=2f 5";
    connection.publish(DATABASE + "/myTopic", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
    Thread.sleep(10);

    payload = "# It's a remark\n " + "test1,tag1=t1,tag2=t2 field4=2,field5=2i32,field6=2f 6";
    connection.publish(DATABASE + "/myTopic", payload.getBytes(), QoS.AT_LEAST_ONCE, false);
    Thread.sleep(10);
  }
}
