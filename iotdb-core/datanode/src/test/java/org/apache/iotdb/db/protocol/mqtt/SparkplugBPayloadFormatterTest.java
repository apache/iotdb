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
package org.apache.iotdb.db.protocol.mqtt;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class SparkplugBPayloadFormatterTest {

  @Test
  public void formatJson() {
    String topic = "spBv1.0/Group ID/Message Type/Edge Node ID/Device ID";
    String payload =
        " {\n"
            + "    \"timestamp\": 1486144502122,\n"
            + "    \"metrics\": [{\n"
            + "        \"name\": \"My Metric\",\n"
            + "        \"alias\": 1,\n"
            + "        \"timestamp\": 1479123452194,\n"
            + "        \"dataType\": \"String\",\n"
            + "        \"value\": \"Test\"\n"
            + "    }],\n"
            + "    \"seq\": 2\n"
            + "}";

    ByteBuf buf = Unpooled.copiedBuffer(payload, StandardCharsets.UTF_8);

    SparkplugBPayloadFormatter formatter = new SparkplugBPayloadFormatter();
    Message message = formatter.format(topic, buf).get(0);

    assertEquals("root.`Group ID`.`Message Type`.`Edge Node ID`.`Device ID`", message.getDevice());
    assertEquals(Long.valueOf(1479123452194L), message.getTimestamp());
    assertEquals("My Metric", message.getMeasurements().get(0));
    assertEquals(TSDataType.TEXT, message.getDataTypes().get(0));
    assertEquals("Test", message.getValues().get(0));
  }
}
