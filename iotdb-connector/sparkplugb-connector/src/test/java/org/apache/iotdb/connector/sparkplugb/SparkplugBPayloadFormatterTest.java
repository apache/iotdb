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
package org.apache.iotdb.connector.sparkplugb;

import org.apache.iotdb.db.protocol.mqtt.Message;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.eclipse.tahu.protobuf.SparkplugBProto;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SparkplugBPayloadFormatterTest {

  @Test
  public void formatJson() {
    String topic = "spBv1.0/Group ID/Message Type/Edge Node ID/Device ID";
    SparkplugBProto.Payload payload =
        SparkplugBProto.Payload.newBuilder()
            .setTimestamp(1479123452194L)
            .setSeq(2L)
            .addMetrics(
                SparkplugBProto.Payload.Metric.newBuilder()
                    .setName("My Metric")
                    .setAlias(1)
                    .setTimestamp(1479123452194L)
                    .setDatatype(SparkplugBProto.DataType.String.getNumber())
                    .setStringValue("Test")
                    .build())
            .build();
    ByteBuf buf = Unpooled.copiedBuffer(payload.toByteArray());

    SparkplugBPayloadFormatter formatter = new SparkplugBPayloadFormatter();
    Message message = formatter.format(topic, buf).get(0);

    assertEquals("root.`Group ID`.`Message Type`.`Edge Node ID`.`Device ID`", message.getDevice());
    assertEquals(Long.valueOf(1479123452194L), message.getTimestamp());
    assertEquals("My Metric", message.getMeasurements().get(0));
    assertEquals(TSDataType.TEXT, message.getDataTypes().get(0));
    assertEquals("Test", message.getValues().get(0));
  }
}
