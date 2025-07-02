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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LinePayloadFormatterTest {

  @Test
  public void formatLine() {
    String payload =
        "test1,tag1=t1,tag2=t2 attr1=a1,attr2=a2 field1=\"value1\",field2=1i,field3=2u,field4=3i32,field5=t,field6=false,field7=4,field8=5f 1";

    ByteBuf buf = Unpooled.copiedBuffer(payload, StandardCharsets.UTF_8);
    String topic = "";

    LinePayloadFormatter formatter = new LinePayloadFormatter();
    TableMessage message = (TableMessage) formatter.format(topic, buf).get(0);

    assertEquals("test1", message.getTable());
    assertEquals(Long.valueOf(1L), message.getTimestamp());
    assertEquals("tag1", message.getTagKeys().get(0));
    assertEquals("attr1", message.getAttributeKeys().get(0));
    assertEquals(
        "value1",
        ((Binary[]) message.getValues().get(0))[0].getStringValue(StandardCharsets.UTF_8));
    assertEquals(1L, ((long[]) message.getValues().get(1))[0], 0);
    assertEquals(2L, ((long[]) message.getValues().get(2))[0], 0);
    assertEquals(3L, ((int[]) message.getValues().get(3))[0], 0);
    assertTrue(((boolean[]) message.getValues().get(4))[0]);
    assertFalse(((boolean[]) message.getValues().get(5))[0]);
    assertEquals(4d, ((double[]) message.getValues().get(6))[0], 0);
    assertEquals(5f, ((float[]) message.getValues().get(7))[0], 0);
  }

  @Test
  public void formatBatchLine() {
    String payload =
        "test1,tag1=t1,tag2=t2 attr1=a1,attr2=a2 field1=\"value1\",field2=1i,field3=1u 1 \n"
            + "test2,tag3=t3,tag4=t4 attr3=a3,attr4=a4 field4=\"value4\",field5=10i,field6=10i32 2 ";

    ByteBuf buf = Unpooled.copiedBuffer(payload, StandardCharsets.UTF_8);
    String topic = "";

    LinePayloadFormatter formatter = new LinePayloadFormatter();
    TableMessage message = (TableMessage) formatter.format(topic, buf).get(1);

    assertEquals("test2", message.getTable());
    assertEquals(Long.valueOf(2L), message.getTimestamp());
    assertEquals("tag3", message.getTagKeys().get(0));
    assertEquals("attr3", message.getAttributeKeys().get(0));
    assertEquals(10, ((int[]) message.getValues().get(2))[0], 0);
  }

  @Test
  public void formatLineAnnotation() {
    String payload =
        "test1,tag1=t1,tag2=t2 attr1=a1,attr2=a2 field1=\"value1\",field2=1i,field3=1u 1 \n"
            + " # test2,tag3=t3,tag4=t4 attr3=a3,attr4=a4 field4=\"value4\",field5=10i,field6=10i32 2 ";

    ByteBuf buf = Unpooled.copiedBuffer(payload, StandardCharsets.UTF_8);
    String topic = "";

    LinePayloadFormatter formatter = new LinePayloadFormatter();
    List<Message> message = formatter.format(topic, buf);

    assertEquals(1, message.size());
  }
}
