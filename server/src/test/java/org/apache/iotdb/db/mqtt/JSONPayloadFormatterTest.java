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
package org.apache.iotdb.db.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class JSONPayloadFormatterTest {

  @Test
  public void formatJson() {
    String payload =
        " {\n"
            + "      \"device\":\"root.sg.d1\",\n"
            + "      \"timestamp\":1586076045524,\n"
            + "      \"measurements\":[\"s1\",\"s2\"],\n"
            + "      \"values\":[0.530635,0.530635]\n"
            + " }";

    ByteBuf buf = Unpooled.copiedBuffer(payload, StandardCharsets.UTF_8);

    JSONPayloadFormatter formatter = new JSONPayloadFormatter();
    Message message = formatter.format(buf).get(0);

    assertEquals("root.sg.d1", message.getDevice());
    assertEquals(Long.valueOf(1586076045524L), message.getTimestamp());
    assertEquals("s1", message.getMeasurements().get(0));
    assertEquals(0.530635D, Double.parseDouble(message.getValues().get(0)), 0);
  }

  @Test
  public void formatBatchJson() {
    String payload =
        " {\n"
            + "      \"device\":\"root.sg.d1\",\n"
            + "      \"timestamps\":[1586076045524,1586076065526],\n"
            + "      \"measurements\":[\"s1\",\"s2\"],\n"
            + "      \"values\":[[0.530635,0.530635], [0.530655,0.530695]]\n"
            + "  }";

    ByteBuf buf = Unpooled.copiedBuffer(payload, StandardCharsets.UTF_8);

    JSONPayloadFormatter formatter = new JSONPayloadFormatter();
    Message message = formatter.format(buf).get(1);

    assertEquals("root.sg.d1", message.getDevice());
    assertEquals(Long.valueOf(1586076065526L), message.getTimestamp());
    assertEquals("s2", message.getMeasurements().get(1));
    assertEquals(0.530695D, Double.parseDouble(message.getValues().get(1)), 0);
  }
}
