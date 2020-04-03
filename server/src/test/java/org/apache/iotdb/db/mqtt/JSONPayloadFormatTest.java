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

import com.alibaba.fastjson.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class JSONPayloadFormatTest {

    @Test
    public void format() {
        Map<String,Object> tuple = new HashMap();
        tuple.put("device", "root.sg.d1");
        tuple.put("timestamp", System.currentTimeMillis());
        tuple.put("measurements", "s1");
        tuple.put("values", 36.51D);
        String payload = JSON.toJSONString(tuple);
        ByteBuf buf = Unpooled.copiedBuffer(payload, StandardCharsets.UTF_8);

        JSONPayloadFormatter formatter = new JSONPayloadFormatter();
        Message message = formatter.format(buf);

        assertEquals(tuple.get("device"), message.getDevice());
        assertEquals(tuple.get("timestamp"), message.getTimestamp());
        assertEquals(tuple.get("measurements"), message.getMeasurements().get(0));
        assertEquals(tuple.get("values"), Double.parseDouble(message.getValues().get(0)));
    }
}
