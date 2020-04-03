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
import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PublishHandlerTest {

    @Test
    public void onPublish() throws Exception {
        IPlanExecutor executor = mock(IPlanExecutor.class);
        PayloadFormatter payloadFormat = PayloadFormatManager.getPayloadFormat("json");
        PublishHandler handler = new PublishHandler(executor, payloadFormat);

        Map<String,Object> tuple = new HashMap();
        tuple.put("device", "root.sg.d1");
        tuple.put("timestamp", System.currentTimeMillis());
        tuple.put("measurements", "s1");
        tuple.put("values", 36.51D);
        String payload = JSON.toJSONString(tuple);
        ByteBuf buf = Unpooled.copiedBuffer(payload, StandardCharsets.UTF_8);

        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader("root.sg.d1", 1);
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 1);

        MqttPublishMessage publishMessage = new MqttPublishMessage(fixedHeader, variableHeader, buf);
        InterceptPublishMessage message = new InterceptPublishMessage(publishMessage, null, null);
        handler.onPublish(message);
        verify(executor).processNonQuery(any(InsertPlan.class));
    }
}