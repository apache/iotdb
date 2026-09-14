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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.MqttClientSession;

import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class PayloadFormatterContextTest {

  private static int originalDataNodeId;

  @BeforeClass
  public static void setUp() {
    originalDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
  }

  @AfterClass
  public static void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(originalDataNodeId);
  }

  @Test
  public void delegatesToPayloadOnlyFormatter() {
    ByteBuf payload = Unpooled.copiedBuffer("value", StandardCharsets.UTF_8);
    List<Message> expected = Collections.singletonList(new TreeMessage());
    PayloadFormatter formatter =
        new TestFormatter() {
          @Override
          public List<Message> format(ByteBuf actual) {
            assertSame(payload, actual);
            return expected;
          }
        };
    try {
      assertSame(expected, formatter.formatMessage(publication(payload)));
      assertEquals(1, payload.refCnt());
    } finally {
      payload.release();
    }
  }

  @Test
  public void delegatesToTopicAwareFormatter() {
    ByteBuf payload = Unpooled.copiedBuffer("value", StandardCharsets.UTF_8);
    PayloadFormatter formatter =
        new TestFormatter() {
          @Override
          public List<Message> format(String topic, ByteBuf actual) {
            assertEquals("sensors/temperature", topic);
            assertSame(payload, actual);
            return null;
          }
        };
    try {
      assertNull(formatter.formatMessage(publication(payload)));
      assertEquals(1, payload.refCnt());
    } finally {
      payload.release();
    }
  }

  @Test
  public void handlerPassesPublicationAndReleasesPayload() throws Exception {
    checkHandlerDispatch(false);
  }

  @Test
  public void handlerReleasesPayloadWhenFormatterFails() throws Exception {
    checkHandlerDispatch(true);
  }

  private void checkHandlerDispatch(boolean fail) throws Exception {
    AtomicReference<InterceptPublishMessage> received = new AtomicReference<>();
    PayloadFormatter formatter =
        new TestFormatter() {
          @Override
          public List<Message> formatMessage(InterceptPublishMessage message) {
            received.set(message);
            if (fail) {
              throw new IllegalArgumentException("invalid custom payload");
            }
            return Collections.emptyList();
          }
        };
    MPPPublishHandler handler = new MPPPublishHandler(IoTDBDescriptor.getInstance().getConfig());
    Field formatterField = MPPPublishHandler.class.getDeclaredField("payloadFormat");
    formatterField.setAccessible(true);
    formatterField.set(handler, formatter);
    Field sessionsField = MPPPublishHandler.class.getDeclaredField("clientIdToSessionMap");
    sessionsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<String, MqttClientSession> sessions =
        (Map<String, MqttClientSession>) sessionsField.get(handler);
    sessions.put("sensor-1", new MqttClientSession("sensor-1"));

    ByteBuf payload = Unpooled.copiedBuffer("value", StandardCharsets.UTF_8);
    InterceptPublishMessage publication = publication(payload);
    try {
      handler.onPublish(publication);
      assertSame(publication, received.get());
      assertEquals(0, payload.refCnt());
    } finally {
      if (payload.refCnt() > 0) {
        payload.release();
      }
    }
  }

  private static InterceptPublishMessage publication(ByteBuf payload) {
    return new InterceptPublishMessage(
        new MqttPublishMessage(
            new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0),
            new MqttPublishVariableHeader("sensors/temperature", 1),
            payload),
        "sensor-1",
        "alice");
  }

  private abstract static class TestFormatter implements PayloadFormatter {
    @Override
    public List<Message> format(ByteBuf payload) {
      throw new AssertionError("The payload-only callback must not be called");
    }

    @Override
    public String getName() {
      return "context-test";
    }

    @Override
    public String getType() {
      return TREE_TYPE;
    }
  }
}
