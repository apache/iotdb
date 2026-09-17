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

import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.buffer.ByteBuf;

import java.util.List;

/**
 * PayloadFormatter format the payload to the messages.
 *
 * <p>This is a SPI interface.
 *
 * @see JSONPayloadFormatter
 */
public interface PayloadFormatter {

  public static final String TREE_TYPE = "tree";
  public static final String TABLE_TYPE = "table";

  /**
   * format a payload to a list of messages
   *
   * @param payload
   * @return
   */
  @Deprecated
  List<Message> format(ByteBuf payload);

  /**
   * format a payload of a topic to a list of messages
   *
   * @param topic
   * @param payload
   * @return
   */
  default List<Message> format(String topic, ByteBuf payload) {
    return format(payload);
  }

  /**
   * Formats a publication, including its client ID, username, QoS, topic and payload.
   *
   * <p>The default implementation delegates to {@link #format(String, ByteBuf)}, preserving
   * existing formatters. Override this method when the publication metadata is needed to construct
   * messages.
   *
   * <p>The broker releases the payload after publication handling. Implementations must not release
   * it or keep it for later use without retaining their own reference.
   *
   * @param message the publication received by the MQTT broker
   * @return parsed messages, or {@code null} to ignore the publication
   */
  default List<Message> formatMessage(InterceptPublishMessage message) {
    return format(message.getTopicName(), message.getPayload());
  }

  /**
   * get the formatter name
   *
   * @return
   */
  String getName();

  String getType();
}
