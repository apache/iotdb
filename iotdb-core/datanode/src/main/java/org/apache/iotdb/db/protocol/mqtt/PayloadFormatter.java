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
   * get the formatter name
   *
   * @return
   */
  String getName();

  String getType();
}
