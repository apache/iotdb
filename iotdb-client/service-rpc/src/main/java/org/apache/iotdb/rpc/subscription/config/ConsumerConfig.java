/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.rpc.subscription.config;

import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class ConsumerConfig extends PipeParameters {

  public ConsumerConfig() {
    super(Collections.emptyMap());
  }

  public ConsumerConfig(Map<String, String> attributes) {
    super(attributes);
  }

  /////////////////////////////// de/ser ///////////////////////////////

  public static ByteBuffer serialize(ConsumerConfig consumerConfig) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      consumerConfig.serialize(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(attributes, stream);
  }

  public static ConsumerConfig deserialize(ByteBuffer buffer) {
    return new ConsumerConfig(ReadWriteIOUtils.readMap(buffer));
  }

  /////////////////////////////// utilities ///////////////////////////////

  public String getConsumerId() {
    return getString(ConsumerConstant.CONSUMER_ID_KEY);
  }

  public String getConsumerGroupId() {
    return getString(ConsumerConstant.CONSUMER_GROUP_ID_KEY);
  }

  public String getUsername() {
    return getString(ConsumerConstant.USERNAME_KEY);
  }

  public String getPassword() {
    return getString(ConsumerConstant.PASSWORD_KEY);
  }

  public String getSqlDialect() {
    return getString(ConsumerConstant.SQL_DIALECT_KEY);
  }

  public void setConsumerId(final String consumerId) {
    attributes.put(ConsumerConstant.CONSUMER_ID_KEY, consumerId);
  }

  public void setConsumerGroupId(final String consumerGroupId) {
    attributes.put(ConsumerConstant.CONSUMER_GROUP_ID_KEY, consumerGroupId);
  }
}
