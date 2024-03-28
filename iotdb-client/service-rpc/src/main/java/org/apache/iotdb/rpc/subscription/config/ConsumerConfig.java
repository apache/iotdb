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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

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

  public void serialize(DataOutputStream stream) throws IOException {
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
}
