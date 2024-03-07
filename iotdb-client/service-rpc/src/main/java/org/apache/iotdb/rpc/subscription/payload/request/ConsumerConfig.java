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

package org.apache.iotdb.rpc.subscription.payload.request;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class ConsumerConfig {

  private transient String consumerGroupID;
  private transient String consumerClientID;

  // TODO: more configs

  public String getConsumerClientID() {
    return consumerClientID;
  }

  public String getConsumerGroupID() {
    return consumerGroupID;
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(consumerGroupID, stream);
    ReadWriteIOUtils.write(consumerClientID, stream);
  }

  public static ConsumerConfig deserialize(ByteBuffer buffer) {
    final ConsumerConfig consumerConfig = new ConsumerConfig();
    consumerConfig.consumerGroupID = ReadWriteIOUtils.readString(buffer);
    consumerConfig.consumerClientID = ReadWriteIOUtils.readString(buffer);
    return consumerConfig;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ConsumerConfig that = (ConsumerConfig) obj;
    return Objects.equals(this.consumerGroupID, that.consumerGroupID)
        && Objects.equals(this.consumerClientID, that.consumerClientID);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consumerGroupID, consumerClientID);
  }

  @Override
  public String toString() {
    return "ConsumerConfig{"
        + "consumerGroupID='"
        + consumerGroupID
        + "', consumerClientID="
        + consumerClientID
        + "}";
  }
}
