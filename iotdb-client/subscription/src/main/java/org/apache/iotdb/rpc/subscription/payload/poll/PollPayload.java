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

package org.apache.iotdb.rpc.subscription.payload.poll;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class PollPayload implements SubscriptionPollPayload {

  /** The set of topic names that need to be polled. */
  private transient Set<String> topicNames = new HashSet<>();

  public PollPayload() {}

  public PollPayload(final Set<String> topicNames) {
    this.topicNames = topicNames;
  }

  public Set<String> getTopicNames() {
    return topicNames;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.writeObjectSet(topicNames, stream);
  }

  @Override
  public SubscriptionPollPayload deserialize(final ByteBuffer buffer) {
    topicNames = ReadWriteIOUtils.readObjectSet(buffer);
    return this;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PollPayload that = (PollPayload) obj;
    return Objects.equals(this.topicNames, that.topicNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicNames);
  }

  @Override
  public String toString() {
    return "PollPayload{topicNames=" + topicNames + "}";
  }
}
