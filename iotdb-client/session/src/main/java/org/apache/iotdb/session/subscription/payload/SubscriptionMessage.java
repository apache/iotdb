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

package org.apache.iotdb.session.subscription.payload;

import org.apache.iotdb.rpc.subscription.payload.common.SubscriptionCommitContext;

import org.apache.tsfile.write.record.Tablet;

import java.util.List;
import java.util.Objects;

public class SubscriptionMessage implements Comparable<SubscriptionMessage> {

  private final SubscriptionCommitContext commitContext;

  private final short messageType;

  private final SubscriptionMessagePayload payload;

  public SubscriptionMessage(SubscriptionCommitContext commitContext, List<Tablet> tablets) {
    this.commitContext = commitContext;
    this.messageType = SubscriptionMessageType.SESSION_DATA_SET.getType();
    this.payload = new SubscriptionSessionDataSets(tablets);
  }

  public SubscriptionMessage(SubscriptionCommitContext commitContext, String filePath) {
    this.commitContext = commitContext;
    this.messageType = SubscriptionMessageType.TS_FILE_READER.getType();
    this.payload = new SubscriptionTsFileReader(filePath);
  }

  public SubscriptionCommitContext getCommitContext() {
    return commitContext;
  }

  public short getMessageType() {
    return messageType;
  }

  public SubscriptionMessagePayload getPayload() {
    return payload;
  }

  /////////////////////////////// override ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    SubscriptionMessage that = (SubscriptionMessage) obj;
    return Objects.equals(this.commitContext, that.commitContext)
        && Objects.equals(this.messageType, that.messageType)
        && Objects.equals(this.payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commitContext, messageType, payload);
  }

  @Override
  public int compareTo(SubscriptionMessage that) {
    return this.commitContext.compareTo(that.commitContext);
  }

  @Override
  public String toString() {
    return "SubscriptionMessage{commitContext="
        + commitContext
        + ", messageType="
        + SubscriptionMessageType.valueOf(messageType).toString()
        + "}";
  }
}
