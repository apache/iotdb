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

package org.apache.iotdb.session.subscription;

import org.apache.iotdb.rpc.subscription.payload.EnrichedTablets;

import java.util.Objects;

public class SubscriptionMessage implements Comparable<SubscriptionMessage> {

  // TODO: support more data format
  private final SubscriptionMessagePayload payload;

  private final String topicName;
  private final String subscriptionCommitId;

  public SubscriptionMessage(EnrichedTablets tablets) {
    this.payload = new SubscriptionSessionDataSets(tablets.getTablets());
    this.topicName = tablets.getTopicName();
    this.subscriptionCommitId = tablets.getSubscriptionCommitId();
  }

  public String getTopicName() {
    return topicName;
  }

  public SubscriptionMessagePayload getPayload() {
    return payload;
  }

  String getSubscriptionCommitId() {
    // make it package-private
    return subscriptionCommitId;
  }

  int parseDataNodeIdFromSubscriptionCommitId() {
    // make it package-private
    return Integer.parseInt(subscriptionCommitId.split("#")[0]);
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
    return Objects.equals(this.topicName, that.topicName)
        && Objects.equals(this.subscriptionCommitId, that.subscriptionCommitId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicName, subscriptionCommitId);
  }

  @Override
  public int compareTo(SubscriptionMessage that) {
    if (this.topicName.compareTo(that.topicName) == 0) {
      return this.subscriptionCommitId.compareTo(that.subscriptionCommitId);
    }
    return this.topicName.compareTo(that.topicName);
  }
}
