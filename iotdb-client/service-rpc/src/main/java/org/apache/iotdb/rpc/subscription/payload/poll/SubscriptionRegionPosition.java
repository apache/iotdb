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

import java.util.Objects;

public class SubscriptionRegionPosition {

  private final long epoch;
  private final long syncIndex;

  public SubscriptionRegionPosition(final long epoch, final long syncIndex) {
    this.epoch = epoch;
    this.syncIndex = syncIndex;
  }

  public long getEpoch() {
    return epoch;
  }

  public long getSyncIndex() {
    return syncIndex;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SubscriptionRegionPosition)) {
      return false;
    }
    final SubscriptionRegionPosition that = (SubscriptionRegionPosition) obj;
    return epoch == that.epoch && syncIndex == that.syncIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(epoch, syncIndex);
  }

  @Override
  public String toString() {
    return "SubscriptionRegionPosition{" + "epoch=" + epoch + ", syncIndex=" + syncIndex + '}';
  }
}
