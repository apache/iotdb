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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DropSubscription extends SubscriptionStatement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DropSubscription.class);

  private final String subscriptionId;
  private final boolean ifExistsCondition;

  public DropSubscription(final String subscriptionId, final boolean ifExistsCondition) {
    this.subscriptionId = requireNonNull(subscriptionId, "subscription id can not be null");
    this.ifExistsCondition = ifExistsCondition;
  }

  public String getSubscriptionId() {
    return subscriptionId;
  }

  public boolean hasIfExistsCondition() {
    return ifExistsCondition;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitDropSubscription(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subscriptionId, ifExistsCondition);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final DropSubscription that = (DropSubscription) obj;
    return Objects.equals(this.subscriptionId, that.subscriptionId)
        && Objects.equals(this.ifExistsCondition, that.ifExistsCondition);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("subscriptionId", subscriptionId)
        .add("ifExistsCondition", ifExistsCondition)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += RamUsageEstimator.sizeOf(subscriptionId);
    return size;
  }
}
