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

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateTopic extends SubscriptionStatement {

  private final String topicName;
  private final boolean ifNotExistsCondition;
  private final Map<String, String> topicAttributes;

  public CreateTopic(
      final String topicName,
      final boolean ifNotExistsCondition,
      final Map<String, String> topicAttributes) {
    this.topicName = requireNonNull(topicName, "topic name can not be null");
    this.ifNotExistsCondition = ifNotExistsCondition;
    this.topicAttributes = requireNonNull(topicAttributes, "topic attributes can not be null");
  }

  public String getTopicName() {
    return topicName;
  }

  public boolean hasIfNotExistsCondition() {
    return ifNotExistsCondition;
  }

  public Map<String, String> getTopicAttributes() {
    return topicAttributes;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateTopic(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicAttributes, ifNotExistsCondition, topicAttributes);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final CreateTopic that = (CreateTopic) obj;
    return Objects.equals(this.topicName, that.topicName)
        && Objects.equals(this.ifNotExistsCondition, that.ifNotExistsCondition)
        && Objects.equals(this.topicAttributes, that.topicAttributes);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("pipeName", topicName)
        .add("ifNotExistsCondition", ifNotExistsCondition)
        .add("topicAttributes", topicAttributes)
        .toString();
  }
}
