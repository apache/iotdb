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

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class JoinOn extends JoinCriteria {
  private static final long INSTANCE_SIZE = RamUsageEstimator.shallowSizeOfInstance(JoinOn.class);

  // this can be null when it is AsofJoinOn
  @Nullable protected final Expression expression;

  public JoinOn(@Nullable Expression expression) {
    this.expression = expression;
  }

  @Nullable
  public Expression getExpression() {
    return expression;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    JoinOn o = (JoinOn) obj;
    return Objects.equals(expression, o.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression);
  }

  @Override
  public String toString() {
    return toStringHelper(this).addValue(expression).toString();
  }

  @Override
  public List<Node> getNodes() {
    return ImmutableList.of(expression);
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(expression);
    return size;
  }

  protected long ramBytesUsedExcludingInstanceSize() {
    return AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(expression);
  }
}
