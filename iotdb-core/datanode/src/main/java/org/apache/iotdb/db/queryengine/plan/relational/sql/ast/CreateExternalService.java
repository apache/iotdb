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

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateExternalService extends Statement {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(CreateExternalService.class);

  private final String serviceName;
  private final String className;

  public CreateExternalService(NodeLocation location, String serviceName, String className) {
    super(requireNonNull(location, "location is null"));

    this.serviceName = requireNonNull(serviceName, "serviceName is null");
    this.className = requireNonNull(className, "className is null");
  }

  public String getServiceName() {
    return serviceName;
  }

  public String getClassName() {
    return className;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateExternalService(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateExternalService that = (CreateExternalService) o;
    return Objects.equals(serviceName, that.serviceName)
        && Objects.equals(className, that.className);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serviceName, className);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("serviceName", serviceName)
        .add("className", className)
        .toString();
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += RamUsageEstimator.sizeOf(serviceName);
    size += RamUsageEstimator.sizeOf(className);
    return size;
  }
}
