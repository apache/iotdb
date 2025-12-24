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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WindowReference extends Node implements Window {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(WindowReference.class);

  private final Identifier name;

  public WindowReference(NodeLocation location, Identifier name) {
    super(location);
    this.name = requireNonNull(name, "name is null");
  }

  public Identifier getName() {
    return name;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitWindowReference(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(name);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    WindowReference o = (WindowReference) obj;
    return Objects.equals(name, o.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("name", name).toString();
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    name.serialize(byteBuffer);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    name.serialize(stream);
  }

  public WindowReference(ByteBuffer byteBuffer) {
    super(null);
    this.name = new Identifier(byteBuffer);
  }

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(name);
    return size;
  }
}
