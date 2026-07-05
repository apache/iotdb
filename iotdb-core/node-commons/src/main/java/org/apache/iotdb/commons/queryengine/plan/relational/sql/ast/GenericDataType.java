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

package org.apache.iotdb.commons.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.i18n.QueryMessages;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nonnull;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class GenericDataType extends DataType {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(GenericDataType.class);

  private final Identifier name;
  private final List<DataTypeParameter> arguments;

  public GenericDataType(Identifier name, List<DataTypeParameter> arguments) {
    super(null);
    this.name = requireNonNull(name, QueryMessages.EXCEPTION_NAME_IS_NULL_C8B35959);
    this.arguments = requireNonNull(arguments, QueryMessages.EXCEPTION_ARGUMENTS_IS_NULL_B1F6D4F2);
  }

  public GenericDataType(
      @Nonnull NodeLocation location, Identifier name, List<DataTypeParameter> arguments) {
    super(requireNonNull(location, QueryMessages.EXCEPTION_LOCATION_IS_NULL_F134D388));
    this.name = requireNonNull(name, QueryMessages.EXCEPTION_NAME_IS_NULL_C8B35959);
    this.arguments = requireNonNull(arguments, QueryMessages.EXCEPTION_ARGUMENTS_IS_NULL_B1F6D4F2);
  }

  public Identifier getName() {
    return name;
  }

  public List<DataTypeParameter> getArguments() {
    return arguments;
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.<Node>builder().add(name).addAll(arguments).build();
  }

  @Override
  public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
    return ((CommonQueryAstVisitor<R, C>) visitor).visitGenericDataType(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GenericDataType that = (GenericDataType) o;
    return name.equals(that.name) && arguments.equals(that.arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, arguments);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }

  // =============== serialize =================
  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.GENERIC_DATA_TYPE;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    serialize(name, stream);
    // ReadWriteIOUtils.write(arguments.size(), stream);
  }

  public GenericDataType(ByteBuffer byteBuffer) {
    super(null);
    this.name = (Identifier) deserialize(byteBuffer);
    // arguments are always empty now
    this.arguments = Collections.emptyList();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal())
        + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(name)
        + AstMemoryEstimationHelper.getEstimatedSizeOfNodeList(arguments);
  }
}
