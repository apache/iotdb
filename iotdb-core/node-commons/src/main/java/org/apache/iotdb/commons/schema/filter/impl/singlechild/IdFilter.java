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

package org.apache.iotdb.commons.schema.filter.impl.singlechild;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterType;
import org.apache.iotdb.commons.schema.filter.SchemaFilterVisitor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * {@link IdFilter} and {@link AttributeFilter} share the same values filter for query logic on
 * their values. {@link IdFilter} and {@link AttributeFilter} just indicates that how to get the
 * id/attribute value from the device entry.
 */
public class IdFilter extends AbstractSingleChildFilter {
  private final int index;

  public IdFilter(final SchemaFilter child, final int index) {
    super(child);
    this.index = index;
  }

  public IdFilter(final ByteBuffer byteBuffer) {
    super(byteBuffer);
    index = ReadWriteIOUtils.readInt(byteBuffer);
  }

  public int getIndex() {
    return index;
  }

  @Override
  public <C> Boolean accept(final SchemaFilterVisitor<C> visitor, final C node) {
    return visitor.visitIdFilter(this, node);
  }

  @Override
  public SchemaFilterType getSchemaFilterType() {
    return SchemaFilterType.ID;
  }

  @Override
  protected void serialize(final ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(index, byteBuffer);
  }

  @Override
  protected void serialize(final DataOutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(index, stream);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final IdFilter that = (IdFilter) o;
    return super.equals(o) && Objects.equals(index, that.index);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, super.hashCode());
  }
}
