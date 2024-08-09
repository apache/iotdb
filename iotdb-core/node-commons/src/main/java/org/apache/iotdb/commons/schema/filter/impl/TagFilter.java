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

package org.apache.iotdb.commons.schema.filter.impl;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterType;
import org.apache.iotdb.commons.schema.filter.SchemaFilterVisitor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class TagFilter extends SchemaFilter {

  private final String key;
  private final String value;
  private final boolean isContains;

  public TagFilter(final String key, final String value, final boolean isContains) {
    this.key = key;
    this.value = value;
    this.isContains = isContains;
  }

  public TagFilter(final ByteBuffer byteBuffer) {
    this.key = ReadWriteIOUtils.readString(byteBuffer);
    this.value = ReadWriteIOUtils.readString(byteBuffer);
    this.isContains = ReadWriteIOUtils.readBool(byteBuffer);
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public boolean isContains() {
    return isContains;
  }

  @Override
  public <C> Boolean accept(final SchemaFilterVisitor<C> visitor, final C node) {
    return visitor.visitTagFilter(this, node);
  }

  @Override
  public SchemaFilterType getSchemaFilterType() {
    return SchemaFilterType.TAGS_FILTER;
  }

  @Override
  protected void serialize(final ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(key, byteBuffer);
    ReadWriteIOUtils.write(value, byteBuffer);
    ReadWriteIOUtils.write(isContains, byteBuffer);
  }

  @Override
  protected void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(key, stream);
    ReadWriteIOUtils.write(value, stream);
    ReadWriteIOUtils.write(isContains, stream);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TagFilter that = (TagFilter) o;
    return Objects.equals(key, that.key)
        && Objects.equals(value, that.value)
        && Objects.equals(isContains, that.isContains);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value, isContains);
  }
}
