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
import java.util.HashSet;
import java.util.Set;

/**
 * This class can be seen as a combination of {@link DeviceIdFilter} & {@link OrFilter}. Here we
 * construct a new filter to avoid too deep stack and to ensure performance.
 */
public class MultiDeviceIdFilter extends SchemaFilter {

  private final int index;

  private final Set<String> values;

  public MultiDeviceIdFilter(int index, Set<String> values) {
    this.index = index;
    this.values = values;
  }

  public MultiDeviceIdFilter(final ByteBuffer byteBuffer) {
    this.index = ReadWriteIOUtils.readInt(byteBuffer);

    final int length = ReadWriteIOUtils.readInt(byteBuffer);
    this.values = new HashSet<>();
    for (int i = 0; i < length; ++i) {
      values.add(ReadWriteIOUtils.readString(byteBuffer));
    }
  }

  public int getIndex() {
    return index;
  }

  public Set<String> getValues() {
    return values;
  }

  @Override
  public <C> boolean accept(final SchemaFilterVisitor<C> visitor, final C node) {
    return visitor.visitMultiDeviceIdFilter(this, node);
  }

  @Override
  public SchemaFilterType getSchemaFilterType() {
    return SchemaFilterType.MULTI_DEVICE_ID;
  }

  @Override
  public void serialize(final ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(index, byteBuffer);
    ReadWriteIOUtils.write(values.size(), byteBuffer);
    for (final String value : values) {
      ReadWriteIOUtils.write(value, byteBuffer);
    }
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(index, stream);
    ReadWriteIOUtils.write(values.size(), stream);
    for (final String value : values) {
      ReadWriteIOUtils.write(value, stream);
    }
  }
}
