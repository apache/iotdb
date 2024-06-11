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

public class DeviceIdFilter extends SchemaFilter {

  // id column index
  // when used in partialPath, the index of node in path shall be [this.index + 3]
  // since a partialPath start with {root, db, table}
  private final int index;

  private final String value;

  public DeviceIdFilter(int index, String value) {
    this.index = index;
    this.value = value;
  }

  public DeviceIdFilter(ByteBuffer byteBuffer) {
    this.index = ReadWriteIOUtils.readInt(byteBuffer);
    this.value = ReadWriteIOUtils.readString(byteBuffer);
  }

  public int getIndex() {
    return index;
  }

  public String getValue() {
    return value;
  }

  @Override
  public <C> boolean accept(SchemaFilterVisitor<C> visitor, C node) {
    return visitor.visitDeviceIdFilter(this, node);
  }

  @Override
  public SchemaFilterType getSchemaFilterType() {
    return SchemaFilterType.DEVICE_ID;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(index, byteBuffer);
    ReadWriteIOUtils.write(value, byteBuffer);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(index, stream);
    ReadWriteIOUtils.write(value, stream);
  }
}
