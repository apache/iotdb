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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DataTypeFilter extends SchemaFilter {

  private final TSDataType dataType;

  public DataTypeFilter(TSDataType dataType) {
    this.dataType = dataType;
  }

  public DataTypeFilter(ByteBuffer byteBuffer) {
    this.dataType = TSDataType.deserializeFrom(byteBuffer);
  }

  public TSDataType getDataType() {
    return dataType;
  }

  @Override
  public <R, C> R accept(SchemaFilterVisitor<R, C> visitor, C node) {
    return visitor.visitDataTypeFilter(this, node);
  }

  @Override
  public SchemaFilterType getSchemaFilterType() {
    return SchemaFilterType.DATA_TYPE;
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    dataType.serializeTo(byteBuffer);
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    dataType.serializeTo(stream);
  }
}
