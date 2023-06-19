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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class AndFilter extends SchemaFilter {

  private final SchemaFilter left;
  private final SchemaFilter right;

  public AndFilter(SchemaFilter left, SchemaFilter right) {
    // left and right should not be null
    this.left = left;
    this.right = right;
  }

  public AndFilter(ByteBuffer byteBuffer) {
    this.left = SchemaFilter.deserialize(byteBuffer);
    this.right = SchemaFilter.deserialize(byteBuffer);
  }

  public SchemaFilter getLeft() {
    return left;
  }

  public SchemaFilter getRight() {
    return right;
  }

  @Override
  public <C> boolean accept(SchemaFilterVisitor<C> visitor, C node) {
    return visitor.visitAndFilter(this, node);
  }

  @Override
  public SchemaFilterType getSchemaFilterType() {
    return SchemaFilterType.AND;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    left.serialize(byteBuffer);
    right.serialize(byteBuffer);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    left.serialize(stream);
    right.serialize(stream);
  }
}
