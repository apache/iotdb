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
import org.apache.iotdb.commons.schema.view.ViewType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ViewTypeFilter extends SchemaFilter {

  private final ViewType viewType;

  public ViewTypeFilter(ViewType viewType) {
    this.viewType = viewType;
  }

  public ViewTypeFilter(ByteBuffer byteBuffer) {
    this.viewType = ViewType.deserializeFrom(byteBuffer);
  }

  public ViewType getViewType() {
    return viewType;
  }

  @Override
  public <C> boolean accept(SchemaFilterVisitor<C> visitor, C node) {
    return visitor.visitViewTypeFilter(this, node);
  }

  @Override
  public SchemaFilterType getSchemaFilterType() {
    return SchemaFilterType.VIEW_TYPE;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    viewType.serializeTo(byteBuffer);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    viewType.serializeTo(stream);
  }
}
