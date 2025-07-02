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
package org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.impl;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ITimeSeriesSchemaInfo;

import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.util.Map;

public class TimeseriesSchemaInfo implements ITimeSeriesSchemaInfo {

  private final IMeasurementMNode<?> node;
  private final PartialPath fullPath;
  private final Map<String, String> tags;
  private final Map<String, String> attributes;
  private final boolean isUnderAlignedDevice;

  public TimeseriesSchemaInfo(
      final IMeasurementMNode<?> node,
      final PartialPath fullPath,
      final Map<String, String> tags,
      final Map<String, String> attributes,
      final boolean isUnderAlignedDevice) {
    this.node = node;
    this.fullPath = fullPath;
    this.tags = tags;
    this.attributes = attributes;
    this.isUnderAlignedDevice = isUnderAlignedDevice;
  }

  @Override
  public String getFullPath() {
    return fullPath.getFullPath();
  }

  @Override
  public PartialPath getPartialPath() {
    return fullPath;
  }

  @Override
  public String getAlias() {
    return node.getAlias();
  }

  @Override
  public IMeasurementSchema getSchema() {
    return node.getSchema();
  }

  @Override
  public Map<String, String> getTags() {
    return tags;
  }

  @Override
  public Map<String, String> getAttributes() {
    return attributes;
  }

  @Override
  public boolean isUnderAlignedDevice() {
    return isUnderAlignedDevice;
  }

  @Override
  public boolean isLogicalView() {
    return node.isLogicalView();
  }

  @Override
  public ITimeSeriesSchemaInfo snapshot() {
    return this;
  }
}
