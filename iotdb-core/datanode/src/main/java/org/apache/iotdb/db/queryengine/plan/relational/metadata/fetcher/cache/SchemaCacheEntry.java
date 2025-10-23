/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache;

import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.queryengine.common.schematree.IMeasurementSchemaInfo;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Map;

public class SchemaCacheEntry implements IMeasurementSchemaInfo {

  /**
   * MeasurementSchema takes 75B
   *
   * <ul>
   *   <li>MeasurementSchema
   *       <ul>
   *         <li>Object header, 8B
   *         <li>String measurementId basic, 8 + 8 + 4 + 8 + 4 = 32B
   *         <li>type, encoding, compressor, 3 B
   *         <li>encodingConverter, 8 + 8 + 8 = 24B
   *         <li>props, 8B
   *       </ul>
   * </ul>
   */
  private static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(SchemaCacheEntry.class) + 75;

  private final IMeasurementSchema iMeasurementSchema;

  private final Map<String, String> tagMap;

  public SchemaCacheEntry(
      final @Nonnull IMeasurementSchema iMeasurementSchema,
      final @Nullable Map<String, String> tagMap) {
    this.iMeasurementSchema = iMeasurementSchema;
    this.tagMap = tagMap;
  }

  @Override
  public Map<String, String> getTagMap() {
    return tagMap;
  }

  @Override
  public Map<String, String> getAttributeMap() {
    return null;
  }

  public TSDataType getTsDataType() {
    return iMeasurementSchema.getType();
  }

  public static int estimateSize(final SchemaCacheEntry schemaCacheEntry) {
    return INSTANCE_SIZE
        + 2 * schemaCacheEntry.getSchema().getMeasurementName().length()
        + (int) RamUsageEstimator.sizeOfMap(schemaCacheEntry.getTagMap());
  }

  @Override
  public String getName() {
    return iMeasurementSchema.getMeasurementName();
  }

  @Override
  public IMeasurementSchema getSchema() {
    return iMeasurementSchema;
  }

  @Override
  public MeasurementSchema getSchemaAsMeasurementSchema() {
    if (this.iMeasurementSchema instanceof MeasurementSchema) {
      return (MeasurementSchema) this.getSchema();
    }
    return null;
  }

  @Override
  public LogicalViewSchema getSchemaAsLogicalViewSchema() {
    if (this.iMeasurementSchema instanceof LogicalViewSchema) {
      return (LogicalViewSchema) this.getSchema();
    }
    return null;
  }

  @Override
  public String getAlias() {
    return null;
  }

  @Override
  public boolean isLogicalView() {
    return this.iMeasurementSchema.isLogicalView();
  }
}
