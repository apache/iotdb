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

package org.apache.iotdb.db.queryengine.plan.analyze.cache.schema;

import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.queryengine.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.lastcache.ILastCacheContainer;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.lastcache.LastCacheContainer;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.Map;

public class SchemaCacheEntry implements IMeasurementSchemaInfo {

  private final String storageGroup;

  private final IMeasurementSchema iMeasurementSchema;

  private final Map<String, String> tagMap;
  private final boolean isAligned;

  @SuppressWarnings("java:S3077")
  private volatile ILastCacheContainer lastCacheContainer = null;

  public SchemaCacheEntry(
      String storageGroup,
      IMeasurementSchema iMeasurementSchema,
      Map<String, String> tagMap,
      boolean isAligned) {
    this.storageGroup = storageGroup.intern();
    this.iMeasurementSchema = iMeasurementSchema;
    this.isAligned = isAligned;
    this.tagMap = tagMap;
  }

  public String getSchemaEntryId() {
    return iMeasurementSchema.getMeasurementId();
  }

  public String getStorageGroup() {
    return storageGroup;
  }

  public IMeasurementSchema getIMeasurementSchema() {
    return iMeasurementSchema;
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

  public boolean isAligned() {
    return isAligned;
  }

  public ILastCacheContainer getLastCacheContainer() {
    return lastCacheContainer;
  }

  public int updateLastCache(
      TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime) {

    if (lastCacheContainer == null) {
      synchronized (this) {
        if (lastCacheContainer == null) {
          ILastCacheContainer tmp = new LastCacheContainer();
          int changeSize = tmp.estimateSize();
          changeSize += tmp.updateCachedLast(timeValuePair, highPriorityUpdate, latestFlushedTime);
          lastCacheContainer = tmp;
          return changeSize;
        }
      }
    }
    return lastCacheContainer.updateCachedLast(
        timeValuePair, highPriorityUpdate, latestFlushedTime);
  }

  /**
   * Total basic 100B
   *
   * <ul>
   *   <li>SchemaCacheEntry Object header, 8B
   *   <li>isAligned, 1B
   *   <li>LastCacheContainer reference, 8B
   *   <li>MeasurementSchema
   *       <ul>
   *         <li>Reference, 8B
   *         <li>Object header, 8B
   *         <li>String measurementId basic, 8 + 8 + 4 + 8 + 4 = 32B
   *         <li>type, encoding, compressor, 3 B
   *         <li>encodingConverter, 8 + 8 + 8 = 24B
   *         <li>props, 8B
   *       </ul>
   * </ul>
   */
  public static int estimateSize(SchemaCacheEntry schemaCacheEntry) {
    // each char takes 2B in Java
    int lastCacheContainerSize =
        schemaCacheEntry.getLastCacheContainer() == null
            ? 0
            : schemaCacheEntry.getLastCacheContainer().estimateSize();
    return 100
        + 2 * schemaCacheEntry.getIMeasurementSchema().getMeasurementId().length()
        + lastCacheContainerSize;
  }

  @Override
  public String getName() {
    return iMeasurementSchema.getMeasurementId();
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

  public int invalidateLastCache() {
    if (this.lastCacheContainer == null || this.lastCacheContainer.getCachedLast() == null) {
      return 0;
    }

    return this.lastCacheContainer.invalidateLastCache();
  }
}
