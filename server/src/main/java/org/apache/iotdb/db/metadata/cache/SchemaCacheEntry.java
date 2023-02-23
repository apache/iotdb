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

package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.cache.lastCache.container.ILastCacheContainer;
import org.apache.iotdb.db.metadata.cache.lastCache.container.LastCacheContainer;
import org.apache.iotdb.db.mpp.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.Map;

public class SchemaCacheEntry implements IMeasurementSchemaInfo {

  private final String storageGroup;

  private final MeasurementSchema measurementSchema;

  private final Map<String, String> tagMap;
  private final boolean isAligned;

  private volatile ILastCacheContainer lastCacheContainer = null;

  SchemaCacheEntry(
      String storageGroup,
      MeasurementSchema measurementSchema,
      Map<String, String> tagMap,
      boolean isAligned) {
    this.storageGroup = storageGroup.intern();
    this.measurementSchema = measurementSchema;
    this.isAligned = isAligned;
    this.tagMap = tagMap;
  }

  public String getSchemaEntryId() {
    return measurementSchema.getMeasurementId();
  }

  public String getStorageGroup() {
    return storageGroup;
  }

  public MeasurementSchema getMeasurementSchema() {
    return measurementSchema;
  }

  public Map<String, String> getTagMap() {
    return tagMap;
  }

  public TSDataType getTsDataType() {
    return measurementSchema.getType();
  }

  public boolean isAligned() {
    return isAligned;
  }

  public ILastCacheContainer getLastCacheContainer() {
    if (lastCacheContainer == null) {
      synchronized (this) {
        if (lastCacheContainer == null) {
          lastCacheContainer = new LastCacheContainer();
        }
      }
    }
    return lastCacheContainer;
  }

  public void setLastCacheContainer(ILastCacheContainer lastCacheContainer) {
    this.lastCacheContainer = lastCacheContainer;
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
    return 100 + 2 * schemaCacheEntry.getMeasurementSchema().getMeasurementId().length();
  }

  @Override
  public String getName() {
    return measurementSchema.getMeasurementId();
  }

  @Override
  public MeasurementSchema getSchema() {
    return measurementSchema;
  }

  @Override
  public String getAlias() {
    return null;
  }
}
