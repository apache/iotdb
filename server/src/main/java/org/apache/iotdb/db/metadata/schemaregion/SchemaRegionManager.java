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

package org.apache.iotdb.db.metadata.schemaregion;

import org.apache.iotdb.commons.partition.SchemaRegionId;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// manage all the schemaRegion in this dataNode
public class SchemaRegionManager {

  private Map<SchemaRegionId, SchemaRegion> schemaRegionMap;

  private static class SchemaRegionManagerHolder {
    private static final SchemaRegionManager INSTANCE = new SchemaRegionManager();

    private SchemaRegionManagerHolder() {}
  }

  private SchemaRegionManager() {}

  public static SchemaRegionManager getInstance() {
    return SchemaRegionManagerHolder.INSTANCE;
  }

  public void init() {
    schemaRegionMap = new ConcurrentHashMap<>();
  }

  public void clear() {
    if (schemaRegionMap != null) {
      schemaRegionMap.clear();
      schemaRegionMap = null;
    }
  }

  public SchemaRegion getSchemaRegion(SchemaRegionId schemaRegionId) {
    return schemaRegionMap.get(schemaRegionId);
  }

  public Collection<SchemaRegion> getAllSchemaRegions() {
    return schemaRegionMap.values();
  }

  public synchronized SchemaRegion createSchemaRegion(
      PartialPath storageGroup, SchemaRegionId schemaRegionId, IStorageGroupMNode storageGroupMNode)
      throws MetadataException {
    SchemaRegion schemaRegion = schemaRegionMap.get(schemaRegionId);
    if (schemaRegion != null) {
      return schemaRegion;
    }
    schemaRegion = new SchemaRegion(storageGroup, schemaRegionId, storageGroupMNode);
    schemaRegionMap.put(schemaRegionId, schemaRegion);
    return schemaRegion;
  }

  public void deleteSchemaRegion(SchemaRegionId schemaRegionId) throws MetadataException {
    schemaRegionMap.remove(schemaRegionId).deleteSchemaRegion();
  }
}
