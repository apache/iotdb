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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.schemaengine.template.ITemplateManager;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DeviceUsingTemplateSchemaCache {

  private static final Logger logger =
      LoggerFactory.getLogger(DeviceUsingTemplateSchemaCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Cache<PartialPath, DeviceCacheEntry> cache;

  private final ITemplateManager templateManager;

  DeviceUsingTemplateSchemaCache(ITemplateManager templateManager) {
    this.templateManager = templateManager;
    // TODO proprietary config parameter expected
    cache =
        Caffeine.newBuilder()
            .maximumWeight(config.getAllocateMemoryForSchemaCache())
            .weigher(
                (Weigher<PartialPath, DeviceCacheEntry>)
                    (key, val) -> (PartialPath.estimateSize(key) + 32))
            .recordStats()
            .build();
  }

  public void put(PartialPath path, String database, Integer id) {
    cache.put(path, new DeviceCacheEntry(database, id));
  }

  public void invalidateCache(List<? extends PartialPath> partialPathList) {
    for (PartialPath path : partialPathList) {
      for (PartialPath key : cache.asMap().keySet()) {
        if (key.startsWith(path.getIDeviceID().toString())) {
          cache.invalidate(key);
        }
      }
    }
  }

  private static class DeviceCacheEntry {

    private final String database;

    private final int templateId;

    private DeviceCacheEntry(String database, int templateId) {
      this.database = database.intern();
      this.templateId = templateId;
    }

    public int getTemplateId() {
      return templateId;
    }

    public String getDatabase() {
      return database;
    }
  }
}
