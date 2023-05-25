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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.template.ITemplateManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaComputation;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

  public long getHitCount() {
    return cache.stats().hitCount();
  }

  public long getRequestCount() {
    return cache.stats().requestCount();
  }

  public ClusterSchemaTree get(PartialPath fullPath) {
    DeviceCacheEntry deviceCacheEntry = cache.getIfPresent(fullPath.getDevicePath());
    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    if (deviceCacheEntry != null) {
      Template template = templateManager.getTemplate(deviceCacheEntry.getTemplateId());
      IMeasurementSchema measurementSchema = template.getSchema(fullPath.getMeasurement());
      if (measurementSchema != null) {
        schemaTree.appendSingleMeasurement(
            fullPath,
            (MeasurementSchema) measurementSchema,
            null,
            null,
            template.isDirectAligned());
        schemaTree.setDatabases(Collections.singleton(deviceCacheEntry.getDatabase()));
      }
    }
    return schemaTree;
  }

  public ClusterSchemaTree getMatchedSchemaWithTemplate(PartialPath path) {
    PartialPath devicePath = path.getDevicePath();
    DeviceCacheEntry deviceCacheEntry = cache.getIfPresent(devicePath);
    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    if (deviceCacheEntry != null) {
      Template template = templateManager.getTemplate(deviceCacheEntry.getTemplateId());
      String measurement = path.getMeasurement();
      if (PathPatternUtil.hasWildcard(measurement)) {
        for (Map.Entry<String, IMeasurementSchema> entry : template.getSchemaMap().entrySet()) {
          if (PathPatternUtil.isNodeMatch(measurement, entry.getKey())) {
            schemaTree.appendSingleMeasurement(
                devicePath.concatNode(entry.getKey()),
                entry.getValue(),
                null,
                null,
                template.isDirectAligned());
            schemaTree.setDatabases(Collections.singleton(deviceCacheEntry.getDatabase()));
          }
        }
      } else {
        IMeasurementSchema measurementSchema = template.getSchema(measurement);
        if (measurementSchema != null) {
          schemaTree.appendSingleMeasurement(
              path, measurementSchema, null, null, template.isDirectAligned());
          schemaTree.setDatabases(Collections.singleton(deviceCacheEntry.getDatabase()));
        }
      }
    }
    return schemaTree;
  }

  /**
   * CONFORM indicates that the provided devicePath had been cached as a template activated path,
   * ensuring that the alignment of the device, as well as the name and schema of every measurement
   * are consistent with the cache.
   *
   * @param computation
   * @return true if conform to template cache, which means no need to fetch or create anymore
   */
  public List<Integer> compute(ISchemaComputation computation) {
    List<Integer> indexOfMissingMeasurements = new ArrayList<>();
    PartialPath devicePath = computation.getDevicePath();
    String[] measurements = computation.getMeasurements();
    DeviceCacheEntry deviceCacheEntry = cache.getIfPresent(devicePath);
    if (deviceCacheEntry == null) {
      for (int i = 0; i < measurements.length; i++) {
        indexOfMissingMeasurements.add(i);
      }
      return indexOfMissingMeasurements;
    }

    computation.computeDevice(
        templateManager.getTemplate(deviceCacheEntry.getTemplateId()).isDirectAligned());
    Map<String, IMeasurementSchema> templateSchema =
        templateManager.getTemplate(deviceCacheEntry.getTemplateId()).getSchemaMap();
    for (int i = 0; i < measurements.length; i++) {
      if (!templateSchema.containsKey(measurements[i])) {
        indexOfMissingMeasurements.add(i);
        continue;
      }
      IMeasurementSchema schema = templateSchema.get(measurements[i]);
      computation.computeMeasurement(
          i,
          new IMeasurementSchemaInfo() {
            @Override
            public String getName() {
              return schema.getMeasurementId();
            }

            @Override
            public IMeasurementSchema getSchema() {
              if (isLogicalView()) {
                return new LogicalViewSchema(
                    schema.getMeasurementId(), ((LogicalViewSchema) schema).getExpression());
              } else {
                return this.getSchemaAsMeasurementSchema();
              }
            }

            @Override
            public MeasurementSchema getSchemaAsMeasurementSchema() {
              return new MeasurementSchema(
                  schema.getMeasurementId(),
                  schema.getType(),
                  schema.getEncodingType(),
                  schema.getCompressor());
            }

            @Override
            public LogicalViewSchema getSchemaAsLogicalViewSchema() {
              if (isLogicalView()) {
                return new LogicalViewSchema(
                    schema.getMeasurementId(), ((LogicalViewSchema) schema).getExpression());
              }
              return null;
            }

            @Override
            public String getAlias() {
              return null;
            }

            @Override
            public boolean isLogicalView() {
              return schema.isLogicalView();
            }
          });
    }
    return indexOfMissingMeasurements;
  }

  public void put(PartialPath path, String database, Integer id) {
    cache.put(path, new DeviceCacheEntry(database, id));
  }

  public void invalidateCache() {
    cache.invalidateAll();
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
