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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.template.TemplateImcompatibeException;
import org.apache.iotdb.db.metadata.template.ClusterTemplateManager;
import org.apache.iotdb.db.metadata.template.ITemplateManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.schematree.IMeasurementSchemaInfo;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaComputationWithAutoCreation;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataNodeTemplateSchemaCache {

  private static final Logger logger = LoggerFactory.getLogger(DataNodeTemplateSchemaCache.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Cache<PartialPath, Integer> cache;

  private final ITemplateManager templateManager = ClusterTemplateManager.getInstance();

  // cache update due to activation or clear procedure
  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(false);

  private DataNodeTemplateSchemaCache() {
    // TODO proprietary config parameter expected
    cache =
        Caffeine.newBuilder()
            .maximumWeight(config.getAllocateMemoryForSchemaCache())
            .weigher(
                (Weigher<PartialPath, Integer>) (key, val) -> (PartialPath.estimateSize(key) + 16))
            .build();
  }

  public static DataNodeTemplateSchemaCache getInstance() {
    return DataNodeTemplateSchemaCacheHolder.INSTANCE;
  }

  /** singleton pattern. */
  private static class DataNodeTemplateSchemaCacheHolder {
    private static final DataNodeTemplateSchemaCache INSTANCE = new DataNodeTemplateSchemaCache();
  }

  public void takeReadLock() {
    readWriteLock.readLock().lock();
  }

  public void releaseReadLock() {
    readWriteLock.readLock().unlock();
  }

  public void takeWriteLock() {
    readWriteLock.writeLock().lock();
  }

  public void releaseWriteLock() {
    readWriteLock.writeLock().unlock();
  }

  public Integer get(PartialPath path) {
    readWriteLock.readLock().lock();
    try {
      return cache.getIfPresent(path);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void put(PartialPath path, Integer id) {
    readWriteLock.writeLock().lock();
    try {
      cache.put(path, id);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void invalidateCache() {
    cache.invalidateAll();
  }

  public boolean compatibleCheckAndPut(
      ISchemaComputationWithAutoCreation schemaComputationWithAutoCreation)
      throws TemplateImcompatibeException {
    PartialPath devicePath = schemaComputationWithAutoCreation.getDevicePath();
    String[] measurements = schemaComputationWithAutoCreation.getMeasurements();

    Template template = null;
    if (this.get(devicePath) != null) {
      // exists in cache, to check compatible
      template = templateManager.getTemplate(this.get(devicePath));
    } else {
      // check validity to activate
      Optional<Pair<Template, ?>> templateInfo =
          Optional.ofNullable(
              templateManager.checkTemplateSetInfo(
                  schemaComputationWithAutoCreation.getDevicePath()));

      // ONE check is sufficient due to the restriction of activation
      if (templateInfo.isPresent() && templateInfo.get().left.hasSchema(measurements[0])) {
        // measurement overlapped, check compatible to activate
        template = templateInfo.get().left;
      }
    }

    if (template != null) {
      schemaComputationWithAutoCreation.computeDevice(template.isDirectAligned());
      for (int i = 0; i < measurements.length; i++) {
        if (!template.hasSchema(measurements[i])) {
          throw new TemplateImcompatibeException(
              String.format(
                  "Cannot create timeseries[%s] not subject to template[%s] under"
                      + "an activated node[%s].",
                  measurements[i], template.getName(), devicePath.getDevicePath()));
        }

        IMeasurementSchema schema = template.getSchema(measurements[i]);
        schemaComputationWithAutoCreation.computeMeasurement(
            i,
            new IMeasurementSchemaInfo() {
              @Override
              public String getName() {
                return schema.getMeasurementId();
              }

              @Override
              public MeasurementSchema getSchema() {
                return new MeasurementSchema(
                    schema.getMeasurementId(),
                    schema.getType(),
                    schema.getTimeTSEncoding(),
                    schema.getCompressor());
              }

              @Override
              public String getAlias() {
                // FIXME alias is not used CURRENTLY
                return null;
              }
            });
      }
      this.put(devicePath, template.getId());
      return true;
    }
    return false;
  }
}
