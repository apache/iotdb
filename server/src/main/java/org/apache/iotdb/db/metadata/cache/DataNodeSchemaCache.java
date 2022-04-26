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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.sql.analyze.FakeSchemaFetcherImpl;
import org.apache.iotdb.db.mpp.sql.analyze.ISchemaFetcher;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class takes the responsibility of metadata cache management of all DataRegions under
 * StorageEngine
 */
public class DataNodeSchemaCache {
  private static final Logger logger = LoggerFactory.getLogger(DataNodeSchemaCache.class);

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private Cache<PartialPath, SchemaCacheEntity> schemaEntityCache;

  // TODO use fakeSchemaFetcherImpl for test temporarily
  private static final ISchemaFetcher schemaFetcher = new FakeSchemaFetcherImpl();

  private DataNodeSchemaCache() {
    schemaEntityCache =
        Caffeine.newBuilder().maximumSize(config.getDataNodeSchemaCacheSize()).build();
  }

  public static DataNodeSchemaCache getInstance() {
    return DataNodeSchemaCache.DataNodeSchemaEntryCacheHolder.INSTANCE;
  }

  /** singleton pattern. */
  private static class DataNodeSchemaEntryCacheHolder {
    private static final DataNodeSchemaCache INSTANCE = new DataNodeSchemaCache();
  }

  /**
   * Get SchemaEntity info without auto create schema
   *
   * @param devicePath should not be measurementPath or AlignedPath
   * @param measurements
   * @return timeseries partialPath and its SchemaEntity
   */
  public Map<PartialPath, SchemaCacheEntity> getSchemaEntity(
      PartialPath devicePath, String[] measurements) {
    Map<PartialPath, SchemaCacheEntity> schemaCacheEntityMap = new HashMap<>();
    SchemaCacheEntity schemaCacheEntity;
    List<String> fetchMeasurements = new ArrayList<>();
    for (String measurement : measurements) {
      PartialPath path = null;
      try {
        path = new PartialPath(devicePath.getFullPath(), measurement);
      } catch (IllegalPathException e) {
        logger.error(
            "Create PartialPath:{} failed.",
            devicePath.getFullPath() + TsFileConstant.PATH_SEPARATOR + measurement);
      }
      schemaCacheEntity = schemaEntityCache.getIfPresent(path);
      if (schemaCacheEntity != null) {
        schemaCacheEntityMap.put(path, schemaCacheEntity);
      } else {
        fetchMeasurements.add(measurement);
      }
    }
    if (fetchMeasurements.size() != 0) {
      SchemaTree schemaTree;
      schemaTree = schemaFetcher.fetchSchema(new PathPatternTree(devicePath, fetchMeasurements));
      // TODO need to construct schemaEntry from schemaTree

    }
    return schemaCacheEntityMap;
  }

  /**
   * Get SchemaEntity info with auto create schema
   *
   * @param devicePath
   * @param measurements
   * @param tsDataTypes
   * @param isAligned
   * @return timeseries partialPath and its SchemaEntity
   */
  public Map<PartialPath, SchemaCacheEntity> getSchemaEntityWithAutoCreate(
      PartialPath devicePath, String[] measurements, TSDataType[] tsDataTypes, boolean isAligned) {
    Map<PartialPath, SchemaCacheEntity> schemaCacheEntityMap = new HashMap<>();
    SchemaCacheEntity schemaCacheEntity;
    List<String> fetchMeasurements = new ArrayList<>();
    List<TSDataType> fetchTsDataTypes = new ArrayList<>();
    for (int i = 0; i < measurements.length; i++) {
      PartialPath path = null;
      try {
        path = new PartialPath(devicePath.getFullPath(), measurements[i]);
      } catch (IllegalPathException e) {
        logger.error(
            "Create PartialPath:{} failed.",
            devicePath.getFullPath() + TsFileConstant.PATH_SEPARATOR + measurements[i]);
      }
      schemaCacheEntity = schemaEntityCache.getIfPresent(path);
      if (schemaCacheEntity != null) {
        schemaCacheEntityMap.put(path, schemaCacheEntity);
      } else {
        fetchMeasurements.add(measurements[i]);
        fetchTsDataTypes.add(tsDataTypes[i]);
      }
    }
    if (fetchMeasurements.size() != 0) {
      SchemaTree schemaTree;
      schemaTree =
          schemaFetcher.fetchSchemaWithAutoCreate(
              devicePath,
              fetchMeasurements.toArray(new String[fetchMeasurements.size()]),
              fetchTsDataTypes.toArray(new TSDataType[fetchTsDataTypes.size()]),
              isAligned);
      // TODO need to construct schemaEntry from schemaTree

      for (int i = 0; i < fetchMeasurements.size(); i++) {
        try {
          PartialPath path = new PartialPath(devicePath.getFullPath(), fetchMeasurements.get(i));
          SchemaCacheEntity entity =
              new SchemaCacheEntity(fetchMeasurements.get(i), fetchTsDataTypes.get(i), isAligned);
          schemaEntityCache.put(path, entity);
          schemaCacheEntityMap.put(path, entity);
        } catch (IllegalPathException e) {
          logger.error("Create PartialPath:{} failed.", devicePath.getFullPath());
        }
      }
    }
    return schemaCacheEntityMap;
  }

  /**
   * For delete timeseries meatadata cache operation
   *
   * @param partialPath
   * @return
   */
  public void invalidate(PartialPath partialPath) {
    schemaEntityCache.invalidate(partialPath);
  }

  @TestOnly
  public void cleanUp() {
    schemaEntityCache.invalidateAll();
    schemaEntityCache.cleanUp();
  }

  @TestOnly
  protected Cache<PartialPath, SchemaCacheEntity> getSchemaEntityCache() {
    return schemaEntityCache;
  }
}
