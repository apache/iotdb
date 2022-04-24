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
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.sql.analyze.FakeSchemaFetcherImpl;
import org.apache.iotdb.db.mpp.sql.analyze.ISchemaFetcher;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public SchemaCacheEntity getSingleSchemaEntity(
      PartialPath partialPath, boolean isAutoCreateSchema) {
    SchemaCacheEntity schemaCacheEntity;
    if (partialPath instanceof AlignedPath) {
      return null;
    }
    schemaCacheEntity = schemaEntityCache.getIfPresent(partialPath);
    if (schemaCacheEntity == null) {
      PartialPath cacheKey;
      if (partialPath instanceof MeasurementPath) {
        try {
          cacheKey = new PartialPath(partialPath.getFullPath());
        } catch (IllegalPathException e) {
          e.printStackTrace();
          return null;
        }
      } else {
        cacheKey = partialPath;
      }
      schemaCacheEntity =
          schemaEntityCache.get(
              cacheKey, k -> fetchAndAddSchemaEntity(partialPath, isAutoCreateSchema));
    }
    return schemaCacheEntity;
  }

  public Map<PartialPath, SchemaCacheEntity> getAlignedSchemaEntity(
      PartialPath partialPath, boolean isAutoCreateSchema) {
    Map<PartialPath, SchemaCacheEntity> alignedPathMap = new HashMap<>();
    if (partialPath instanceof AlignedPath) {
      AlignedPath alignedPath = new AlignedPath(partialPath.getDevicePath());
      for (int i = 0; i < ((AlignedPath) partialPath).getMeasurementListSize(); i++) {
        PartialPath path = ((AlignedPath) partialPath).getPathWithMeasurement(i);
        SchemaCacheEntity schemaCacheEntity = schemaEntityCache.getIfPresent(path);
        if (schemaCacheEntity != null) {
          alignedPathMap.put(path, schemaCacheEntity);
        } else {
          alignedPath.addMeasurement(((AlignedPath) partialPath).getMeasurementPath(i));
        }
      }
      alignedPathMap.putAll(fetchAndAddAlignedSchemaEntity(alignedPath, isAutoCreateSchema));
    }
    return alignedPathMap;
  }

  private SchemaCacheEntity fetchAndAddSchemaEntity(
      PartialPath partialPath, boolean isAutoCreateSchema) {
    SchemaCacheEntity schemaCacheEntity = null;
    // TODO fetch local schema first

    // TODO fetch remote schema region to obtain schemaTree
    logger.info("Need fetch from remote Schema Region.");
    SchemaTree schemaTree;
    if (!isAutoCreateSchema) {
      schemaTree = schemaFetcher.fetchSchema(new PathPatternTree(partialPath));
      // TODO need to construct schemaEntry from schemaTree

    } else if (!(partialPath instanceof MeasurementPath)) {
      return null;
    } else {
      MeasurementPath measurementPath = (MeasurementPath) partialPath;
      String[] measurement = new String[] {measurementPath.getMeasurement()};
      TSDataType[] tsDataType = new TSDataType[] {measurementPath.getSeriesType()};
      schemaTree =
          schemaFetcher.fetchSchemaWithAutoCreate(
              measurementPath.getDevicePath(), measurement, tsDataType, false);

      // TODO need to construct schemaEntry from schemaTree, currently just from partialPath for
      // test
      MeasurementSchema measurementSchema =
          (MeasurementSchema) measurementPath.getMeasurementSchema();
      schemaCacheEntity =
          new SchemaCacheEntity(
              measurementSchema.getMeasurementId(),
              measurementSchema.getType(),
              measurementSchema.getEncodingType(),
              measurementSchema.getCompressor(),
              measurementPath.getMeasurementAlias(),
              false);
    }
    return schemaCacheEntity;
  }

  private Map<PartialPath, SchemaCacheEntity> fetchAndAddAlignedSchemaEntity(
      AlignedPath alignedPath, boolean isAutoCreateSchema) {
    Map<PartialPath, SchemaCacheEntity> alignedPathMap = new HashMap<>();
    // TODO fetch local schema first

    // TODO fetch remote schema region to obtain schemaTree
    logger.info("Need fetch from remote Schema Region.");
    SchemaTree schemaTree;
    if (!isAutoCreateSchema) {

      schemaTree = schemaFetcher.fetchSchema(new PathPatternTree(alignedPath));
      // TODO need to construct schemaEntry from schemaTree

    } else {
      // TODO need to use schemaFetcher.fetcheSchemaWithAutoCreate()
      String[] measurements =
          alignedPath
              .getMeasurementList()
              .toArray(new String[alignedPath.getMeasurementListSize()]);
      TSDataType[] tsDataTypes = new TSDataType[alignedPath.getMeasurementListSize()];
      for (int i = 0; i < tsDataTypes.length; i++) {
        tsDataTypes[i] = alignedPath.getSchemaList().get(i).getType();
      }
      schemaTree =
          schemaFetcher.fetchSchemaWithAutoCreate(
              alignedPath.getDevicePath(), measurements, tsDataTypes, true);

      List<IMeasurementSchema> schemaList = alignedPath.getSchemaList();
      for (int i = 0; i < schemaList.size(); i++) {
        PartialPath path =
            alignedPath.getDevicePath().concatNode(schemaList.get(i).getMeasurementId());
        SchemaCacheEntity schemaCacheEntity =
            new SchemaCacheEntity(
                schemaList.get(i).getMeasurementId(),
                schemaList.get(i).getType(),
                schemaList.get(i).getEncodingType(),
                schemaList.get(i).getCompressor(),
                "",
                true);
        schemaEntityCache.put(path, schemaCacheEntity);
        alignedPathMap.put(path, schemaCacheEntity);
      }
    }
    return alignedPathMap;
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
