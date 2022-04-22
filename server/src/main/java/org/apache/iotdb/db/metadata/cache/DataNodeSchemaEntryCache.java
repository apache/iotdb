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
public class DataNodeSchemaEntryCache {
  private static final Logger logger = LoggerFactory.getLogger(DataNodeSchemaEntryCache.class);

  // private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private Cache<PartialPath, SchemaEntry> schemaEntryCache;

  // TODO use fakeSchemaFetcherImpl for test temporarily
  private static final ISchemaFetcher schemaFetcher = new FakeSchemaFetcherImpl();

  private DataNodeSchemaEntryCache() {
    schemaEntryCache =
        Caffeine.newBuilder()
            // .maximumSize(config.getDataNodeTreeCacheSize())
            .maximumSize(10)
            .build();
  }

  public static DataNodeSchemaEntryCache getInstance() {
    return DataNodeSchemaEntryCache.DataNodeSchemaEntryCacheHolder.INSTANCE;
  }

  /** singleton pattern. */
  private static class DataNodeSchemaEntryCacheHolder {
    private static final DataNodeSchemaEntryCache INSTANCE = new DataNodeSchemaEntryCache();
  }

  public SchemaEntry getSingleSchemaEntry(PartialPath partialPath) {
    SchemaEntry schemaEntry;
    schemaEntry = schemaEntryCache.get(partialPath, k -> fetchAndAddSchemaEntry(k, false));
    return schemaEntry;
  }

  public SchemaEntry getSingleSchemaEntryWithAutoCreate(PartialPath partialPath) {
    SchemaEntry schemaEntry = null;
    if (partialPath instanceof AlignedPath) {
      return null;
    } else if (partialPath instanceof MeasurementPath) {
      schemaEntry = schemaEntryCache.getIfPresent(partialPath);
      if (schemaEntry == null) {
        PartialPath newPath;
        try {
          newPath = new PartialPath(partialPath.getFullPath());
        } catch (IllegalPathException e) {
          e.printStackTrace();
          return null;
        }
        schemaEntry = schemaEntryCache.get(newPath, k -> fetchAndAddSchemaEntry(partialPath, true));
      }
    } else {
      schemaEntry = schemaEntryCache.getIfPresent(partialPath);
    }
    return schemaEntry;
  }

  public Map<PartialPath, SchemaEntry> getBatchSchemaEntry(PartialPath partialPath) {
    if (partialPath instanceof AlignedPath) {
      return fetchAndAddAlignedSchemaEntry((AlignedPath) partialPath, false);
    }
    return null;
  }

  public Map<PartialPath, SchemaEntry> getBatchSchemaEntryWithAutoCreate(PartialPath partialPath) {
    if (partialPath instanceof AlignedPath) {
      return fetchAndAddAlignedSchemaEntry((AlignedPath) partialPath, true);
    }
    return null;
  }

  private SchemaEntry fetchAndAddSchemaEntry(PartialPath partialPath, boolean isAutoCreateSchema) {
    SchemaEntry schemaEntry = null;
    // TODO fetch local schema first

    // TODO fetch remote schema region to obtain schemaTree
    logger.info("Need fetch from remote Schema Region.");
    SchemaTree schemaTree;
    if (!isAutoCreateSchema) {
      schemaTree = schemaFetcher.fetchSchema(new PathPatternTree(partialPath));
      // TODO need to construct schemaEntry from schemaTree

    } else {
      MeasurementPath measurementPath = (MeasurementPath) partialPath;
      MeasurementSchema measurementSchema =
          (MeasurementSchema) measurementPath.getMeasurementSchema();
      String[] measurement = new String[] {partialPath.getMeasurement()};
      TSDataType[] tsDataType = new TSDataType[] {partialPath.getSeriesType()};
      schemaTree =
          schemaFetcher.fetchSchemaWithAutoCreate(
              partialPath.getDevicePath(), measurement, tsDataType, false);

      // TODO need to construct schemaEntry from schemaTree, currently just from partialPath for
      // test
      schemaEntry =
          new SchemaEntry(
              measurementSchema.getMeasurementId(),
              measurementSchema.getType(),
              measurementSchema.getEncodingType(),
              measurementSchema.getCompressor(),
              measurementPath.getMeasurementAlias(),
              false);
    }
    return schemaEntry;
  }

  private Map<PartialPath, SchemaEntry> fetchAndAddAlignedSchemaEntry(
      AlignedPath alignedPath, boolean isAutoCreateSchema) {
    // TODO fetch local schema first

    // TODO fetch remote schema region to obtain schemaTree
    logger.info("Need fetch from remote Schema Region.");
    SchemaTree schemaTree;
    if (!isAutoCreateSchema) {

      schemaTree = schemaFetcher.fetchSchema(new PathPatternTree(alignedPath));
      // TODO need to construct schemaEntry from schemaTree

    } else {
      // TODO need to use schemaFetcher.fetcheSchemaWithAutoCreate()

      List<IMeasurementSchema> schemaList = alignedPath.getSchemaList();
      Map<PartialPath, SchemaEntry> alignedPaths = new HashMap<>();
      for (int i = 0; i < schemaList.size(); i++) {
        PartialPath path =
            alignedPath.getDevicePath().concatNode(schemaList.get(i).getMeasurementId());
        SchemaEntry schemaEntry =
            new SchemaEntry(
                schemaList.get(i).getMeasurementId(),
                schemaList.get(i).getType(),
                schemaList.get(i).getEncodingType(),
                schemaList.get(i).getCompressor(),
                "",
                true);
        schemaEntryCache.put(path, schemaEntry);
        alignedPaths.put(path, schemaEntry);
      }
      return alignedPaths;
    }
    return null;
  }

  /**
   * For insert operation to verify timeseries metadata consistency
   *
   * @param
   * @return
   */
  public boolean validate(PartialPath partialPath) {

    // SchemaEntry schemaEntry = getSingleSchemaEntryWithAutoCreate(partialPath);

    getBatchSchemaEntryWithAutoCreate(partialPath);
    return true;
  }

  /**
   * For delete timeseries meatadata cache operation
   *
   * @param partialPath
   * @return
   */
  public void invalidate(PartialPath partialPath) {
    schemaEntryCache.invalidate(partialPath);
  }

  @TestOnly
  protected Cache<PartialPath, SchemaEntry> getSchemaEntryCache() {
    return schemaEntryCache;
  }
}
