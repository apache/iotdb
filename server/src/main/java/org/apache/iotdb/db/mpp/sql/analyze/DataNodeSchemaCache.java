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

package org.apache.iotdb.db.mpp.sql.analyze;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.schematree.SchemaNode;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class takes the responsibility of metadata cache management of all DataRegions under
 * StorageEngine
 */
public class DataNodeSchemaCache {
  private static final Logger logger = LoggerFactory.getLogger(DataNodeSchemaCache.class);

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private SchemaTree root;

  private LoadingCache<MeasurementPath, SchemaNode> schemaNodeCache;

  private DataNodeSchemaCache() {
    root = new SchemaTree();
    schemaNodeCache =
        Caffeine.newBuilder()
            .maximumSize(config.getDataNodeSchemaCacheSize())
            .removalListener(
                (RemovalListener<MeasurementPath, SchemaNode>)
                    (path, schemaNode, removalCause) -> {
                      removeSchemaNode(schemaNode);
                    })
            .build(
                new CacheLoader<MeasurementPath, SchemaNode>() {
                  @Override
                  public @Nullable SchemaNode load(@NonNull MeasurementPath measurementPath) {
                    return fetchAndAddMeasurementMNode(measurementPath);
                  }
                });
  }

  public static DataNodeSchemaCache getInstance() {
    return DataNodeSchemaCache.DataNodeSchemaCacheHolder.INSTANCE;
  }

  /** singleton pattern. */
  private static class DataNodeSchemaCacheHolder {
    private static final DataNodeSchemaCache INSTANCE = new DataNodeSchemaCache();
  }

  private SchemaNode fetchAndAddMeasurementMNode(MeasurementPath measurementPath) {
    // TODO fetch remote schema region to obtain measurementPath
    logger.info("Need fetch from remote Schema Region.");

    return root.appendSingleMeasurementPath(measurementPath);
  }

  private void removeSchemaNode(SchemaNode schemaNode) {
    if (schemaNode.getChildren().size() != 0) {
      return;
    }
    SchemaNode parent = schemaNode.getParent();
    parent.deleteChild(schemaNode.getName(), schemaNode);
    removeSchemaNode(parent);
  }

  /**
   * For insert operation to verify timeseries metadata consistency
   *
   * @param measurementPath
   * @return
   */
  public boolean validate(MeasurementPath measurementPath) {
    SchemaNode schemaNode = schemaNodeCache.get(measurementPath);
    if (schemaNode == null) {
      logger.error(
          "Timeseries path:{} does not created, please make sure you have set auto_create_schema = true",
          measurementPath.getFullPath());
      return false;
    }
    if (!measurementPath
        .getMeasurementSchema()
        .equals(schemaNode.getAsMeasurementNode().getSchema())) {
      logger.error("Timeseries path:{} measurement schema is not consistency.");
      return false;
    }
    return true;
  }

  /**
   * For delete timeseries meatadata cache operation
   *
   * @param measurementPath
   * @return
   */
  public void invalidate(MeasurementPath measurementPath) {
    schemaNodeCache.invalidate(measurementPath);
  }

  @TestOnly
  protected LoadingCache<MeasurementPath, SchemaNode> getSchemaNodeCache() {
    return schemaNodeCache;
  }

  @TestOnly
  protected SchemaTree getRoot() {
    return root;
  }
}
