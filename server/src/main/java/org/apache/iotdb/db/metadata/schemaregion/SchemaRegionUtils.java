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

package org.apache.iotdb.db.metadata.schemaregion;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.CreateTimeSeriesPlanImpl;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.slf4j.Logger;

import java.io.File;
import java.util.List;
import java.util.Objects;

public class SchemaRegionUtils {

  public static void deleteSchemaRegionFolder(String schemaRegionDirPath, Logger logger)
      throws MetadataException {
    File schemaRegionDir = SystemFileFactory.INSTANCE.getFile(schemaRegionDirPath);
    File[] sgFiles = schemaRegionDir.listFiles();
    if (sgFiles == null) {
      throw new MetadataException(
          String.format("Can't get files in schema region dir %s", schemaRegionDirPath));
    }
    for (File file : sgFiles) {
      if (file.delete()) {
        logger.info("delete schema region file {}", file.getAbsolutePath());
      } else {
        logger.info("delete schema region file {} failed.", file.getAbsolutePath());
        throw new MetadataException(
            String.format("Failed to delete schema region file %s", file.getAbsolutePath()));
      }
    }

    if (schemaRegionDir.delete()) {
      logger.info("delete schema region folder {}", schemaRegionDir.getAbsolutePath());
    } else {
      logger.info("delete schema region folder {} failed.", schemaRegionDir.getAbsolutePath());
      throw new MetadataException(
          String.format(
              "Failed to delete schema region folder %s", schemaRegionDir.getAbsolutePath()));
    }
    final File storageGroupDir = schemaRegionDir.getParentFile();
    if (Objects.requireNonNull(storageGroupDir.listFiles()).length == 0) {
      storageGroupDir.delete();
    }
  }

  /**
   * When testing some interfaces, if you only care about path and do not care the data type or
   * compression type and other details, then use this function to create a timeseries quickly. It
   * returns a CreateTimeSeriesPlanImpl with data type of INT64, TSEncoding of PLAIN, compression
   * type of SNAPPY and without any tags or templates.
   */
  public static CreateTimeSeriesPlanImpl getSimpleCreateTSPlanImpl(PartialPath path) {
    return new CreateTimeSeriesPlanImpl(
        path, TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY, null, null, null, null);
  }

  /**
   * Create timeseries quickly using CreateTimeSeriesPlanImpl with given string list of paths.
   *
   * @param schemaRegion schemaRegion which you want to create timeseries
   * @param pathList
   */
  public static void createSimpleTimeseriesByList(ISchemaRegion schemaRegion, List<String> pathList)
      throws Exception {
    for (String path : pathList) {
      schemaRegion.createTimeseries(
          SchemaRegionUtils.getSimpleCreateTSPlanImpl(new PartialPath(path)), -1);
    }
  }
}
